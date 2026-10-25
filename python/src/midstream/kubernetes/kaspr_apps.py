"""KasprApp client protocols and Kubernetes-backed implementation."""

from __future__ import annotations

from collections.abc import Coroutine
from dataclasses import replace
from datetime import UTC, datetime
import inspect
import logging
from typing import Any, Protocol, TypeVar

from midstream.models import ResourceCondition, RuntimeMemberState
from midstream.observation.reader import KasprAppObservationInput
from midstream.utils.background_loop import BackgroundLoop

T = TypeVar("T")

LOGGER = logging.getLogger(__name__)

#: The container a member's pod runs the Kaspr worker in.
KASPR_CONTAINER = "kaspr"

#: Seconds a Kubernetes API call may take before it is cancelled. kubernetes_asyncio sets no
#: timeout of its own, so without one a call waits for as long as its connection stays open. The
#: API server gives up on a request after 60 s (`--request-timeout`) and answers with an error, so
#: this only ends calls it will never answer: a slow but healthy API server still finishes first.
CALL_TIMEOUT_SECONDS = 90.0


class AsyncKasprAppClient(Protocol):
    """Async contract for applying, deleting, and observing Kaspr resources."""

    async def apply(self, resource: dict[str, Any]) -> None:
        ...

    async def delete(self, *, namespace: str, name: str, kind: str = "KasprApp") -> None:
        ...

    async def observe(self, *, namespace: str, name: str) -> KasprAppObservationInput:
        ...


class KasprAppClient(Protocol):
    """Synchronous facade used by workflow handlers to manage Kaspr resources.

    An implementation may also offer `apply_async`, `delete_async` and `observe_async`, which the
    agents await instead so that their event loop keeps running while the API server answers.
    """

    def apply(self, resource: dict[str, Any]) -> None:
        ...

    def delete(self, *, namespace: str, name: str, kind: str = "KasprApp") -> None:
        ...

    def observe(self, *, namespace: str, name: str) -> KasprAppObservationInput:
        ...


class KubernetesKasprAppClient:
    """Kubernetes-backed KasprApp client that uses server-side apply semantics.

    The API calls run on the client's own event loop, on a thread of its own. Each waits at most
    `call_timeout_seconds` and is cancelled at that deadline with `AdapterTimeoutError`. The
    `*_async` variants await the same calls from another event loop without blocking it.
    """

    def __init__(
        self,
        *,
        group: str = "kaspr.io",
        version: str = "v1alpha1",
        field_manager: str = "control-plane",
        call_timeout_seconds: float = CALL_TIMEOUT_SECONDS,
    ) -> None:
        if call_timeout_seconds <= 0:
            raise ValueError("call_timeout_seconds must be > 0")
        self._group = group
        self._version = version
        self._field_manager = field_manager
        self.call_timeout_seconds = call_timeout_seconds
        self._runner = BackgroundLoop(name="kaspr-app-client")
        self._api: Any = None
        #: Reads the members' pods; built beside `_api`. None (a test's own `_api`) reads none.
        self._core_api: Any = None
        self._pods_unreadable_reported = False

    def apply(self, resource: dict[str, Any]) -> None:
        self._run(self._apply(resource), _describe_apply(resource))

    def delete(self, *, namespace: str, name: str, kind: str = "KasprApp") -> None:
        self._run(
            self._delete(namespace=namespace, name=name, kind=kind),
            f"Kubernetes delete of {kind} {namespace}/{name}",
        )

    def observe(self, *, namespace: str, name: str) -> KasprAppObservationInput:
        return self._run(
            self._observe(namespace=namespace, name=name),
            f"Kubernetes read of KasprApp {namespace}/{name}",
        )

    async def apply_async(self, resource: dict[str, Any]) -> None:
        """`apply`, awaited without blocking the caller's event loop."""
        await self._run_async(self._apply(resource), _describe_apply(resource))

    async def delete_async(self, *, namespace: str, name: str, kind: str = "KasprApp") -> None:
        """`delete`, awaited without blocking the caller's event loop."""
        await self._run_async(
            self._delete(namespace=namespace, name=name, kind=kind),
            f"Kubernetes delete of {kind} {namespace}/{name}",
        )

    async def observe_async(self, *, namespace: str, name: str) -> KasprAppObservationInput:
        """`observe`, awaited without blocking the caller's event loop."""
        return await self._run_async(
            self._observe(namespace=namespace, name=name),
            f"Kubernetes read of KasprApp {namespace}/{name}",
        )

    def _run(self, coroutine: Coroutine[Any, Any, T], description: str) -> T:
        return self._runner.run(coroutine, timeout=self.call_timeout_seconds, description=description)

    async def _run_async(self, coroutine: Coroutine[Any, Any, T], description: str) -> T:
        return await self._runner.run_async(coroutine, timeout=self.call_timeout_seconds, description=description)

    async def _apply(self, resource: dict[str, Any]) -> None:
        api = await self._get_api()
        metadata = resource.get("metadata", {})
        namespace = metadata["namespace"]
        name = metadata["name"]
        plural = _plural_for_kind(resource["kind"])
        try:
            await api.patch_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=namespace,
                plural=plural,
                name=name,
                body=resource,
                field_manager=self._field_manager,
                force=True,
                _content_type="application/apply-patch+yaml",
            )
        except Exception as exc:
            if _api_exception_status(exc) != 404:
                raise
            await api.create_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=namespace,
                plural=plural,
                body=resource,
            )

    async def _delete(self, *, namespace: str, name: str, kind: str = "KasprApp") -> None:
        api = await self._get_api()
        try:
            await api.delete_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=namespace,
                plural=_plural_for_kind(kind),
                name=name,
            )
        except Exception as exc:
            if _api_exception_status(exc) != 404:
                raise

    async def _observe(self, *, namespace: str, name: str) -> KasprAppObservationInput:
        api = await self._get_api()
        try:
            resource = await api.get_namespaced_custom_object(
                group=self._group,
                version=self._version,
                namespace=namespace,
                plural="kasprapps",
                name=name,
            )
        except Exception as exc:
            if _api_exception_status(exc) == 404:
                return KasprAppObservationInput(
                    namespace=namespace,
                    kaspr_app_name=name,
                    resource_exists=False,
                    managed_agent_states={},
                )
            raise
        observation = _observation_from_resource(resource, namespace=namespace, name=name)
        members = await self._members(namespace=namespace, name=name)
        return observation if members is None else replace(observation, members=members)

    async def _members(self, *, namespace: str, name: str) -> tuple[RuntimeMemberState, ...] | None:
        """The app's member pods and how often each restarted; None when they cannot be read.

        Supplementary to the KasprApp: a read that fails or is refused - a control plane whose role
        does not yet grant pods - leaves the observation without them, reported once.
        """
        core = self._core_api
        if core is None:
            return None
        try:
            pods = await core.list_namespaced_pod(namespace, label_selector=f"kaspr.io/app={name}")
        except Exception as exc:  # noqa: BLE001 - never fails the observation it supplements
            if not self._pods_unreadable_reported:
                self._pods_unreadable_reported = True
                LOGGER.warning(
                    "Cannot read the pods of runtime %s/%s, so restarts are not observed "
                    "(the control plane's role needs get and list on pods): %s",
                    namespace,
                    name,
                    exc,
                )
            return None
        return tuple(sorted((_member_from_pod(pod) for pod in getattr(pods, "items", None) or []), key=lambda member: member.name))

    async def _get_api(self) -> Any:
        if self._api is not None:
            return self._api
        from kubernetes_asyncio import client as k8s_client
        from kubernetes_asyncio import config as k8s_config

        try:
            await _run_config_loader(k8s_config.load_incluster_config)
        except Exception:
            await _run_config_loader(k8s_config.load_kube_config)
        api_client = k8s_client.ApiClient()
        self._core_api = k8s_client.CoreV1Api(api_client)
        self._api = k8s_client.CustomObjectsApi(api_client)
        return self._api


def _describe_apply(resource: dict[str, Any]) -> str:
    metadata = resource.get("metadata") or {}
    return (
        f"Kubernetes apply of {resource.get('kind', 'resource')} "
        f"{metadata.get('namespace', '?')}/{metadata.get('name', '?')}"
    )


async def _run_config_loader(loader: Any) -> None:
    result = loader()
    if inspect.isawaitable(result):
        await result


def _api_exception_status(exc: Exception) -> int | None:
    status = getattr(exc, "status", None)
    return status if isinstance(status, int) else None


def _plural_for_kind(kind: str) -> str:
    mapping = {
        "KasprApp": "kasprapps",
        "KasprAgent": "kaspragents",
        "KasprTable": "kasprtables",
        "KasprWebView": "kasprwebviews",
        "KasprJoin": "kasprjoins",
        "KasprTask": "kasprtasks",
    }
    try:
        return mapping[kind]
    except KeyError as exc:
        raise ValueError(f"unsupported Kaspr resource kind {kind}") from exc


def _observation_from_resource(resource: dict[str, Any], *, namespace: str, name: str) -> KasprAppObservationInput:
    metadata = resource.get("metadata", {})
    status = resource.get("status", {})
    linked_resources = status.get("linkedResources", {})
    agents = linked_resources.get("agents", [])
    managed_agent_states = {
        agent["name"]: agent.get("state", "unknown")
        for agent in agents
        if agent.get("name")
    }
    conditions = {
        condition.get("type"): condition
        for condition in status.get("conditions", [])
        if condition.get("type")
    }
    return KasprAppObservationInput(
        namespace=namespace,
        kaspr_app_name=name,
        resource_exists=True,
        managed_agent_states=managed_agent_states,
        metadata_generation=metadata.get("generation"),
        observed_generation=status.get("observedGeneration"),
        ready_condition=_resource_condition_from_status(conditions.get("Ready")),
        progressing_condition=_resource_condition_from_status(conditions.get("Progressing")),
        rollout_in_progress=status.get("rolloutInProgress"),
        available_members=_stringify_optional(status.get("availableMembers")),
        rebalance_required=status.get("rebalanceRequired"),
        rebalancing_members=_stringify_optional(status.get("rebalancingMembers")),
    )


def _member_from_pod(pod: Any) -> RuntimeMemberState:
    """A member's pod as its `kaspr` container's restarts, last exit and present wait."""
    status = getattr(pod, "status", None)
    statuses = list(getattr(status, "container_statuses", None) or [])
    container = next((item for item in statuses if getattr(item, "name", None) == KASPR_CONTAINER), None)
    name = str(pod.metadata.name)
    if container is None:
        return RuntimeMemberState(name=name)
    terminated = getattr(getattr(container, "last_state", None), "terminated", None)
    waiting = getattr(getattr(container, "state", None), "waiting", None)
    return RuntimeMemberState(
        name=name,
        restarts=int(getattr(container, "restart_count", 0) or 0),
        last_exit_reason=getattr(terminated, "reason", None),
        last_exit_code=getattr(terminated, "exit_code", None),
        last_exited_at=_timestamp(getattr(terminated, "finished_at", None)),
        waiting_reason=getattr(waiting, "reason", None),
    )


def _timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        moment = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return moment.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return str(value)


def _resource_condition_from_status(value: dict[str, Any] | None) -> ResourceCondition | None:
    if value is None:
        return None
    return ResourceCondition(
        status=value.get("status", "Unknown"),
        reason=value.get("reason"),
        message=value.get("message"),
        last_transition_time=value.get("lastTransitionTime"),
    )


def _stringify_optional(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)