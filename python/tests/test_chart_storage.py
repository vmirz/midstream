"""The lake's object storage in the chart: each backend's credentials, the console's profiles, and what the chart refuses.

A domain's control plane, maintenance and tail apps and every runtime reach the lake with the same
credentials, as Secret references in the variables each backend's SDK reads itself, plus Midstream's own
STORAGE_* settings (docs/design/storage-backends.md §5.7, §6.3). The console reads through its storage
profiles instead, one per destination prefix with credentials of its own (§5.8, §6.4). No rendered
manifest ever holds a credential, only the names of the Secrets that do.
"""

from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from chart_support import CHART, NAMESPACE, RELEASE, VALUES_FILES, bare, require_helm  # noqa: E402

CI = CHART / "ci"
DEFAULT = CI / "test-values.yaml"
MINIMAL = CI / "minimal-values.yaml"
CONSOLE_ONLY = CI / "console-only-values.yaml"
S3 = CI / "s3-values.yaml"
AZURE = CI / "azure-values.yaml"

#: Every variable `midstream.storageEnv` may render, in the order it renders them.
STORAGE_VARIABLES = (
    "GOOGLE_APPLICATION_CREDENTIALS",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "STORAGE_S3_REGION",
    "STORAGE_S3_ENDPOINT",
    "STORAGE_S3_PATH_STYLE",
    "AZURE_STORAGE_CONNECTION_STRING",
    "AZURE_STORAGE_ACCOUNT_KEY",
    "AZURE_CLIENT_ID",
    "AZURE_TENANT_ID",
    "AZURE_CLIENT_SECRET",
    "STORAGE_AZURE_ENDPOINT",
)
#: The variables that carry a credential: they may only ever come from a Secret.
CREDENTIAL_VARIABLES = frozenset(
    {
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AZURE_STORAGE_CONNECTION_STRING",
        "AZURE_STORAGE_ACCOUNT_KEY",
        "AZURE_CLIENT_ID",
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_SECRET",
    }
)
#: The three destination forms, as every refusal names them.
FORMS = "gs://bucket[/prefix], s3://bucket[/prefix] or abfss://container@account.dfs.core.windows.net[/prefix]"


def secret_ref(name: str, key: str, *, optional: bool = False) -> dict:
    """An env entry's `valueFrom`, as the chart renders a Secret reference."""
    reference = {"name": name, "key": key}
    if optional:
        reference["optional"] = True
    return {"secretKeyRef": reference}


GCS_ENV = [{"name": "GOOGLE_APPLICATION_CREDENTIALS", "value": "/etc/midstream/gcs/service-account.json"}]
GCS_VOLUMES = [
    {
        "name": "gcs-credentials",
        "secret": {"secretName": "midstream-gcs", "items": [{"key": "service-account.json", "path": "service-account.json"}]},
    }
]
GCS_MOUNTS = [{"name": "gcs-credentials", "mountPath": "/etc/midstream/gcs", "readOnly": True}]
S3_ENV = [
    {"name": "AWS_ACCESS_KEY_ID", "valueFrom": secret_ref("midstream-s3", "access-key-id")},
    {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": secret_ref("midstream-s3", "secret-access-key")},
    {"name": "AWS_SESSION_TOKEN", "valueFrom": secret_ref("midstream-s3", "session-token", optional=True)},
    {"name": "STORAGE_S3_REGION", "value": "us-east-1"},
    {"name": "STORAGE_S3_ENDPOINT", "value": "http://s3.example.com:8333"},
    {"name": "STORAGE_S3_PATH_STYLE", "value": "true"},
]


def azure_env(secret: str, endpoint: str | None = None) -> list[dict]:
    entries = [
        {"name": name, "valueFrom": secret_ref(secret, key, optional=True)}
        for name, key in (
            ("AZURE_STORAGE_CONNECTION_STRING", "connection-string"),
            ("AZURE_STORAGE_ACCOUNT_KEY", "account-key"),
            ("AZURE_CLIENT_ID", "client-id"),
            ("AZURE_TENANT_ID", "tenant-id"),
            ("AZURE_CLIENT_SECRET", "client-secret"),
        )
    ]
    if endpoint is not None:
        entries.append({"name": "STORAGE_AZURE_ENDPOINT", "value": endpoint})
    return entries


def _command(values: Path, settings: tuple[str, ...], overrides: Path | None) -> list[str]:
    command = ["helm", "template", RELEASE, str(CHART), "-n", NAMESPACE, "-f", str(values)]
    if overrides is not None:
        command += ["-f", str(overrides)]
    for setting in settings:
        command += ["--set", setting]
    return command


def _helm(values: Path, settings: tuple[str, ...], overrides: dict | None) -> subprocess.CompletedProcess:
    import yaml

    require_helm()
    with tempfile.TemporaryDirectory() as directory:
        extra = None
        if overrides:
            extra = Path(directory) / "overrides.yaml"
            extra.write_text(yaml.safe_dump(overrides))
        return subprocess.run(_command(values, settings, extra), capture_output=True, text=True)


@lru_cache(maxsize=None)
def _rendered(values: Path, settings: tuple[str, ...], overrides: str) -> tuple[dict, ...]:
    import yaml

    completed = _helm(values, settings, json.loads(overrides))
    if completed.returncode != 0:
        raise AssertionError(f"the chart refused {values.name} {settings} {overrides}: {completed.stderr}")
    return tuple(document for document in yaml.safe_load_all(completed.stdout) if document)


def render_with(values: Path, *settings: str, overrides: dict | None = None) -> tuple[dict, ...]:
    """Every document the chart renders with a values file, `--set` settings and a further values document."""
    return _rendered(values, settings, json.dumps(overrides or {}, sort_keys=True))


def render_failure(values: Path, *settings: str, overrides: dict | None = None) -> str:
    """The error `helm template` stops with, for values the chart must refuse."""
    completed = _helm(values, settings, overrides)
    if completed.returncode == 0:
        raise AssertionError(f"the chart rendered {values.name} {settings} {overrides}")
    return completed.stderr


def kaspr_specs(documents: tuple[dict, ...]) -> dict[str, dict]:
    """Every KasprApp's spec by its bare name, and, as `runtime`, the spec every runtime is rendered from."""
    import yaml

    specs = {bare(document["metadata"]["name"]): document["spec"] for document in documents if document.get("kind") == "KasprApp"}
    if "control-plane" in specs:
        env = {entry["name"]: entry for entry in specs["control-plane"]["template"]["kasprContainer"]["env"]}
        specs["runtime"] = yaml.safe_load(env["CONTROL_PLANE_RUNTIME_APP_SPEC"]["value"])
    return specs


def storage_env(spec: dict) -> list[dict]:
    """The storage variables of a KasprApp's (or the runtime spec's) container, in their order."""
    return [entry for entry in spec["template"]["kasprContainer"]["env"] if entry["name"] in STORAGE_VARIABLES]


def has_console(documents: tuple[dict, ...]) -> bool:
    """Whether the release runs the console (a domain alone does not)."""
    return any(document.get("kind") == "Deployment" for document in documents)


def console_container(documents: tuple[dict, ...]) -> dict:
    deployment = next(document for document in documents if document.get("kind") == "Deployment")
    return next(container for container in deployment["spec"]["template"]["spec"]["containers"] if container["name"] == "console")


def console_pod(documents: tuple[dict, ...]) -> dict:
    return next(document for document in documents if document.get("kind") == "Deployment")["spec"]["template"]


def storage_registry(documents: tuple[dict, ...]) -> list[dict]:
    config_map = next(
        document
        for document in documents
        if document.get("kind") == "ConfigMap" and document["metadata"]["name"].endswith("-console-domains")
    )
    return json.loads(config_map["data"]["storage.json"])


def every_env(documents: tuple[dict, ...]) -> list[tuple[str, dict]]:
    """Every env entry of every container the chart renders, the runtime spec's included, with where it is."""
    found: list[tuple[str, dict]] = []
    for document in documents:
        name = document["metadata"]["name"]
        if document.get("kind") == "KasprApp":
            for entry in document["spec"]["template"]["kasprContainer"].get("env") or []:
                found.append((name, entry))
        elif document.get("kind") == "Deployment":
            for container in document["spec"]["template"]["spec"]["containers"]:
                for entry in container.get("env") or []:
                    found.append((f"{name}/{container['name']}", entry))
    for name, spec in kaspr_specs(documents).items():
        if name == "runtime":
            for entry in spec["template"]["kasprContainer"].get("env") or []:
                found.append(("the runtime spec", entry))
    return found


class AppsAndRuntimesTests(unittest.TestCase):
    """Every app of a domain and every runtime reach the lake with the same storage settings."""

    APPS = ("control-plane", "maintenance", "tail", "runtime")

    def assert_storage(self, documents: tuple[dict, ...], expected_env: list[dict], volumes=None, mounts=None) -> None:
        specs = kaspr_specs(documents)
        self.assertEqual(set(self.APPS), set(specs), "the control plane, the maintenance and tail apps and the runtime spec")
        for name in self.APPS:
            with self.subTest(app=name):
                template = specs[name]["template"]
                self.assertEqual(expected_env, storage_env(specs[name]))
                self.assertEqual(volumes, (template.get("pod") or {}).get("volumes"))
                self.assertEqual(mounts, template["kasprContainer"].get("volumeMounts"))

    def test_gcs_mounts_the_service_account_key_and_names_it(self) -> None:
        self.assert_storage(render_with(DEFAULT), GCS_ENV, GCS_VOLUMES, GCS_MOUNTS)
        # The variable names the file the mount puts there.
        self.assertTrue(GCS_ENV[0]["value"].startswith(GCS_MOUNTS[0]["mountPath"] + "/"))

    def test_s3_reads_its_key_pair_from_the_secret_and_its_endpoint_from_the_values(self) -> None:
        # The pair is required and the session token optional; nothing is mounted for S3.
        self.assert_storage(render_with(S3), S3_ENV)

    def test_azure_with_workload_identity_renders_no_credential(self) -> None:
        self.assert_storage(render_with(AZURE), [])

    def test_azure_reads_every_kind_of_credential_from_the_secret_each_optional(self) -> None:
        documents = render_with(AZURE, "storage.azure.credentialsSecret=lake-azure", "storage.azure.endpoint=http://azurite.example.com:10000/")
        self.assert_storage(documents, azure_env("lake-azure", "http://azurite.example.com:10000"))

    def test_without_storage_values_the_pods_use_their_own_identity(self) -> None:
        self.assert_storage(render_with(MINIMAL), [])

    def test_settings_render_without_credentials_and_only_when_set(self) -> None:
        # An S3-compatible server reached with the pods' own identity: the endpoint, no key pair.
        documents = render_with(MINIMAL, "storage.s3.endpoint=https://s3.example.com/", "controlPlane.settings.defaultDestinationUri=s3://example-lake")
        self.assert_storage(documents, [{"name": "STORAGE_S3_ENDPOINT", "value": "https://s3.example.com"}])
        # pathStyle false and an empty region render nothing.
        documents = render_with(S3, "storage.s3.pathStyle=false", "storage.s3.region=")
        self.assert_storage(documents, [entry for entry in S3_ENV if entry["name"] not in ("STORAGE_S3_PATH_STYLE", "STORAGE_S3_REGION")])

    def test_every_backend_at_once_for_destinations_on_several(self) -> None:
        # A domain whose destination moved keeps serving its older tables where they are (design §7.1).
        documents = render_with(DEFAULT, *(f"storage.s3.{key}={value}" for key, value in (("credentialsSecret", "midstream-s3"), ("region", "us-east-1"), ("endpoint", "http://s3.example.com:8333"), ("pathStyle", "true"))), "storage.azure.credentialsSecret=lake-azure")
        self.assert_storage(documents, GCS_ENV + S3_ENV + azure_env("lake-azure"), GCS_VOLUMES, GCS_MOUNTS)

    def test_the_destination_reaches_the_control_plane_as_given(self) -> None:
        for values, expected in ((DEFAULT, "gs://example-lake/midstream"), (S3, "s3://example-lake"), (AZURE, "abfss://lake@exampleaccount.dfs.core.windows.net"), (MINIMAL, "")):
            with self.subTest(values=values.name):
                env = {entry["name"]: entry for entry in kaspr_specs(render_with(values))["control-plane"]["template"]["kasprContainer"]["env"]}
                self.assertEqual(expected, env["CONTROL_PLANE_DEFAULT_DESTINATION_URI"]["value"])

    def test_the_storage_prefix_reaches_the_control_plane_trimmed_as_it_trims_it(self) -> None:
        # Whitespace, then slashes; nothing left takes the release name, as an empty prefix does.
        for prefix, expected in ((" /lake/ ", "lake"), ("lake/eu", "lake/eu"), ("/", RELEASE), ("", RELEASE)):
            with self.subTest(prefix=prefix):
                env = {entry["name"]: entry for entry in kaspr_specs(render_with(MINIMAL, f"controlPlane.settings.storagePrefix={prefix}"))["control-plane"]["template"]["kasprContainer"]["env"]}
                self.assertEqual(expected, env["CONTROL_PLANE_STORAGE_PREFIX"]["value"])


class EndpointTests(unittest.TestCase):
    """An endpoint is an http:// or https:// URL naming a server, as the stores and the console require."""

    #: Each place an endpoint is set, with the values file that renders it and how a refusal names it.
    PLACES = (
        ("storage.s3.endpoint", S3, 'storage.s3.endpoint "{}"'),
        ("storage.azure.endpoint", AZURE, 'storage.azure.endpoint "{}"'),
        ("console.storage[0].endpoint", S3, 'console.storage[0] (lake): endpoint "{}"'),
    )

    def test_anything_but_a_servers_url_is_refused(self) -> None:
        # host:port is DuckDB's spelling, which every store would refuse at its first operation; a path or
        # a user has no place in an endpoint.
        for value in ("s3.example.com:8333", "ftp://s3.example.com", "http://", "http://:8333", "https://s3.example.com/lake", "http://user:secret@s3.example.com", "http://s3 example.com"):
            for setting, values, named in self.PLACES:
                with self.subTest(setting=setting, value=value):
                    message = render_failure(values, f"{setting}={value}")
                    self.assertIn(named.format(value) + " must be an http:// or https:// URL naming the server alone", message)

    def test_an_endpoint_renders_trimmed_of_whitespace_and_trailing_slashes(self) -> None:
        documents = render_with(S3, "storage.s3.endpoint= https://s3.example.com:8333// ", "storage.azure.endpoint=HTTP://azurite.example.com:10000/", "console.storage[0].endpoint=https://s3.example.com/ ")
        for name in AppsAndRuntimesTests.APPS:
            with self.subTest(app=name):
                env = {entry["name"]: entry.get("value") for entry in storage_env(kaspr_specs(documents)[name])}
                self.assertEqual("https://s3.example.com:8333", env["STORAGE_S3_ENDPOINT"])
                self.assertEqual("HTTP://azurite.example.com:10000", env["STORAGE_AZURE_ENDPOINT"])
        self.assertEqual("https://s3.example.com", storage_registry(documents)[0]["endpoint"])


class CredentialTests(unittest.TestCase):
    """A rendered manifest names Secrets, never a credential (design §8)."""

    def all_backends(self) -> tuple[dict, ...]:
        return render_with(
            DEFAULT,
            "storage.s3.credentialsSecret=midstream-s3",
            "storage.azure.credentialsSecret=midstream-azure",
            overrides={
                "console": {
                    "storage": [
                        {"name": "gcs-lake", "scope": "gs://example-lake/", "kind": "gcs", "credentialsSecret": "gcs-reader"},
                        {"name": "s3-lake", "scope": "s3://example-lake/", "kind": "s3", "credentialsSecret": "s3-reader"},
                        {"name": "azure-lake", "scope": "abfss://lake@exampleaccount.dfs.core.windows.net/", "kind": "azure", "credentialsSecret": "azure-reader"},
                    ]
                }
            },
        )

    def test_every_credential_variable_comes_from_a_secret(self) -> None:
        renders = {values.name: render_with(values) for values in VALUES_FILES}
        renders["every backend"] = self.all_backends()
        checked = 0
        for label, documents in renders.items():
            for where, entry in every_env(documents):
                name = entry["name"]
                if name not in CREDENTIAL_VARIABLES and not (name.startswith("CONSOLE_STORAGE_") and name != "CONSOLE_STORAGE_FILE"):
                    continue
                checked += 1
                with self.subTest(values=label, where=where, variable=name):
                    self.assertNotIn("value", entry, "a credential inlined in a manifest")
                    self.assertIn("secretKeyRef", entry["valueFrom"])
        self.assertGreaterEqual(checked, 8 * 4 + 10, "the scan found fewer credential variables than the renders hold")

    def test_the_chart_renders_no_secret(self) -> None:
        for values in VALUES_FILES:
            with self.subTest(values=values.name):
                self.assertFalse([document for document in render_with(values) if document.get("kind") == "Secret"])

    def test_a_credential_given_as_a_value_goes_nowhere(self) -> None:
        # The storage blocks take Secret names only: a key typed into them is ignored, never rendered.
        documents = render_with(
            S3,
            "storage.s3.accessKeyId=AKIACANARYACCESSKEY",
            "storage.s3.secretAccessKey=canary-secret-access-key",
            "storage.azure.accountKey=canary-account-key",
            "storage.gcs.serviceAccountJson=canary-service-account",
        )
        rendered = json.dumps(documents)
        for canary in ("AKIACANARYACCESSKEY", "canary-secret-access-key", "canary-account-key", "canary-service-account"):
            self.assertNotIn(canary, rendered)
        # A console profile refuses any field it does not know, credentials included.
        self.assertIn('unknown field "secretAccessKey"', render_failure(S3, "console.storage[0].secretAccessKey=canary"))

    def test_the_consoles_registry_holds_no_credential(self) -> None:
        allowed = {"name", "scope", "kind", "endpoint", "region", "pathStyle", "access", "keyFile"}
        renders = [(values.name, render_with(values)) for values in VALUES_FILES] + [("every backend", self.all_backends())]
        for label, documents in [(label, documents) for label, documents in renders if has_console(documents)]:
            with self.subTest(values=label):
                for profile in storage_registry(documents):
                    self.assertLessEqual(set(profile), allowed)
                    self.assertNotIn("credentialsSecret", profile, "the file names no Secret either: the env does")


class DestinationTests(unittest.TestCase):
    """controlPlane.settings.defaultDestinationUri takes the three forms engines read, and nothing else (design §6.5)."""

    def failure(self, destination: str) -> str:
        return render_failure(MINIMAL, f"controlPlane.settings.defaultDestinationUri={destination}")

    def test_a_file_destination_is_for_tests_only(self) -> None:
        message = self.failure("file:///tmp/lake")
        self.assertIn("file:// destinations are for tests only", message)
        self.assertIn(FORMS, message)

    def test_an_unknown_scheme_is_refused_naming_the_three_forms(self) -> None:
        for destination in ("hdfs://lake/warehouse", "az://lake/prod", "wasbs://lake@exampleaccount.blob.core.windows.net", "example-lake", "gcs://example-lake"):
            with self.subTest(destination=destination):
                self.assertIn(f'"{destination}" must be {FORMS}', self.failure(destination))

    def test_the_three_forms_render(self) -> None:
        for destination in (
            "gs://example-lake",
            "gs://example-lake/midstream/",
            "gs://example.lake_eu/a/b",
            "s3://example-lake",
            "s3://example.lake/prod//",
            "abfss://lake@exampleaccount.dfs.core.windows.net",
            "abfss://my-lake2@exampleaccount.dfs.core.windows.net/prod/",
        ):
            with self.subTest(destination=destination):
                specs = kaspr_specs(render_with(MINIMAL, f"controlPlane.settings.defaultDestinationUri={destination}"))
                env = {entry["name"]: entry for entry in specs["control-plane"]["template"]["kasprContainer"]["env"]}
                self.assertEqual(destination, env["CONTROL_PLANE_DEFAULT_DESTINATION_URI"]["value"])

    def test_surrounding_whitespace_is_trimmed_as_the_control_plane_trims_it(self) -> None:
        specs = kaspr_specs(render_with(MINIMAL, "controlPlane.settings.defaultDestinationUri=  s3://example-lake/prod/ "))
        env = {entry["name"]: entry for entry in specs["control-plane"]["template"]["kasprContainer"]["env"]}
        self.assertEqual("s3://example-lake/prod/", env["CONTROL_PLANE_DEFAULT_DESTINATION_URI"]["value"])

    def test_a_malformed_destination_is_refused_by_its_schemes_rule(self) -> None:
        for destination, rule in (
            ("gs://Example_Lake", "is not gs://bucket[/prefix]"),
            ("gs://-lake", "is not gs://bucket[/prefix]"),
            ("s3://ab", "is not s3://bucket[/prefix]"),
            ("s3://Example-Lake", "is not s3://bucket[/prefix]"),
            ("s3://", "is not s3://bucket[/prefix]"),
            ("abfss://lake@exampleaccount.blob.core.windows.net", "is not abfss://container@account.dfs.core.windows.net[/prefix]"),
            ("abfss://exampleaccount.dfs.core.windows.net/lake", "is not abfss://container@account.dfs.core.windows.net[/prefix]"),
            ("abfss://la--ke@exampleaccount.dfs.core.windows.net", "is not abfss://container@account.dfs.core.windows.net[/prefix]"),
            ("abfss://lake-@exampleaccount.dfs.core.windows.net", "is not abfss://container@account.dfs.core.windows.net[/prefix]"),
            ("abfss://lake@example-account.dfs.core.windows.net", "is not abfss://container@account.dfs.core.windows.net[/prefix]"),
            ("gs://example-lake/" + "x" * 1020, "longer than 1024 characters"),
        ):
            with self.subTest(destination=destination[:60]):
                self.assertIn(rule, self.failure(destination))


class ConsoleProfileTests(unittest.TestCase):
    """The console's storage profiles: a registry file without credentials, and each profile's Secret."""

    def test_the_registry_holds_each_profile_without_its_credentials(self) -> None:
        expected = {
            DEFAULT: [{"name": "lake", "scope": "gs://example-lake/", "kind": "gcs", "keyFile": "/etc/midstream-storage/lake/service-account.json"}],
            CONSOLE_ONLY: [
                {"name": "domain-a", "scope": "gs://domain-a-lake/", "kind": "gcs", "keyFile": "/etc/midstream-storage/domain-a/service-account.json"},
                {
                    "name": "domain-b",
                    "scope": "s3://domain-b-lake/",
                    "kind": "s3",
                    "endpoint": "https://s3.example.com",
                    "region": "eu-west-1",
                    "pathStyle": True,
                    "access": "cached",
                },
            ],
            S3: [{"name": "lake", "scope": "s3://example-lake/", "kind": "s3", "endpoint": "http://s3.example.com:8333", "region": "us-east-1", "pathStyle": True}],
            AZURE: [{"name": "lake", "scope": "abfss://lake@exampleaccount.dfs.core.windows.net/", "kind": "azure"}],
            MINIMAL: [],
        }
        for values, profiles in expected.items():
            with self.subTest(values=values.name):
                self.assertEqual(profiles, storage_registry(render_with(values)))

    def test_the_console_reads_the_registry_from_the_config_maps_mount(self) -> None:
        for values in (DEFAULT, CONSOLE_ONLY, MINIMAL):
            with self.subTest(values=values.name):
                documents = render_with(values)
                container = console_container(documents)
                env = {entry["name"]: entry for entry in container["env"]}
                self.assertEqual({"name": "CONSOLE_STORAGE_FILE", "value": "/etc/midstream/storage.json"}, env["CONSOLE_STORAGE_FILE"])
                mount = next(item for item in container["volumeMounts"] if item["name"] == "domains")
                self.assertEqual("/etc/midstream", mount["mountPath"])
                volume = next(item for item in console_pod(documents)["spec"]["volumes"] if item["name"] == "domains")
                config_map = next(document for document in documents if document.get("kind") == "ConfigMap")
                self.assertEqual(config_map["metadata"]["name"], volume["configMap"]["name"])
                self.assertEqual({"domains.json", "storage.json"}, set(config_map["data"]))

    def test_each_profiles_credentials_come_from_its_secret_by_kind(self) -> None:
        def entries(prefix: str, secret: str, pairs) -> list[dict]:
            return [{"name": f"{prefix}_{name}", "valueFrom": secret_ref(secret, key, optional=True)} for name, key in pairs]

        gcs = (("GCS_HMAC_KEY_ID", "hmac-key-id"), ("GCS_HMAC_SECRET", "hmac-secret"))
        s3 = (("ACCESS_KEY_ID", "access-key-id"), ("SECRET_ACCESS_KEY", "secret-access-key"), ("SESSION_TOKEN", "session-token"))
        azure = (
            ("AZURE_CONNECTION_STRING", "connection-string"),
            ("AZURE_ACCOUNT_KEY", "account-key"),
            ("AZURE_CLIENT_ID", "client-id"),
            ("AZURE_TENANT_ID", "tenant-id"),
            ("AZURE_CLIENT_SECRET", "client-secret"),
        )
        expected = {
            DEFAULT: entries("CONSOLE_STORAGE_LAKE", "midstream-lake-reader", gcs),
            CONSOLE_ONLY: entries("CONSOLE_STORAGE_DOMAIN_A", "domain-a-lake-reader", gcs) + entries("CONSOLE_STORAGE_DOMAIN_B", "domain-b-lake-reader", s3),
            S3: entries("CONSOLE_STORAGE_LAKE", "midstream-s3-reader", s3),
            AZURE: entries("CONSOLE_STORAGE_LAKE", "midstream-azure-reader", azure),
            MINIMAL: [],
        }
        for values, wanted in expected.items():
            with self.subTest(values=values.name):
                env = console_container(render_with(values))["env"]
                self.assertEqual(wanted, [entry for entry in env if entry["name"].startswith("CONSOLE_STORAGE_") and entry["name"] != "CONSOLE_STORAGE_FILE"])

    def test_a_gcs_profiles_key_file_is_mounted_beside_the_registry_and_named_by_it(self) -> None:
        documents = render_with(CONSOLE_ONLY)
        (profile,) = [item for item in storage_registry(documents) if item["kind"] == "gcs"]
        mounts = [item for item in console_container(documents)["volumeMounts"] if item["mountPath"].startswith("/etc/midstream-storage/")]
        self.assertEqual([{"name": "storage-domain-a-key", "mountPath": "/etc/midstream-storage/domain-a", "readOnly": True}], mounts)
        self.assertEqual(f"{mounts[0]['mountPath']}/service-account.json", profile["keyFile"])
        volumes = [item for item in console_pod(documents)["spec"]["volumes"] if item["name"].startswith("storage-")]
        self.assertEqual(
            [
                {
                    "name": "storage-domain-a-key",
                    "secret": {
                        "secretName": "domain-a-lake-reader",
                        "optional": True,
                        "items": [{"key": "service-account.json", "path": "service-account.json"}],
                    },
                }
            ],
            volumes,
        )
        # Not inside /etc/midstream, which is the registries' ConfigMap: the two volumes would collide.
        self.assertFalse(mounts[0]["mountPath"].startswith("/etc/midstream/"))

    def test_a_profile_without_a_secret_uses_the_consoles_own_identity(self) -> None:
        documents = render_with(
            MINIMAL,
            overrides={"console": {"storage": [{"name": "lake", "scope": "gs://example-lake/", "kind": "gcs"}, {"name": "archive", "scope": "s3://example-archive/", "kind": "s3", "access": "direct", "pathStyle": False}]}},
        )
        self.assertEqual(
            [
                {"name": "lake", "scope": "gs://example-lake/", "kind": "gcs"},
                {"name": "archive", "scope": "s3://example-archive/", "kind": "s3", "access": "direct", "pathStyle": False},
            ],
            storage_registry(documents),
        )
        container = console_container(documents)
        self.assertFalse([entry for entry in container["env"] if entry["name"].startswith("CONSOLE_STORAGE_") and entry["name"] != "CONSOLE_STORAGE_FILE"])
        self.assertEqual(["data", "domains"], [item["name"] for item in container["volumeMounts"]])

    def test_a_change_of_profiles_restarts_the_console(self) -> None:
        def checksum(documents: tuple[dict, ...]) -> str:
            return console_pod(documents)["metadata"]["annotations"]["checksum/storage"]

        before = checksum(render_with(DEFAULT))
        self.assertNotEqual(before, checksum(render_with(DEFAULT, "console.storage[0].access=cached")))
        self.assertNotEqual(before, checksum(render_with(DEFAULT, "console.storage[0].scope=gs://example-lake/midstream/")))
        self.assertEqual(before, checksum(render_with(DEFAULT)))

    def test_the_console_takes_no_storage_values_of_the_domains(self) -> None:
        # The console reads through its profiles alone: none of the apps' variables, no envFrom.
        for values in (DEFAULT, S3):
            with self.subTest(values=values.name):
                container = console_container(render_with(values, "storage.s3.credentialsSecret=midstream-s3", "storage.azure.credentialsSecret=midstream-azure"))
                names = {entry["name"] for entry in container["env"]}
                self.assertFalse(names & set(STORAGE_VARIABLES), "the domain's storage variables reached the console")
                self.assertNotIn("envFrom", container)
                self.assertFalse([item for item in container["volumeMounts"] if item["name"] == "gcs-credentials"])


class ConsoleProfileRefusalTests(unittest.TestCase):
    """A profile the console could not use stops the install with a message naming the profile and the rule."""

    def test_names_are_unique(self) -> None:
        message = render_failure(DEFAULT, "console.storage[1].name=lake", "console.storage[1].scope=gs://other-lake/", "console.storage[1].kind=gcs")
        self.assertIn("console.storage[1] (lake): the name is used twice", message)

    def test_names_are_usable_in_a_variable(self) -> None:
        for name in ("Lake", "1lake", "lake_eu", "l" * 41, ""):
            with self.subTest(name=name):
                self.assertIn("must be a lower case letter followed by", render_failure(DEFAULT, f"console.storage[0].name={name}"))

    def test_a_scope_ends_with_a_slash(self) -> None:
        message = render_failure(DEFAULT, "console.storage[0].scope=gs://example-lake")
        self.assertIn("console.storage[0] (lake): scope \"gs://example-lake\" must end with '/'", message)

    def test_a_scope_names_a_bucket_or_container(self) -> None:
        # Such a scope passes the scheme and the slash, but could never be a prefix of a table's location.
        for values, scope, rule in (
            (DEFAULT, "gs:///", "must name a bucket and hold no whitespace: gs://bucket/[prefix/]"),
            (DEFAULT, "gs://example lake/", "must name a bucket and hold no whitespace"),
            (S3, "s3:///prod/", "must name a bucket and hold no whitespace: s3://bucket/[prefix/]"),
            (AZURE, "abfss://lake/", "must name a container and its account and hold no whitespace: abfss://container@account.dfs.core.windows.net/[prefix/]"),
            # The console parses an Azure location in its one form alone (design D4): another endpoint's
            # host, a bare account or no account name could never match a table's location.
            (AZURE, "abfss://lake@exampleaccount.blob.core.windows.net/", "must name a container and its account"),
            (AZURE, "abfss://lake@exampleaccount/", "must name a container and its account"),
            (AZURE, "abfss://lake@Exampleaccount.dfs.core.windows.net/", "must name a container and its account"),
            (AZURE, "abfss://lake@ab.dfs.core.windows.net/", "must name a container and its account"),
            # . and .. are never a bucket or a container, and must never become a cache path's segment.
            (DEFAULT, "gs://../", "must name a bucket"),
            (S3, "s3://./prod/", "must name a bucket"),
            (AZURE, "abfss://..@exampleaccount.dfs.core.windows.net/", "must name a container and its account"),
        ):
            with self.subTest(scope=scope):
                self.assertIn(f'console.storage[0] (lake): scope "{scope}" {rule}', render_failure(values, f"console.storage[0].scope={scope}"))

    def test_a_scope_in_each_form_renders(self) -> None:
        for values, scope in (
            (AZURE, "abfss://lake@exampleaccount.dfs.core.windows.net/prod/"),
            (AZURE, "abfss://my-lake2@account123.dfs.core.windows.net/"),
            (S3, "s3://example.lake/prod/"),
            (DEFAULT, "gs://example_lake/a/b/"),
        ):
            with self.subTest(scope=scope):
                self.assertEqual(scope, storage_registry(render_with(values, f"console.storage[0].scope={scope}"))[0]["scope"])

    def test_a_kind_matches_its_scopes_scheme(self) -> None:
        for kind, scheme in (("s3", "s3://"), ("azure", "abfss://")):
            with self.subTest(kind=kind):
                message = render_failure(DEFAULT, f"console.storage[0].kind={kind}")
                self.assertIn(f'console.storage[0] (lake): scope "gs://example-lake/" does not start with {scheme}, the scheme of kind {kind}', message)
        self.assertIn('kind "hdfs" must be gcs, s3 or azure', render_failure(DEFAULT, "console.storage[0].kind=hdfs"))

    def test_access_is_direct_or_cached(self) -> None:
        self.assertIn('console.storage[0] (lake): access "lazy" must be direct or cached', render_failure(DEFAULT, "console.storage[0].access=lazy"))

    def test_a_scope_is_served_by_one_profile(self) -> None:
        message = render_failure(DEFAULT, "console.storage[1].name=other", "console.storage[1].scope=gs://example-lake/", "console.storage[1].kind=gcs")
        self.assertIn('console.storage[1] (other): scope "gs://example-lake/" is already served by profile lake', message)

    def test_the_s3_profiles_of_one_bucket_share_its_endpoint(self) -> None:
        # An s3:// path names no endpoint, so DuckDB and the console could not tell two servers' objects
        # apart: the console refuses such profiles at boot (registry.ts::assertEndpointsDistinguishable),
        # and the chart before the install. In s3-values.yaml, lake serves s3://example-lake/ through
        # http://s3.example.com:8333.
        archive = ("console.storage[1].name=archive", "console.storage[1].scope=s3://example-lake/archive/", "console.storage[1].kind=s3")
        for endpoint in ("http://other.example.com:8333", "http://s3.example.com:9000", None):
            with self.subTest(endpoint=endpoint):
                settings = archive + (() if endpoint is None else (f"console.storage[1].endpoint={endpoint}",))
                self.assertIn(
                    f"console.storage[1] (archive): profile lake serves the bucket s3://example-lake/ through http://s3.example.com:8333, "
                    f"this one through {endpoint or 'AWS'}: an s3:// path names no endpoint",
                    render_failure(S3, *settings),
                )
        # The same server however it is written - case, a trailing slash, a default port - is one endpoint,
        # as the console compares them; another bucket may live on another server; and AWS is one endpoint.
        for settings in (
            archive + ("console.storage[1].endpoint=HTTP://S3.Example.COM:8333/",),
            ("console.storage[1].name=other", "console.storage[1].scope=s3://other-lake/", "console.storage[1].kind=s3", "console.storage[1].endpoint=http://other.example.com"),
        ):
            with self.subTest(settings=settings):
                self.assertEqual(2, len(storage_registry(render_with(S3, *settings))))
        aws = {"name": "lake", "scope": "s3://example-lake/", "kind": "s3"}
        for second in (
            {"name": "archive", "scope": "s3://example-lake/archive/", "kind": "s3"},
            {"name": "eu", "scope": "s3://example-lake/eu/", "kind": "s3", "endpoint": "https://s3.example.com"},
        ):
            first = aws if "endpoint" not in second else {**aws, "endpoint": "https://s3.example.com:443"}
            with self.subTest(second=second):
                self.assertEqual(2, len(storage_registry(render_with(S3, overrides={"console": {"storage": [first, second]}}))))
        # A default port is dropped only for its own scheme: http and https are two endpoints.
        both = [{**aws, "endpoint": "https://s3.example.com:443"}, {**aws, "name": "plain", "scope": "s3://example-lake/plain/", "endpoint": "http://s3.example.com:80"}]
        self.assertIn(
            "console.storage[1] (plain): profile lake serves the bucket s3://example-lake/ through https://s3.example.com, this one through http://s3.example.com: an s3://",
            render_failure(S3, overrides={"console": {"storage": both}}),
        )

    def test_unknown_fields_and_mistyped_values_are_refused(self) -> None:
        self.assertIn('unknown field "credentialSecret"', render_failure(DEFAULT, "console.storage[0].credentialSecret=lake-reader"))
        self.assertIn("pathStyle must be true or false", render_failure(DEFAULT, "console.storage[0].pathStyle=yes"))
        self.assertIn(
            "credentialsSecret must be the name of a Secret",
            render_failure(DEFAULT, overrides={"console": {"storage": [{"name": "lake", "scope": "gs://example-lake/", "kind": "gcs", "credentialsSecret": {"name": "lake-reader"}}]}}),
        )


class IdentityTests(unittest.TestCase):
    """A backend without a Secret reads as the pods' own cloud identity, which the chart lets them carry.

    IRSA reads a role from the ServiceAccount's annotation, AKS workload identity a label on the pod and a
    client id on its ServiceAccount; kaspr-operator creates one ServiceAccount per app and per runtime, so
    every KasprApp and the runtime spec carry both, and the console has a ServiceAccount of its own.
    """

    WORKLOAD_IDENTITY_LABELS = {"azure.workload.identity/use": "true"}
    WORKLOAD_IDENTITY_ANNOTATIONS = {"azure.workload.identity/client-id": "00000000-0000-0000-0000-000000000000"}
    IRSA = {"eks.amazonaws.com/role-arn": "arn:aws:iam::123456789012:role/midstream"}

    def identity(self, documents: tuple[dict, ...]) -> dict[str, tuple]:
        """Each KasprApp's and the runtime spec's pod labels and ServiceAccount annotations."""
        found = {}
        for name, spec in kaspr_specs(documents).items():
            template = spec["template"]
            labels = ((template.get("pod") or {}).get("metadata") or {}).get("labels")
            annotations = ((template.get("serviceAccount") or {}).get("metadata") or {}).get("annotations")
            found[name] = (labels, annotations)
        self.assertEqual(set(AppsAndRuntimesTests.APPS), set(found))
        return found

    def console_service_account(self, documents: tuple[dict, ...]) -> dict:
        accounts = [document for document in documents if document.get("kind") == "ServiceAccount"]
        self.assertEqual([f"{RELEASE}-console"], [account["metadata"]["name"] for account in accounts])
        self.assertEqual(accounts[0]["metadata"]["name"], console_pod(documents)["spec"]["serviceAccountName"])
        return accounts[0]

    def test_aks_workload_identity_reaches_every_app_and_runtime(self) -> None:
        documents = render_with(AZURE)
        for name, found in self.identity(documents).items():
            with self.subTest(app=name):
                self.assertEqual((self.WORKLOAD_IDENTITY_LABELS, self.WORKLOAD_IDENTITY_ANNOTATIONS), found)
                self.assertEqual([], storage_env(kaspr_specs(documents)[name]))

    def test_irsa_annotates_every_service_account(self) -> None:
        documents = render_with(S3, "storage.s3.credentialsSecret=", overrides={"kaspr": {"serviceAccountAnnotations": self.IRSA}})
        for name, found in self.identity(documents).items():
            with self.subTest(app=name):
                self.assertEqual((None, self.IRSA), found)
                names = {entry["name"] for entry in storage_env(kaspr_specs(documents)[name])}
                self.assertFalse(names & CREDENTIAL_VARIABLES, "IRSA needs no key pair")

    def test_without_identity_values_nothing_is_added(self) -> None:
        for values in (DEFAULT, MINIMAL, S3):
            with self.subTest(values=values.name):
                documents = render_with(values)
                for name, spec in kaspr_specs(documents).items():
                    self.assertNotIn("serviceAccount", spec["template"], name)
                    self.assertNotIn("metadata", spec["template"].get("pod") or {}, name)
                self.assertNotIn("annotations", self.console_service_account(documents)["metadata"])

    def test_values_render_as_the_strings_kubernetes_takes(self) -> None:
        documents = render_with(
            MINIMAL,
            overrides={
                "kaspr": {"podLabels": {"example.com/flag": True, "example.com/tier": 3}, "serviceAccountAnnotations": {"example.com/flag": False}},
                "console": {"podLabels": {"example.com/flag": True}, "serviceAccountAnnotations": {"example.com/count": 2}},
            },
        )
        for name, found in self.identity(documents).items():
            with self.subTest(app=name):
                self.assertEqual(({"example.com/flag": "true", "example.com/tier": "3"}, {"example.com/flag": "false"}), found)
        # A label the chart does not set itself is the person's to give, app.kubernetes.io/ ones included.
        others = render_with(MINIMAL, overrides={"kaspr": {"podLabels": {"app.kubernetes.io/part-of": "lake"}}, "console": {"podLabels": {"app.kubernetes.io/part-of": "lake"}}})
        self.assertEqual("lake", console_pod(others)["metadata"]["labels"]["app.kubernetes.io/part-of"])
        self.assertEqual({"app.kubernetes.io/part-of": "lake"}, self.identity(others)["runtime"][0])
        self.assertEqual("true", console_pod(documents)["metadata"]["labels"]["example.com/flag"])
        self.assertEqual({"example.com/count": "2"}, self.console_service_account(documents)["metadata"]["annotations"])

    def test_the_console_has_an_identity_of_its_own(self) -> None:
        for values in (DEFAULT, CONSOLE_ONLY, MINIMAL):
            with self.subTest(values=values.name):
                self.console_service_account(render_with(values))
        documents = render_with(AZURE, overrides={"console": {"podLabels": self.WORKLOAD_IDENTITY_LABELS, "serviceAccountAnnotations": self.IRSA}})
        self.assertEqual(self.IRSA, self.console_service_account(documents)["metadata"]["annotations"])
        labels = console_pod(documents)["metadata"]["labels"]
        self.assertEqual("true", labels["azure.workload.identity/use"])
        deployment = next(document for document in documents if document.get("kind") == "Deployment")
        self.assertLessEqual(deployment["spec"]["selector"]["matchLabels"].items(), labels.items(), "the selector still matches")
        # The console does not follow kaspr.*: the domain's identity is not the console's.
        documents = render_with(AZURE)
        self.assertNotIn("azure.workload.identity/use", console_pod(documents)["metadata"]["labels"])
        self.assertNotIn("annotations", self.console_service_account(documents)["metadata"])

    def test_a_label_of_the_charts_own_and_a_value_that_is_no_map_are_refused(self) -> None:
        for setting, rule in (
            ("kaspr.podLabels.app\\.kubernetes\\.io/name=other", 'kaspr.podLabels: "app.kubernetes.io/name" is a label the chart sets itself'),
            ("console.podLabels.helm\\.sh/chart=other", 'console.podLabels: "helm.sh/chart" is a label the chart sets itself'),
            ("kaspr.serviceAccountAnnotations=role", "kaspr.serviceAccountAnnotations must be a map of keys to values"),
            ("console.serviceAccountAnnotations=role", "console.serviceAccountAnnotations must be a map of keys to values"),
        ):
            with self.subTest(setting=setting):
                self.assertIn(rule, render_failure(DEFAULT, setting))


class RemovedValuesTests(unittest.TestCase):
    """Every installation is new (design D10): the GCS-only values are gone, not kept as aliases."""

    def test_the_old_gcs_values_have_no_effect(self) -> None:
        legacy = render_with(MINIMAL, "gcs.credentialsSecret=legacy-gcs", "gcs.credentialsKey=legacy.json", "console.hmacSecret=legacy-hmac")
        self.assertEqual(render_with(MINIMAL), legacy)
        self.assertNotIn("legacy", json.dumps(legacy))

    def test_the_values_file_and_the_templates_no_longer_know_them(self) -> None:
        import yaml

        values = yaml.safe_load((CHART / "values.yaml").read_text())
        self.assertNotIn("gcs", values)
        self.assertNotIn("hmacSecret", values["console"])
        self.assertEqual({"gcs", "s3", "azure"}, set(values["storage"]))
        self.assertEqual([], values["console"]["storage"])
        for template in sorted((CHART / "templates").rglob("*")):
            if template.is_file():
                with self.subTest(template=template.name):
                    text = template.read_text()
                    for gone in (".Values.gcs", "hmacSecret", "midstream.gcs", "/app/gcp", "CONSOLE_GCS_HMAC"):
                        self.assertNotIn(gone, text)

    def test_no_file_of_the_chart_mentions_them(self) -> None:
        # The values tables, the comments and the ci files too, not only the templates.
        import re

        gone = re.compile(r"(?<![\w.$])gcs\.credentials|credentialsKey|hmacSecret|gcp-credentials|/app/gcp|google_application_credentials|CONSOLE_GCS_HMAC")
        for path in sorted(CHART.rglob("*")):
            if path.is_file():
                with self.subTest(path=str(path.relative_to(CHART))):
                    self.assertIsNone(gone.search(path.read_text()))

    def test_no_console_has_google_application_credentials(self) -> None:
        for values in VALUES_FILES:
            documents = render_with(values)
            if not [document for document in documents if document.get("kind") == "Deployment"]:
                continue
            with self.subTest(values=values.name):
                names = {entry["name"] for entry in console_container(documents)["env"]}
                self.assertNotIn("GOOGLE_APPLICATION_CREDENTIALS", names)


class NotesTests(unittest.TestCase):
    """NOTES.txt warns when the console could read no table, or none of this domain's.

    `helm template` does not print NOTES.txt, so the tests render it through `tpl` from a copy of the chart.
    """

    @classmethod
    def setUpClass(cls) -> None:
        require_helm()
        cls._directory = tempfile.TemporaryDirectory()
        cls.chart = Path(cls._directory.name) / "midstream"
        shutil.copytree(CHART, cls.chart)
        shutil.copyfile(CHART / "templates" / "NOTES.txt", cls.chart / "notes-under-test.txt")
        (cls.chart / "templates" / "notes-under-test.yaml").write_text(
            'kind: NotesUnderTest\ntext: |\n{{- tpl (.Files.Get "notes-under-test.txt") . | nindent 2 }}\n'
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls._directory.cleanup()

    def notes(self, values: Path, *settings: str) -> str:
        import yaml

        command = ["helm", "template", RELEASE, str(self.chart), "-n", NAMESPACE, "-f", str(values), "--show-only", "templates/notes-under-test.yaml"]
        for setting in settings:
            command += ["--set", setting]
        completed = subprocess.run(command, capture_output=True, text=True, check=True)
        return yaml.safe_load(completed.stdout)["text"]

    def test_a_console_without_profiles_is_warned_that_it_reads_no_table(self) -> None:
        notes = self.notes(MINIMAL)
        self.assertIn("WARNING: console.storage names no storage profile", notes)
        self.assertIn("read none of their tables", notes)

    def test_a_console_whose_profiles_miss_this_domains_destination_is_warned(self) -> None:
        notes = self.notes(DEFAULT, "controlPlane.settings.defaultDestinationUri=s3://other-lake/")
        self.assertIn("WARNING: no console.storage profile serves this domain's destination", notes)
        self.assertIn("a prefix of s3://other-lake/ (the tables live under s3://other-lake/midstream/)", notes)

    def test_a_profile_covering_the_destination_or_its_tables_needs_no_warning(self) -> None:
        for values, settings in (
            (DEFAULT, ()),
            (S3, ()),
            (AZURE, ()),
            # A scope as long as the tables' own prefix serves them too.
            (DEFAULT, ("console.storage[0].scope=gs://example-lake/midstream/midstream/",)),
            # No destination yet: nothing to compare.
            (DEFAULT, ("controlPlane.settings.defaultDestinationUri=",)),
            # A console alone serves other releases' domains, whose destinations it cannot see.
            (CONSOLE_ONLY, ()),
        ):
            with self.subTest(values=values.name, settings=settings):
                self.assertNotIn("WARNING", self.notes(values, *settings))

    def test_the_destination_and_prefix_are_compared_as_the_control_plane_gets_them(self) -> None:
        # Trimmed of whitespace, as the control plane's variables carry them: a profile serving them is no miss.
        for settings in (
            ("controlPlane.settings.defaultDestinationUri= gs://example-lake/midstream ",),
            ("controlPlane.settings.storagePrefix= /lake/ ", "console.storage[0].scope=gs://example-lake/midstream/lake/"),
        ):
            with self.subTest(settings=settings):
                self.assertNotIn("WARNING", self.notes(DEFAULT, *settings))
        notes = self.notes(DEFAULT, "controlPlane.settings.defaultDestinationUri= s3://other-lake/ ")
        self.assertIn("a prefix of s3://other-lake/ (the tables live under s3://other-lake/midstream/)", notes)

    def test_a_blank_destination_is_no_destination(self) -> None:
        notes = self.notes(DEFAULT, "controlPlane.settings.defaultDestinationUri=   ")
        self.assertIn("No destination is configured yet", notes)
        self.assertNotIn("WARNING", notes)
        self.assertNotIn("No destination is configured yet", self.notes(DEFAULT))

    def test_a_scope_that_only_shares_a_name_with_the_destination_does_not_serve_it(self) -> None:
        notes = self.notes(DEFAULT, "console.storage[0].scope=gs://example-lake/mid/")
        self.assertIn("WARNING: no console.storage profile serves this domain's destination", notes)


if __name__ == "__main__":
    unittest.main()
