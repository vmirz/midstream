"""The chart's domains: one release per domain, the shared console once, and how each is reached.

A release deploys a domain - its control plane, maintenance and tail apps - the shared console, or
both (handbook 20). The console's registry names every domain it serves and how to reach each;
routes expose a domain's API, the console, and redirects from the old person-facing hostnames.
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from chart_support import CHART, NAMESPACE, RELEASE, documents, render, require_helm  # noqa: E402

CI = CHART / "ci"
DEFAULT = CI / "test-values.yaml"
DOMAIN_ONLY = CI / "domain-only-values.yaml"
CONSOLE_ONLY = CI / "console-only-values.yaml"
MINIMAL = CI / "minimal-values.yaml"


def env_of(document: dict, container: str | None = None) -> dict[str, dict]:
    """A Deployment's or a KasprApp's container environment, by variable name."""
    spec = document["spec"]
    if document["kind"] == "KasprApp":
        entries = spec["template"]["kasprContainer"]["env"]
    else:
        containers = spec["template"]["spec"]["containers"]
        entries = next(item for item in containers if container is None or item["name"] == container)["env"]
    return {entry["name"]: entry for entry in entries}


def registry(values: Path) -> list[dict]:
    config_map = next(item for item in documents("ConfigMap", values) if item["metadata"]["name"].endswith("-console-domains"))
    return json.loads(config_map["data"]["domains.json"])


def render_failure(values: Path, *settings: str) -> str:
    """The error `helm template` stops with, for values the chart must refuse."""
    require_helm()
    command = ["helm", "template", RELEASE, str(CHART), "-n", NAMESPACE, "-f", str(values)]
    for setting in settings:
        command += ["--set", setting]
    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.returncode == 0:
        raise AssertionError(f"the chart rendered {settings}")
    return completed.stderr


class TopologyTests(unittest.TestCase):
    def test_a_console_only_release_deploys_the_console_and_nothing_else(self) -> None:
        kinds = sorted(document["kind"] for document in render(CONSOLE_ONLY))
        self.assertEqual(kinds, ["ConfigMap", "Deployment", "Ingress", "Service"])

    def test_a_domain_only_release_deploys_no_console(self) -> None:
        names = [document["metadata"]["name"] for document in render(DOMAIN_ONLY)]
        self.assertFalse([name for name in names if "console" in name], names)
        self.assertIn("lake-control-plane", names)
        self.assertNotIn("lake-tail", names, "tail.enabled: false")

    def test_the_maintenance_and_tail_apps_need_the_control_plane(self) -> None:
        apps = [document["metadata"]["name"] for document in documents("KasprApp", DEFAULT)]
        self.assertEqual(sorted(apps), ["midstream-control-plane", "midstream-maintenance", "midstream-tail"])


class DomainIdentityTests(unittest.TestCase):
    def test_the_control_plane_is_told_which_domain_it_is(self) -> None:
        env = env_of(next(item for item in documents("KasprApp", DEFAULT) if item["metadata"]["name"] == "midstream-control-plane"))
        self.assertEqual(env["DOMAIN_ID"]["value"], "domain_a")
        self.assertEqual(env["DOMAIN_NAME"]["value"], "Domain A")

    def test_an_unnamed_domain_takes_the_release_name(self) -> None:
        env = env_of(next(item for item in documents("KasprApp", MINIMAL) if item["metadata"]["name"] == "midstream-control-plane"))
        self.assertEqual((env["DOMAIN_ID"]["value"], env["DOMAIN_NAME"]["value"]), ("midstream", "midstream"))

    def test_the_federation_key_comes_from_the_auth_secret(self) -> None:
        env = env_of(next(item for item in documents("KasprApp", DEFAULT) if item["metadata"]["name"] == "midstream-control-plane"))
        for name, key in (("AUTH_FEDERATION_KEY", "federation-key"), ("AUTH_PREVIOUS_FEDERATION_KEY", "previous-federation-key")):
            reference = env[name]["valueFrom"]["secretKeyRef"]
            self.assertEqual((reference["name"], reference["key"], reference["optional"]), ("midstream-auth", key, True))

    def test_every_app_and_runtime_reads_the_protection_key_from_the_auth_secret(self) -> None:
        # The control plane checks it and renders its id into runtimes; runtimes and the tail app hash
        # with it; the maintenance app will (handbook 22). Optional, so a deployment without it runs.
        apps = {item["metadata"]["name"]: item for item in documents("KasprApp", DEFAULT)}
        for name in ("midstream-control-plane", "midstream-tail", "midstream-maintenance"):
            with self.subTest(app=name):
                reference = env_of(apps[name])["PROTECTION_KEY"]["valueFrom"]["secretKeyRef"]
                self.assertEqual((reference["name"], reference["key"], reference["optional"]), ("midstream-auth", "protection-key", True))
        import yaml

        runtime_spec = yaml.safe_load(env_of(apps["midstream-control-plane"])["CONTROL_PLANE_RUNTIME_APP_SPEC"]["value"])
        runtime_env = {item["name"]: item for item in runtime_spec["template"]["kasprContainer"]["env"]}
        reference = runtime_env["PROTECTION_KEY"]["valueFrom"]["secretKeyRef"]
        self.assertEqual((reference["name"], reference["key"], reference["optional"]), ("midstream-auth", "protection-key", True))

    def test_a_domain_accepts_the_consoles_clients_unless_it_names_its_own(self) -> None:
        env = env_of(next(item for item in documents("KasprApp", DEFAULT) if item["metadata"]["name"] == "midstream-control-plane"))
        self.assertEqual(env["AUTH_SSO_ENTRA_TENANT_ID"]["value"], "00000000-0000-0000-0000-000000000000")
        self.assertEqual(env["AUTH_SSO_ENTRA_CLIENT_ID"]["value"], "11111111-1111-1111-1111-111111111111")
        self.assertEqual(env["AUTH_SSO_GITHUB_CLIENT_SECRET"]["valueFrom"]["secretKeyRef"], {"name": "console-sso", "key": "github"})
        self.assertEqual(env["AUTH_SSO_GOOGLE_HOSTED_DOMAINS"]["value"], "example.com")
        self.assertEqual(env["AUTH_SSO_OIDC_ISSUER"]["value"], "https://dex.example.com")
        self.assertEqual(env["CONTROL_PLANE_SSO_FIRST_SIGN_IN"]["value"], "invited")
        self.assertEqual(env["CONTROL_PLANE_SSO_ALLOWED_EMAIL_DOMAINS"]["value"], "example.com")
        self.assertEqual(env["CONTROL_PLANE_PASSWORD_SIGN_IN_ENABLED"]["value"], "false")

    def test_without_single_sign_on_the_settings_are_off_and_people_sign_in_with_passwords(self) -> None:
        env = env_of(next(item for item in documents("KasprApp", MINIMAL) if item["metadata"]["name"] == "midstream-control-plane"))
        self.assertFalse([name for name in env if name.startswith("AUTH_SSO_")])
        for provider in ("ENTRA", "GITHUB", "GOOGLE", "OIDC"):
            self.assertEqual(env[f"CONTROL_PLANE_SSO_{provider}_ENABLED"]["value"], "false")
        self.assertEqual(env["CONTROL_PLANE_SSO_FIRST_SIGN_IN"]["value"], "open")
        self.assertEqual(env["CONTROL_PLANE_PASSWORD_SIGN_IN_ENABLED"]["value"], "true")


class RegistryTests(unittest.TestCase):
    def test_a_local_domain_is_reached_through_its_services_and_another_through_its_api(self) -> None:
        self.assertEqual(
            registry(DEFAULT),
            [
                {
                    "id": "domain_a",
                    "name": "Domain A",
                    "local": True,
                    "settingsHome": True,
                    "controlPlaneUrl": "http://midstream-control-plane-app-api:6065",
                    "tailUrl": "http://midstream-tail-app-api:6065",
                },
                {
                    "id": "domain_b",
                    "name": "Domain B",
                    "local": False,
                    "settingsHome": False,
                    "controlPlaneUrl": "https://domain-b-api.example.com",
                    "tailUrl": "https://domain-b-api.example.com",
                },
            ],
        )

    def test_a_release_with_its_own_domain_serves_it_alone_by_default(self) -> None:
        self.assertEqual(
            registry(MINIMAL),
            [
                {
                    "id": "midstream",
                    "name": "midstream",
                    "local": True,
                    "settingsHome": True,
                    "controlPlaneUrl": "http://midstream-control-plane-app-api:6065",
                    "tailUrl": "http://midstream-tail-app-api:6065",
                }
            ],
        )

    def test_the_registry_never_holds_a_token_and_each_domains_reaches_the_console_from_a_secret(self) -> None:
        console = next(item for item in documents("Deployment", DEFAULT) if item["metadata"]["name"] == "midstream-console")
        env = env_of(console, "console")
        self.assertNotIn("token", json.dumps(registry(DEFAULT)).lower())
        self.assertEqual(env["CONSOLE_DOMAIN_DOMAIN_A_SERVICE_TOKEN"]["valueFrom"]["secretKeyRef"]["name"], "midstream-auth")
        self.assertEqual(
            env["CONSOLE_DOMAIN_DOMAIN_B_SERVICE_TOKEN"]["valueFrom"]["secretKeyRef"],
            {"name": "domain-b-auth", "key": "service-token"},
        )
        self.assertEqual(env["CONSOLE_DOMAINS_FILE"]["value"], "/etc/midstream/domains.json")
        self.assertEqual(env["CONSOLE_URL"]["value"], "https://midstream.example.com")
        for gone in ("CONTROL_PLANE_URL", "CONSOLE_TAIL_URL", "CONSOLE_SERVICE_TOKEN"):
            self.assertNotIn(gone, env)

    def test_the_console_restarts_when_its_domains_change(self) -> None:
        console = next(item for item in documents("Deployment", DEFAULT) if item["metadata"]["name"] == "midstream-console")
        self.assertIn("checksum/domains", console["spec"]["template"]["metadata"]["annotations"])

    def test_the_consoles_clients_come_from_secrets(self) -> None:
        console = next(item for item in documents("Deployment", DEFAULT) if item["metadata"]["name"] == "midstream-console")
        env = env_of(console, "console")
        for provider in ("ENTRA", "GITHUB", "GOOGLE", "OIDC"):
            self.assertIn(f"CONSOLE_SSO_{provider}_CLIENT_ID", env)
            self.assertIn("secretKeyRef", env[f"CONSOLE_SSO_{provider}_CLIENT_SECRET"]["valueFrom"])
        self.assertEqual(env["CONSOLE_SSO_OIDC_SCOPES"]["value"], "openid email profile")


class RouteTests(unittest.TestCase):
    def routes(self, values: Path) -> dict[str, dict]:
        return {item["metadata"]["name"]: item for item in documents("HTTPRoute", values)}

    def test_a_domains_api_sends_the_tail_to_the_tail_app_and_the_rest_to_the_control_plane(self) -> None:
        api = self.routes(DEFAULT)["midstream-api"]
        self.assertEqual(api["spec"]["hostnames"], ["domain-a-api.example.com"])
        rules = [(rule["matches"][0]["path"]["value"], rule["backendRefs"][0]["name"]) for rule in api["spec"]["rules"]]
        self.assertEqual(rules, [("/tail/", "midstream-tail-app-api"), ("/", "midstream-control-plane-app-api")])

    def test_the_console_has_a_hostname_of_its_own(self) -> None:
        console = self.routes(DEFAULT)["midstream-console"]
        self.assertEqual(console["spec"]["hostnames"], ["midstream.example.com"])
        self.assertEqual(console["spec"]["rules"][0]["backendRefs"][0]["name"], "midstream-console")

    def test_an_old_hostname_redirects_into_its_domain_keeping_the_path(self) -> None:
        redirect = self.routes(DEFAULT)["midstream-redirect"]
        self.assertEqual(redirect["spec"]["hostnames"], ["domain-a-midstream.example.com"])
        self.assertEqual(
            redirect["spec"]["rules"][0]["filters"][0]["requestRedirect"],
            {
                "scheme": "https",
                "hostname": "midstream.example.com",
                "path": {"type": "ReplacePrefixMatch", "replacePrefixMatch": "/d/domain_a/"},
                "statusCode": 301,
            },
        )

    def test_with_an_ingress_the_redirect_is_ingress_nginxs_annotation(self) -> None:
        ingresses = {item["metadata"]["name"]: item for item in documents("Ingress", DOMAIN_ONLY)}
        self.assertEqual(sorted(ingresses), ["lake-api", "lake-redirect"])
        annotation = ingresses["lake-redirect"]["metadata"]["annotations"]["nginx.ingress.kubernetes.io/permanent-redirect"]
        self.assertEqual(annotation, "https://midstream.example.com/d/lake_eu$request_uri")
        self.assertEqual([rule["host"] for rule in ingresses["lake-redirect"]["spec"]["rules"]], ["lake.example.com"])
        paths = ingresses["lake-api"]["spec"]["rules"][0]["http"]["paths"]
        self.assertEqual([path["path"] for path in paths], ["/"], "no tail app in this release")

    def test_a_console_only_release_routes_its_console(self) -> None:
        ingress = documents("Ingress", CONSOLE_ONLY)[0]
        self.assertEqual(ingress["spec"]["rules"][0]["host"], "midstream.example.com")


class RefusalTests(unittest.TestCase):
    """Values that cannot work stop the install with a message naming what to fix."""

    def test_exactly_one_domain_is_the_settings_home(self) -> None:
        self.assertIn("exactly one settingsHome", render_failure(DEFAULT, "console.domains[1].settingsHome=true"))
        self.assertIn("exactly one settingsHome", render_failure(DEFAULT, "console.domains[0].settingsHome=false"))

    def test_domain_ids_are_well_formed_unique_and_not_reserved(self) -> None:
        self.assertIn("lowercase letters", render_failure(DEFAULT, "domain.id=Domain-A"))
        self.assertIn("reserved", render_failure(DEFAULT, "console.domains[1].id=catalog"))
        self.assertIn("reserved", render_failure(DEFAULT, "console.domains[1].id=domain_a_iceberg"))
        self.assertIn("named twice", render_failure(DEFAULT, "console.domains[1].id=domain_a"))

    def test_a_local_domain_is_this_releases_and_another_names_its_secret(self) -> None:
        self.assertIn("must be this release's domain.id", render_failure(DEFAULT, "domain.id=other"))
        self.assertIn("serviceTokenSecret", render_failure(DEFAULT, "console.domains[1].serviceTokenSecret=null"))
        self.assertIn("controlPlaneUrl is required", render_failure(DEFAULT, "console.domains[1].controlPlaneUrl="))
        self.assertIn("has no control plane", render_failure(CONSOLE_ONLY, "console.domains[0].local=true"))

    def test_a_console_alone_names_its_domains(self) -> None:
        self.assertIn("console.domains is required", render_failure(MINIMAL, "controlPlane.enabled=false"))

    def test_a_domain_checks_github_tokens_with_the_apps_secret(self) -> None:
        self.assertIn("clientSecretSecret", render_failure(DOMAIN_ONLY, "auth.sso.github.clientSecretSecret=null"))

    def test_redirects_need_the_consoles_url(self) -> None:
        self.assertIn("console.url is required", render_failure(DOMAIN_ONLY, "console.url="))


if __name__ == "__main__":
    unittest.main()
