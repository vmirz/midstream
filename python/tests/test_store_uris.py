"""Object URIs per scheme and the destinations a table may be created under (`lakehouse/store/uris.py`).

Every scheme parses into its container, key and (Azure) account and rebuilds to the same text; every
destination pattern of docs/design/storage-backends.md §6.5 accepts and refuses the edge cases of its
backend's naming rules; a refusal names the form a person should write instead.
"""

from __future__ import annotations

import unittest

from midstream import config
from midstream.lakehouse.store import join, parse_object_uri, scheme_of
from midstream.lakehouse.store.uris import (
    AZURE_DESTINATION_PATTERN,
    DESTINATION_MAX_LENGTH,
    FILE_DESTINATION_PATTERN,
    GCS_DESTINATION_PATTERN,
    S3_DESTINATION_PATTERN,
    SCHEMES,
    ObjectUri,
    destination_problem,
    is_destination_uri,
    normalize_destination_uri,
    require_object_uri,
    require_prefix_uri,
)

AZURE_HOST = ".dfs.core.windows.net"


class ParseTests(unittest.TestCase):
    def test_every_scheme_parses_and_rebuilds(self) -> None:
        cases = {
            "gs://lake/midstream/mat-1/orders/data/a.parquet": ObjectUri("gs", "lake", "midstream/mat-1/orders/data/a.parquet"),
            "s3://my.lake-1/prod/x.json": ObjectUri("s3", "my.lake-1", "prod/x.json"),
            f"abfss://lake@acme{AZURE_HOST}/midstream/t/x": ObjectUri("abfss", "lake", "midstream/t/x", account="acme"),
            "file:///tmp/lake/x.json": ObjectUri("file", "", "tmp/lake/x.json"),
        }
        for uri, expected in cases.items():
            with self.subTest(uri=uri):
                parsed = parse_object_uri(uri)
                self.assertEqual(parsed, expected)
                self.assertEqual(parsed.uri, uri)
                self.assertEqual(scheme_of(uri), expected.scheme)
        self.assertEqual(SCHEMES, ("gs", "s3", "abfss", "file"))

    def test_a_bucket_root_has_an_empty_key(self) -> None:
        for uri in ("gs://lake", "gs://lake/", "s3://lake/", f"abfss://lake@acme{AZURE_HOST}/", f"abfss://lake@acme{AZURE_HOST}"):
            with self.subTest(uri=uri):
                parsed = parse_object_uri(uri)
                self.assertEqual(parsed.key, "")
                self.assertTrue(parsed.uri.endswith("/"))
        self.assertEqual(parse_object_uri("gs://lake").uri, "gs://lake/")
        self.assertEqual(parse_object_uri("file:///").key, "")

    def test_a_key_keeps_characters_a_url_parser_would_take_apart(self) -> None:
        for key in ("a?b=c", "x#frag", "dir/with space/f", "trailing/"):
            with self.subTest(key=key):
                parsed = parse_object_uri(f"gs://lake/{key}")
                self.assertEqual(parsed.key, key)
                self.assertEqual(parsed.uri, f"gs://lake/{key}")

    def test_unknown_schemes_and_malformed_uris_are_refused(self) -> None:
        refused = [
            "",
            "lake/key",
            "/tmp/x",
            "gcs://lake/x",
            "GS://lake/x",
            "http://lake/x",
            "s3a://lake/x",
            "gs:///key",
            "s3://",
            "gs://la ke/x",
            "gs://lake:443/x",
            "gs://user@lake/x",
            "abfss://lake/x",
            f"abfss://@acme{AZURE_HOST}/x",
            f"abfss://lake@{AZURE_HOST}/x",
            "abfss://lake@acme.blob.core.windows.net/x",
            f"abfss://lake@Acme{AZURE_HOST}/x",
            f"abfss://lake@acme{AZURE_HOST}:443/x",
            "file://host/tmp/x",
            "file:/tmp/x",
            "file:tmp",
        ]
        for uri in refused:
            with self.subTest(uri=uri):
                with self.assertRaises(ValueError):
                    parse_object_uri(uri)

    def test_scheme_of_refuses_text_that_is_no_uri(self) -> None:
        for value in ("lake/x", "ftp://lake/x", "Gs://lake"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    scheme_of(value)

    def test_an_object_needs_a_key_and_a_store_its_own_scheme(self) -> None:
        self.assertEqual(require_object_uri("gs://lake/x", scheme="gs").key, "x")
        with self.assertRaisesRegex(ValueError, "names a bucket"):
            require_object_uri("gs://lake/")
        with self.assertRaisesRegex(ValueError, "names a directory"):
            require_object_uri("file:///")
        with self.assertRaisesRegex(ValueError, "cannot serve"):
            require_object_uri("s3://lake/x", scheme="gs")
        self.assertEqual(require_prefix_uri("gs://lake/", scheme="gs").key, "")


class JoinTests(unittest.TestCase):
    def test_joins_with_single_slashes(self) -> None:
        cases = [
            (("gs://lake", "midstream", "mat-1"), "gs://lake/midstream/mat-1"),
            (("gs://lake/", "/midstream/", "mat-1/"), "gs://lake/midstream/mat-1"),
            (("gs://lake/x/", "/y/", "z"), "gs://lake/x/y/z"),
            (("gs://lake", "a/b", "c"), "gs://lake/a/b/c"),
            (("gs://lake", "", "/", "x"), "gs://lake/x"),
            (("gs://lake",), "gs://lake"),
            (("file:///", "tmp", "lake"), "file:///tmp/lake"),
            (("file:///tmp/", "lake"), "file:///tmp/lake"),
            (("gs://", "lake", "x"), "gs://lake/x"),
            ((f"abfss://lake@acme{AZURE_HOST}", "t"), f"abfss://lake@acme{AZURE_HOST}/t"),
        ]
        for args, expected in cases:
            with self.subTest(args=args):
                self.assertEqual(join(*args), expected)


class DestinationTests(unittest.TestCase):
    def test_gcs_destinations(self) -> None:
        accepted = ["gs://my-lake", "gs://ab", "gs://lake.example_1-x", "gs://lake/prefix", "gs://lake/a/b/c.d", "gs://lake/"]
        refused = ["gs://a", "gs://My-Lake", "gs://-lake", "gs://lake-", "gs://lake_", "gs://la ke", "gs://", "gs:/lake"]
        self._check("gs", accepted, refused)

    def test_s3_destinations(self) -> None:
        accepted = ["s3://abc", "s3://my-lake", "s3://my.lake.1", "s3://" + "a" * 63, "s3://lake/prod/x", "s3://lake/"]
        refused = [
            "s3://ab",
            "s3://" + "a" * 64,
            "s3://My-Lake",
            "s3://my_lake",
            "s3://-lake",
            "s3://lake-",
            "s3://.lake",
            "s3://lake.",
            "s3://",
        ]
        self._check("s3", accepted, refused)

    def test_azure_destinations(self) -> None:
        accepted = [
            f"abfss://lake@acme{AZURE_HOST}",
            f"abfss://abc@abc{AZURE_HOST}",
            f"abfss://my-lake-1@acme2{AZURE_HOST}/midstream",
            f"abfss://{'a' * 63}@{'b' * 24}{AZURE_HOST}",
            f"abfss://a-b-c@acme{AZURE_HOST}/x/y",
        ]
        refused = [
            f"abfss://ab@acme{AZURE_HOST}",  # container of 2
            f"abfss://{'a' * 64}@acme{AZURE_HOST}",  # container of 64
            f"abfss://-lake@acme{AZURE_HOST}",
            f"abfss://lake-@acme{AZURE_HOST}",
            f"abfss://la--ke@acme{AZURE_HOST}",
            f"abfss://Lake@acme{AZURE_HOST}",
            f"abfss://la_ke@acme{AZURE_HOST}",
            f"abfss://lake@ac{AZURE_HOST}",  # account of 2
            f"abfss://lake@{'b' * 25}{AZURE_HOST}",  # account of 25
            f"abfss://lake@ac-me{AZURE_HOST}",
            f"abfss://lake@Acme{AZURE_HOST}",
            "abfss://lake@acme.blob.core.windows.net",
            "abfss://lake@acme.dfs.core.windows.net.evil.example",
            "abfss://acme.dfs.core.windows.net/lake",
        ]
        self._check("abfss", accepted, refused)
        problem = destination_problem(f"abfss://la--ke@acme{AZURE_HOST}")
        self.assertIn("abfss:// destinations look like abfss://<container>@<account>.dfs.core.windows.net[/<prefix>]", problem)

    def test_file_destinations_only_where_allowed(self) -> None:
        for value in ("file:///tmp/lake", "file:///l"):
            with self.subTest(value=value):
                self.assertFalse(is_destination_uri(value))
                self.assertIn("tests only", destination_problem(value))
                self.assertTrue(is_destination_uri(value, allow_file=True))
                self.assertIsNotNone(FILE_DESTINATION_PATTERN.match(value))
        for value in ("file://tmp/lake", "file:///", "file:/tmp"):
            with self.subTest(value=value):
                self.assertFalse(is_destination_uri(value, allow_file=True))
        # Every refusal names the forms that would be accepted, file:// only where it is.
        self.assertNotIn("file:///", destination_problem("ftp://lake"))
        self.assertIn("file:///<absolute path>", destination_problem("ftp://lake", allow_file=True))

    def test_length_is_capped(self) -> None:
        at_cap = "gs://lake/" + "x" * (DESTINATION_MAX_LENGTH - len("gs://lake/"))
        self.assertEqual(len(at_cap), DESTINATION_MAX_LENGTH)
        self.assertTrue(is_destination_uri(at_cap))
        over = at_cap + "x"
        self.assertFalse(is_destination_uri(over))
        self.assertIn(f"at most {DESTINATION_MAX_LENGTH} characters", destination_problem(over))

    def test_refusals_say_what_to_write_instead(self) -> None:
        cases = {
            None: "no destination is set",
            "   ": "no destination is set",
            "lake/prefix": "is not a storage URI",
            "gs://lake/a b": "whitespace",
            "GS://lake": "write the scheme in lowercase: gs://",
            "gcs://lake": "gs://<bucket>[/<prefix>]",
            "s3a://lake": "s3a:// destinations are written s3://<bucket>[/<prefix>]",
            "abfs://lake@acme.dfs.core.windows.net": "abfss://<container>@<account>.dfs.core.windows.net",
            "wasbs://lake@acme.blob.core.windows.net": "abfss://",
            "https://storage.googleapis.com/lake": "not by web address",
            "ftp://lake": "ftp:// is not a storage scheme",
            "s3://ab": "s3:// destinations look like s3://<bucket>[/<prefix>]",
        }
        for value, expected in cases.items():
            with self.subTest(value=value):
                problem = destination_problem(value)
                self.assertIsNotNone(problem)
                self.assertIn(expected, problem)
                self.assertFalse(is_destination_uri(value))
                self.assertNotIn("\n", problem)
        for value in ("gs://lake", "s3://lake", f"abfss://lake@acme{AZURE_HOST}"):
            with self.subTest(value=value):
                self.assertIsNone(destination_problem(value))

    def test_normalize_behaves_as_config_does(self) -> None:
        for value in (None, "", "   ", "gs://lake", " gs://lake/ ", "gs://lake///", "s3://lake/p/", "/", "///", "file:///tmp/x/"):
            with self.subTest(value=value):
                self.assertEqual(normalize_destination_uri(value), config.normalize_destination_uri(value))
        self.assertEqual(normalize_destination_uri(" s3://lake/prod/ "), "s3://lake/prod")

    def test_config_validates_with_these_rules(self) -> None:
        self.assertIs(config.destination_problem, destination_problem)
        self.assertIs(config.is_destination_uri, is_destination_uri)
        self.assertIsNone(config.ControlPlaneConfig(allow_file_destinations=True).destination_problem("file:///tmp/lake"))
        self.assertIn("tests only", config.ControlPlaneConfig().destination_problem("file:///tmp/lake"))

    def _check(self, scheme: str, accepted: list[str], refused: list[str]) -> None:
        pattern = {"gs": GCS_DESTINATION_PATTERN, "s3": S3_DESTINATION_PATTERN, "abfss": AZURE_DESTINATION_PATTERN}[scheme]
        for value in accepted:
            with self.subTest(accepted=value):
                self.assertIsNotNone(pattern.match(value))
                self.assertIsNone(destination_problem(value))
                self.assertTrue(is_destination_uri(normalize_destination_uri(value)))
        for value in refused:
            with self.subTest(refused=value):
                self.assertIsNone(pattern.match(value))
                self.assertFalse(is_destination_uri(value))
                self.assertFalse(is_destination_uri(value, allow_file=True))


if __name__ == "__main__":
    unittest.main()
