import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("publish_results.py")
SPEC = importlib.util.spec_from_file_location("publish_results", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
PUBLISH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PUBLISH)


class FakeReport:
    @staticmethod
    def generate_report(root: Path) -> tuple[str, list[dict]]:
        assert (root / "provenance.json").read_text(encoding="utf-8") == "vpc-secret"
        return "<style>pretty</style><div>report</div>", [{"result": 1}]

class PublishResultsTests(unittest.TestCase):
    def test_publishes_only_data_and_rendered_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifacts = root / "artifacts"
            remote = artifacts / "run" / "run"
            result = root / "results" / "run-id"
            remote.mkdir(parents=True)
            result.mkdir(parents=True)
            (remote / "summary.json").write_text(
                json.dumps(
                    {
                        "metadata": {
                            "endpoint": "10.0.0.10:2049",
                            "tls_server_name": (
                                "fs-0123456789abcdef0.efs.us-east-1.amazonaws.com"
                            ),
                            "host": {"hostname": "ip-10-0-0-20"},
                        }
                    }
                ),
                encoding="utf-8",
            )
            (remote / "events.jsonl").write_text("private run log", encoding="utf-8")
            (remote / "raw-samples.jsonl.gz").write_bytes(b"private raw data")
            (artifacts / "provenance.json").write_text("vpc-secret", encoding="utf-8")
            (artifacts / "cleanup.json").write_text("resource-secret", encoding="utf-8")

            PUBLISH.publish_results(artifacts, result, FakeReport)

            self.assertEqual(
                sorted(path.name for path in result.iterdir()),
                ["report.fragment.html", "summary.json"],
            )
            self.assertFalse((result / "provenance.json").exists())
            self.assertFalse((result / "cleanup.json").exists())
            self.assertNotIn("vpc-secret", (result / "report.fragment.html").read_text())
            summary = json.loads((result / "summary.json").read_text(encoding="utf-8"))
            self.assertNotIn("endpoint", summary["metadata"])
            self.assertNotIn("tls_server_name", summary["metadata"])
            self.assertNotIn("hostname", summary["metadata"]["host"])
            self.assertFalse((result / "events.jsonl").exists())
            self.assertFalse((result / "raw-samples.jsonl.gz").exists())

    def test_omits_nested_identifiers(self) -> None:
        value = {
            "details": [
                "arn:aws:iam::123456789012:role/private",
                "endpoint 192.0.2.10 and subnet-0123456789abcdef0",
                "IPv6 2001:db8::1",
            ],
            "instance_id": "i-0123456789abcdef0",
            "OwnerId": 123456789012,
        }

        sanitized = PUBLISH.sanitize_json(value)

        self.assertNotIn("instance_id", sanitized)
        self.assertNotIn("OwnerId", sanitized)
        self.assertEqual(sanitized["details"], [])

    def test_rejects_nonempty_result_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = root / "result"
            result.mkdir()
            (result / "existing").touch()
            with self.assertRaisesRegex(RuntimeError, "must exist and be empty"):
                PUBLISH.publish_results(root / "artifacts", result, FakeReport)


if __name__ == "__main__":
    unittest.main()
