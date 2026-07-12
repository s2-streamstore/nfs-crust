from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


MODULE_PATH = Path(__file__).with_name("cleanup.py")
SPEC = importlib.util.spec_from_file_location("bench_cleanup", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
cleanup = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cleanup)


def state() -> dict:
    run_token = "1" * 32
    account_id = "123456789012"
    resource_keys = {
        "file_system",
        "mount_target",
        "instance",
        "security_group",
        "role",
        "instance_profile",
        "transfer_bucket",
    }
    return {
        "run_id": "test-run",
        "run_token": run_token,
        "account_id": account_id,
        "region": "us-east-1",
        "vpc_id": "vpc-1234abcd",
        "transfer_bucket": f"nfs-crust-bench-{account_id}-{run_token}",
        "attempted": {key: key == "transfer_bucket" for key in resource_keys},
        "owned": {key: False for key in resource_keys},
    }


class FakeAws:
    def __init__(self, tag_count: str) -> None:
        self.tag_count = tag_count

    def run(self, *args: str, check: bool = True) -> str:
        self.args = args
        self.check = check
        return self.tag_count


class CleanupOwnershipTests(unittest.TestCase):
    def test_state_rejects_bucket_not_derived_from_run_identity(self) -> None:
        value = state()
        value["transfer_bucket"] = "important-existing-bucket"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "transfer bucket"):
                cleanup.load_state(path)

    def test_attempted_bucket_requires_matching_run_token_tag(self) -> None:
        value = state()
        self.assertFalse(cleanup.named_resource_safe(FakeAws(""), value, "transfer_bucket"))
        self.assertTrue(cleanup.named_resource_safe(FakeAws("1"), value, "transfer_bucket"))


if __name__ == "__main__":
    unittest.main()
