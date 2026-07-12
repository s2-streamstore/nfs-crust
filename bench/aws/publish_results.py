#!/usr/bin/env python3
"""Publish review-facing benchmark data while keeping infrastructure evidence private."""

from __future__ import annotations

import argparse
import importlib.util
import ipaddress
import json
import re
import shutil
import tempfile
from pathlib import Path
from types import ModuleType
from typing import Any


DATA_FILES = ("summary.json",)

SENSITIVE_KEYS = frozenset(
    {
        "accountid",
        "amiid",
        "awsaccountid",
        "bucket",
        "bucketname",
        "cleanupaccountid",
        "creatoraccountid",
        "dnsname",
        "endpoint",
        "filesystemid",
        "groupid",
        "hostname",
        "instanceid",
        "instanceprofilename",
        "ipaddress",
        "mounttargetid",
        "networkinterfaceid",
        "ownerid",
        "privateip",
        "privateipaddress",
        "publicip",
        "publicipaddress",
        "resourcearn",
        "rolename",
        "securitygroupid",
        "snapshotid",
        "subnetid",
        "tlsservername",
        "transferbucket",
        "volumeid",
        "vpcid",
    }
)
AWS_RESOURCE_ID = re.compile(
    r"\b(?:acl|ami|eipalloc|eipassoc|eni|fs|fsap|fsmt|igw|i|nat|rtb|sg|"
    r"snap|subnet|vol|vpc|vpce)-[0-9a-f]{8,17}\b",
    re.IGNORECASE,
)
AWS_ARN = re.compile(r"\barn:(?:aws|aws-cn|aws-us-gov):[^\s\"'<>]+")
AWS_ACCOUNT_ID = re.compile(r"(?<!\d)\d{12}(?!\d)")
IPV4_ADDRESS = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")
IPV6_ADDRESS = re.compile(
    r"(?<![0-9a-z_:])(?:[0-9a-f]{0,4}:){2,7}[0-9a-f]{0,4}(?![0-9a-z_:])",
    re.IGNORECASE,
)
OMIT = object()


def contains_sensitive_string(value: str) -> bool:
    if (
        AWS_ARN.search(value)
        or AWS_RESOURCE_ID.search(value)
        or AWS_ACCOUNT_ID.search(value)
    ):
        return True
    for pattern in (IPV4_ADDRESS, IPV6_ADDRESS):
        for match in pattern.finditer(value):
            try:
                ipaddress.ip_address(match.group(0))
            except ValueError:
                continue
            return True
    return False


def sanitize_json(value: Any, key: str | None = None) -> Any:
    normalized_key = re.sub(r"[^a-z0-9]", "", key.lower()) if key else None
    if normalized_key in SENSITIVE_KEYS:
        return OMIT
    if isinstance(value, dict):
        sanitized = {}
        for item_key, item in value.items():
            sanitized_item = sanitize_json(item, item_key)
            if sanitized_item is not OMIT:
                sanitized[item_key] = sanitized_item
        return sanitized
    if isinstance(value, list):
        sanitized = [sanitize_json(item) for item in value]
        return [item for item in sanitized if item is not OMIT]
    if isinstance(value, str) and contains_sensitive_string(value):
        return OMIT
    return value


def sanitize_document(value: Any) -> Any:
    sanitized = sanitize_json(value)
    if sanitized is OMIT:
        raise RuntimeError("benchmark publication document contains only private metadata")
    return sanitized


def document_is_safe(value: Any) -> bool:
    return sanitize_json(value) == value


def report_is_safe(value: str) -> bool:
    return not contains_sensitive_string(value)


def copy_sanitized_json(source: Path, destination: Path) -> None:
    value = json.loads(source.read_text(encoding="utf-8"))
    sanitized = sanitize_document(value)
    if sanitized == value:
        shutil.copy2(source, destination)
        return
    destination.write_text(
        json.dumps(sanitized, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def assert_publication_safe(root: Path) -> None:
    entries = {path.name for path in root.iterdir()}
    if entries != {"summary.json", "report.fragment.html"}:
        raise RuntimeError("published benchmark directory has unexpected files")
    summary = json.loads((root / "summary.json").read_text(encoding="utf-8"))
    if not document_is_safe(summary):
        raise RuntimeError("published benchmark data still contains infrastructure identifiers")

    report = (root / "report.fragment.html").read_text(encoding="utf-8")
    if not report_is_safe(report):
        raise RuntimeError("published benchmark report still contains infrastructure identifiers")


def load_report_module(root: Path) -> ModuleType:
    module_path = root / "bench" / "report" / "generate.py"
    spec = importlib.util.spec_from_file_location("benchmark_report", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load report generator from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def publish_results(
    artifact_root: Path, result_root: Path, report_module: ModuleType
) -> None:
    if not result_root.is_dir() or any(result_root.iterdir()):
        raise RuntimeError(f"result directory must exist and be empty: {result_root}")

    remote_data = artifact_root / "run" / "run"
    missing = [name for name in DATA_FILES if not (remote_data / name).is_file()]
    if missing:
        raise RuntimeError(f"missing benchmark data: {', '.join(missing)}")

    stage = Path(
        tempfile.mkdtemp(prefix=f".{result_root.name}.publish-", dir=result_root.parent)
    )
    try:
        copy_sanitized_json(remote_data / "summary.json", stage / "summary.json")

        fragment, _ = report_module.generate_report(artifact_root)
        (stage / "report.fragment.html").write_text(fragment, encoding="utf-8")
        assert_publication_safe(stage)

        result_root.rmdir()
        stage.rename(result_root)
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-root", required=True, type=Path)
    parser.add_argument("--result-root", required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    publish_results(
        args.artifact_root.resolve(),
        args.result_root.resolve(),
        load_report_module(root),
    )


if __name__ == "__main__":
    main()
