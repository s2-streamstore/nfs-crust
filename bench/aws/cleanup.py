#!/usr/bin/env python3
"""Remove resources owned by one benchmark run."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any


LIVE_INSTANCE_STATES = "pending,running,shutting-down,stopping,stopped"
PROJECT_FILTERS = [
    "Name=tag:Project,Values=nfs-crust",
    "Name=tag:Purpose,Values=benchmark",
]


class Aws:
    def run(self, *args: str, check: bool = True) -> str:
        result = subprocess.run(
            ["aws", *args],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        if check and result.returncode != 0:
            raise RuntimeError(f"AWS command failed: aws {' '.join(args)}")
        return result.stdout.strip() if result.returncode == 0 else ""

    def text(self, *args: str) -> str:
        return self.run(*args, "--output", "text")

    def count(self, *args: str, query: str) -> int:
        return int(self.text(*args, "--query", query))


def words(value: str) -> set[str]:
    return {word for word in value.split() if word and word != "None"}


def retry(attempts: int, delay: float, action: Any) -> Any:
    value = None
    for attempt in range(attempts):
        value = action()
        if value:
            return value
        if attempt + 1 < attempts:
            time.sleep(delay)
    return value


def load_state(path: Path) -> dict[str, Any]:
    state = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "run_id",
        "run_token",
        "account_id",
        "region",
        "vpc_id",
        "attempted",
        "owned",
    }
    if not required <= state.keys():
        raise ValueError("state file is missing required fields")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", state["run_id"]):
        raise ValueError("invalid run id")
    if not re.fullmatch(r"[0-9a-f]{32}", state["run_token"]):
        raise ValueError("invalid run token")
    if not re.fullmatch(r"[0-9]{12}", state["account_id"]):
        raise ValueError("invalid account id")
    if not re.fullmatch(r"vpc-[0-9a-f]+", state["vpc_id"]):
        raise ValueError("invalid VPC id")
    keys = {
        "file_system",
        "mount_target",
        "instance",
        "security_group",
        "role",
        "instance_profile",
        "transfer_bucket",
    }
    if set(state["attempted"]) != keys or set(state["owned"]) != keys:
        raise ValueError("state file has an invalid ownership shape")
    if any(
        type(value) is not bool
        for section in ("attempted", "owned")
        for value in state[section].values()
    ):
        raise ValueError("state ownership fields must be booleans")
    expected_bucket = f'nfs-crust-bench-{state["account_id"]}-{state["run_token"]}'
    if state.get("transfer_bucket") not in (None, "", expected_bucket):
        raise ValueError("transfer bucket does not match the run account and token")
    return state


def filters(state: dict[str, Any]) -> list[str]:
    return [
        *PROJECT_FILTERS,
        f'Name=tag:RunId,Values={state["run_id"]}',
        f'Name=tag:RunToken,Values={state["run_token"]}',
    ]


def has_tag(
    aws: Aws,
    service: str,
    action: str,
    resource_args: list[str],
    state: dict[str, Any],
) -> bool:
    query = f'length(Tags[?Key==`RunToken` && Value==`{state["run_token"]}`])'
    return aws.count(service, action, *resource_args, query=query) == 1


def instance_ids(aws: Aws, state: dict[str, Any]) -> set[str]:
    ids = words(
        aws.text(
            "ec2",
            "describe-instances",
            "--filters",
            *filters(state),
            f"Name=instance-state-name,Values={LIVE_INSTANCE_STATES}",
            "--query",
            "Reservations[].Instances[].InstanceId",
        )
    )
    if state["attempted"]["instance"]:
        ids |= words(
            aws.text(
                "ec2",
                "describe-instances",
                "--filters",
                f'Name=client-token,Values={state["run_token"]}',
                f"Name=instance-state-name,Values={LIVE_INSTANCE_STATES}",
                "--query",
                "Reservations[].Instances[].InstanceId",
            )
        )
    if state["owned"]["instance"] and state.get("instance_id"):
        ids.add(state["instance_id"])
    return ids


def file_system_ids(aws: Aws, state: dict[str, Any]) -> set[str]:
    candidates = words(
        aws.text(
            "efs",
            "describe-file-systems",
            "--creation-token",
            state["run_id"],
            "--query",
            "FileSystems[].FileSystemId",
        )
    )
    verified = {
        fs_id
        for fs_id in candidates
        if has_tag(aws, "efs", "describe-tags", ["--file-system-id", fs_id], state)
    }
    if state["owned"]["file_system"] and state.get("file_system_id"):
        verified.add(state["file_system_id"])
    return verified


def security_group_ids(aws: Aws, state: dict[str, Any]) -> set[str]:
    ids = words(
        aws.text(
            "ec2",
            "describe-security-groups",
            "--filters",
            *filters(state),
            "--query",
            "SecurityGroups[].GroupId",
        )
    )
    if state["attempted"]["security_group"]:
        ids |= words(
            aws.text(
                "ec2",
                "describe-security-groups",
                "--filters",
                f'Name=vpc-id,Values={state["vpc_id"]}',
                f'Name=group-name,Values=nfs-crust-bench-{state["run_id"]}',
                f'Name=tag:RunToken,Values={state["run_token"]}',
                "--query",
                "SecurityGroups[].GroupId",
            )
        )
    if state["owned"]["security_group"] and state.get("security_group_id"):
        ids.add(state["security_group_id"])
    return ids


def terminate_instances(aws: Aws, ids: set[str]) -> None:
    for instance_id in sorted(ids):
        aws.run("ec2", "terminate-instances", "--instance-ids", instance_id, check=False)
        aws.run("ec2", "wait", "instance-terminated", "--instance-ids", instance_id, check=False)


def delete_file_systems(aws: Aws, ids: set[str], state: dict[str, Any]) -> None:
    for fs_id in sorted(ids):
        def mount_targets() -> set[str]:
            return words(
                aws.run(
                    "efs",
                    "describe-mount-targets",
                    "--file-system-id",
                    fs_id,
                    "--query",
                    "MountTargets[].MountTargetId",
                    "--output",
                    "text",
                    check=False,
                )
            )

        targets = (
            retry(15, 2, mount_targets)
            if state["attempted"]["mount_target"]
            else mount_targets()
        )
        if (
            state["owned"]["mount_target"]
            and fs_id == state.get("file_system_id")
            and state.get("mount_target_id")
        ):
            targets.add(state["mount_target_id"])
        for target in sorted(targets):
            aws.run("efs", "delete-mount-target", "--mount-target-id", target, check=False)
            retry(
                120,
                2,
                lambda target=target: not aws.run(
                    "efs", "describe-mount-targets", "--mount-target-id", target, check=False
                ),
            )
        retry(
            10,
            3,
            lambda fs_id=fs_id: bool(
                aws.run("efs", "delete-file-system", "--file-system-id", fs_id, check=False)
            )
            or not aws.run(
                "efs", "describe-file-systems", "--file-system-id", fs_id, check=False
            ),
        )
        retry(
            120,
            2,
            lambda fs_id=fs_id: not aws.run(
                "efs", "describe-file-systems", "--file-system-id", fs_id, check=False
            ),
        )


def delete_security_groups(aws: Aws, ids: set[str]) -> None:
    for group_id in sorted(ids):
        retry(
            60,
            3,
            lambda group_id=group_id: bool(
                aws.run("ec2", "delete-security-group", "--group-id", group_id, check=False)
            ) or not aws.run(
                "ec2", "describe-security-groups", "--group-ids", group_id, check=False
            ),
        )


def delete_volumes(aws: Aws, state: dict[str, Any]) -> None:
    for _ in range(60):
        ids = words(
            aws.text(
                "ec2",
                "describe-volumes",
                "--filters",
                *filters(state),
                "Name=status,Values=available",
                "--query",
                "Volumes[].VolumeId",
            )
        )
        for volume_id in ids:
            aws.run("ec2", "delete-volume", "--volume-id", volume_id, check=False)
        if volume_count(aws, state) == 0:
            return
        time.sleep(3)


def named_resource_safe(aws: Aws, state: dict[str, Any], kind: str) -> bool:
    if state["owned"][kind]:
        return True
    if not state["attempted"][kind]:
        return False
    if kind == "role" and state.get("role_name"):
        return has_tag(aws, "iam", "list-role-tags", ["--role-name", state["role_name"]], state)
    if kind == "instance_profile" and state.get("instance_profile_name"):
        return has_tag(
            aws,
            "iam",
            "list-instance-profile-tags",
            ["--instance-profile-name", state["instance_profile_name"]],
            state,
        )
    if kind == "transfer_bucket" and state.get("transfer_bucket"):
        tagged = aws.run(
            "s3api",
            "get-bucket-tagging",
            "--bucket",
            state["transfer_bucket"],
            "--query",
            f'length(TagSet[?Key==`RunToken` && Value==`{state["run_token"]}`])',
            "--output",
            "text",
            check=False,
        )
        return tagged == "1"
    return False


def delete_named_resources(aws: Aws, state: dict[str, Any]) -> None:
    role_safe = named_resource_safe(aws, state, "role")
    profile_safe = named_resource_safe(aws, state, "instance_profile")
    bucket_safe = named_resource_safe(aws, state, "transfer_bucket")
    role = state.get("role_name", "")
    profile = state.get("instance_profile_name", "")
    bucket = state.get("transfer_bucket", "")
    if profile_safe and role_safe:
        aws.run(
            "iam",
            "remove-role-from-instance-profile",
            "--instance-profile-name",
            profile,
            "--role-name",
            role,
            check=False,
        )
    if profile_safe:
        aws.run(
            "iam",
            "delete-instance-profile",
            "--instance-profile-name",
            profile,
            check=False,
        )
    if role_safe:
        aws.run(
            "iam",
            "detach-role-policy",
            "--role-name",
            role,
            "--policy-arn",
            "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
            check=False,
        )
        aws.run("iam", "delete-role", "--role-name", role, check=False)
    if bucket_safe:
        aws.run("s3", "rm", f"s3://{bucket}", "--recursive", "--only-show-errors", check=False)
        aws.run("s3api", "delete-bucket", "--bucket", bucket, check=False)


def live_instance_count(aws: Aws, state: dict[str, Any]) -> int:
    return aws.count(
        "ec2",
        "describe-instances",
        "--filters",
        *filters(state),
        f"Name=instance-state-name,Values={LIVE_INSTANCE_STATES}",
        query="length(Reservations[].Instances[])",
    )


def volume_count(aws: Aws, state: dict[str, Any]) -> int:
    return aws.count(
        "ec2", "describe-volumes", "--filters", *filters(state), query="length(Volumes)"
    )


def verify(aws: Aws, state: dict[str, Any]) -> dict[str, int]:
    attempted = state["attempted"]
    checks = {
        "file_systems": aws.count(
            "efs",
            "describe-file-systems",
            "--creation-token",
            state["run_id"],
            query="length(FileSystems)",
        ),
        "tagged_live_instances": live_instance_count(aws, state),
        "client_token_live_instances": 0,
        "security_groups": aws.count(
            "ec2",
            "describe-security-groups",
            "--filters",
            *filters(state),
            query="length(SecurityGroups)",
        ),
        "recorded_security_groups": 0,
        "attempted_security_groups": 0,
        "volumes": volume_count(aws, state),
        "roles": 0,
        "instance_profiles": 0,
        "buckets": 0,
    }
    if attempted["instance"]:
        checks["client_token_live_instances"] = aws.count(
            "ec2",
            "describe-instances",
            "--filters",
            f'Name=client-token,Values={state["run_token"]}',
            f"Name=instance-state-name,Values={LIVE_INSTANCE_STATES}",
            query="length(Reservations[].Instances[])",
        )
    if attempted["security_group"]:
        checks["attempted_security_groups"] = aws.count(
            "ec2",
            "describe-security-groups",
            "--filters",
            f'Name=vpc-id,Values={state["vpc_id"]}',
            f'Name=group-name,Values=nfs-crust-bench-{state["run_id"]}',
            query="length(SecurityGroups)",
        )
    if state["owned"]["security_group"] and state.get("security_group_id"):
        checks["recorded_security_groups"] = int(
            bool(
                aws.run(
                    "ec2",
                    "describe-security-groups",
                    "--group-ids",
                    state["security_group_id"],
                    check=False,
                )
            )
        )
    if attempted["role"]:
        checks["roles"] = aws.count(
            "iam",
            "list-roles",
            query=f'length(Roles[?RoleName==`{state.get("role_name", "")}`])',
        )
    if attempted["instance_profile"]:
        checks["instance_profiles"] = aws.count(
            "iam",
            "list-instance-profiles",
            query=(
                "length(InstanceProfiles[?InstanceProfileName==`"
                f'{state.get("instance_profile_name", "")}`])'
            ),
        )
    if attempted["transfer_bucket"]:
        checks["buckets"] = aws.count(
            "s3api",
            "list-buckets",
            query=f'length(Buckets[?Name==`{state.get("transfer_bucket", "")}`])',
        )
    return checks


def write_result(
    path: Path,
    state: dict[str, Any],
    current_account: str,
    checks: dict[str, int],
) -> None:
    result = {
        "run_id": state["run_id"],
        "creator_account_id": state["account_id"],
        "cleanup_account_id": current_account,
        "all_throwaway_resources_removed": all(value == 0 for value in checks.values()),
        "completed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "checks": checks,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("state_file", type=Path)
    parser.add_argument("--result", type=Path)
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()

    state = load_state(args.state_file)
    configured_region = os.environ.get("AWS_REGION")
    if configured_region and configured_region != state["region"]:
        raise SystemExit(
            f'refusing cleanup in region {configured_region}; run was created in {state["region"]}'
        )
    os.environ["AWS_REGION"] = state["region"]
    if args.validate_only:
        return 0
    if not os.environ.get("AWS_PROFILE"):
        raise SystemExit("AWS_PROFILE must name the sandbox profile that created the resources")

    aws = Aws()
    current_account = aws.text("sts", "get-caller-identity", "--query", "Account")
    if current_account != state["account_id"]:
        raise SystemExit(
            f"refusing cleanup in account {current_account}; "
            f'run was created in {state["account_id"]}'
        )

    print(
        f'cleaning benchmark {state["run_id"]} in account {current_account}, '
        f'region {state["region"]}'
    )
    terminate_instances(aws, instance_ids(aws, state))
    delete_file_systems(aws, file_system_ids(aws, state), state)
    delete_security_groups(aws, security_group_ids(aws, state))
    delete_volumes(aws, state)
    delete_named_resources(aws, state)
    checks = verify(aws, state)
    if args.result:
        write_result(args.result, state, current_account, checks)
    if any(checks.values()):
        print(f"cleanup verification failed: {checks}", file=sys.stderr)
        return 1
    print("all owned benchmark resources were removed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
