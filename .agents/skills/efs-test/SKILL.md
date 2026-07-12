---
name: efs-test
description: Run the ignored external NFS interop suite against a throwaway AWS EFS file system, reached from the local machine through an SSM port-forward tunnel. Creates all AWS resources, runs the tests, and tears everything down. Use when a change touches compound shapes, the RPC transport, or anything else where embednfs coverage is not enough.
---

# Exercise nfs-crust against real AWS EFS

Total cost is pennies (t4g.nano + EFS for ~15 minutes). Every resource is
throwaway and torn down at the end — never skip teardown.

## Prerequisites

- AWS credentials for a sandbox-grade account: set `AWS_PROFILE` and
  `AWS_REGION` from the environment or ask the user; never assume a
  production account. Verify with `aws sts get-caller-identity` first.
- `session-manager-plugin` on PATH. Without sudo, it can be installed by
  extracting `session-manager-plugin` from the AWS `sessionmanager-bundle.zip`
  for the local platform into `~/.local/bin`.

## Discover networking

Pick a VPC and a public subnet (`MapPublicIpOnLaunch` true, so SSM can reach
the instance) rather than hardcoding ids:

- `aws ec2 describe-vpcs` (prefer the default VPC; otherwise ask the user
  which VPC is safe to use)
- `aws ec2 describe-subnets --filters Name=vpc-id,Values=<vpc>` and choose a
  public subnet
- Resolve the AMI fresh:
  `aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64 --query Parameter.Value --output text`

## Setup

1. Create a security group in the VPC allowing TCP 2049 from itself; attach
   it to both the mount target and the instance. Tag everything created here
   `Name=nfs-crust-throwaway`.
2. `aws efs create-file-system --creation-token nfs-crust-throwaway-$(date +%s)
   --tags Key=Name,Value=nfs-crust-throwaway`; wait for `available`; create a
   mount target in the public subnet with the security group.
3. Create IAM role + instance profile `nfs-crust-throwaway-ssm` with
   `AmazonSSMManagedInstanceCore` attached (trust `ec2.amazonaws.com`). Sleep
   ~10s after creation for instance-profile propagation.
4. Launch a `t4g.nano` with the AMI, subnet, security group, public IP, and
   the instance profile. Wait for SSM `PingStatus == Online` (~1-2 min).
5. **Open the EFS root**: a fresh EFS root is root-owned mode 755 and the
   client authenticates as the local uid via AUTH_SYS, so writes would fail.
   Via `aws ssm send-command` (AWS-RunShellScript) on the instance:
   mount `nfs4 -o nfsvers=4.1 <mount-target-ip>:/`, `chmod 777` it, unmount.
6. Tunnel, in the background:
   `aws ssm start-session --target <instance> --document-name
   AWS-StartPortForwardingSessionToRemoteHost --parameters
   '{"host":["<mount-target-ip>"],"portNumber":["2049"],"localPortNumber":["12049"]}'`
   then confirm with `nc -z 127.0.0.1 12049`.

## Run

```
NFS_CRUST_EXTERNAL_ENDPOINT=127.0.0.1:12049 \
NFS_CRUST_EXTERNAL_OPERATION_TIMEOUT_SECONDS=120 \
NFS_CRUST_EXTERNAL_CONNECT_TIMEOUT_SECONDS=60 \
cargo test --test external_nfs -- --ignored --test-threads=1
```

Expect all tests to pass in roughly two minutes through the tunnel. The
fused-put test observes RPC compound tags through a local proxy, so it only
asserts on plaintext endpoints (it self-skips under TLS).

## Teardown (always, in this order)

1. Stop the tunnel session.
2. Terminate the instance.
3. Delete the mount target; wait until it is gone, then delete the file
   system (deletion fails while mount targets exist).
4. After the instance is terminated, delete the security group (it is
   referenced by both instance and mount target until then).
5. Remove role from instance profile, delete instance profile, detach the
   policy, delete the role.
6. Confirm: `aws efs describe-file-systems` shows no `nfs-crust-throwaway`.

## EFS behavior notes (verified July 2026)

- EFS accepts the NFSv4.1 special current stateid for `WRITE` in a compound
  but returns `NFS4ERR_BAD_STATEID` for `CLOSE` with it — that is why the
  fused put closes via a detached follow-up compound.
- EFS accepts `VERIFY`, `SAVEFH`/`RESTOREFH`, `COMMIT` before `CLOSE`, and
  ~12-op compounds.
- EFS accepts the fused temporary-file shape (`OPEN` + `WRITE` via current
  stateid + `COMMIT` + `VERIFY` + atomic publish) and the
  `COMMIT`+`VERIFY`+`CLOSE` verified-close compound used for chunked writes.
- Per-operation latency through the tunnel is a few ms; the suite finishing
  in ~2 minutes is normal.
