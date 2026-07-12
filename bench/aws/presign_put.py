#!/usr/bin/env python3
"""Create an AWS SigV4 presigned S3 PUT URL from process credentials on stdin."""

from __future__ import annotations

import argparse
import datetime
import hashlib
import hmac
import json
import sys
import urllib.parse


def encode(value: str, *, safe: str = "-_.~") -> str:
    return urllib.parse.quote(value, safe=safe)


def hmac_sha256(key: bytes, value: str) -> bytes:
    return hmac.new(key, value.encode(), hashlib.sha256).digest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--expires", type=int, default=21_600)
    args = parser.parse_args()
    if not 1 <= args.expires <= 604_800:
        parser.error("--expires must be between 1 and 604800 seconds")

    credentials = json.load(sys.stdin)
    access_key = credentials["AccessKeyId"]
    secret_key = credentials["SecretAccessKey"]
    session_token = credentials.get("SessionToken")
    now = datetime.datetime.now(datetime.timezone.utc)
    date = now.strftime("%Y%m%d")
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    scope = f"{date}/{args.region}/s3/aws4_request"
    host = f"{args.bucket}.s3.{args.region}.amazonaws.com"
    canonical_uri = "/" + encode(args.key, safe="/-_.~")
    parameters = {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{access_key}/{scope}",
        "X-Amz-Date": timestamp,
        "X-Amz-Expires": str(args.expires),
        "X-Amz-SignedHeaders": "host",
    }
    if session_token:
        parameters["X-Amz-Security-Token"] = session_token
    canonical_query = "&".join(
        f"{encode(key)}={encode(value)}" for key, value in sorted(parameters.items())
    )
    canonical_request = "\n".join(
        [
            "PUT",
            canonical_uri,
            canonical_query,
            f"host:{host}\n",
            "host",
            "UNSIGNED-PAYLOAD",
        ]
    )
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            timestamp,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )
    date_key = hmac_sha256(("AWS4" + secret_key).encode(), date)
    region_key = hmac_sha256(date_key, args.region)
    service_key = hmac_sha256(region_key, "s3")
    signing_key = hmac_sha256(service_key, "aws4_request")
    signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    print(f"https://{host}{canonical_uri}?{canonical_query}&X-Amz-Signature={signature}")


if __name__ == "__main__":
    main()
