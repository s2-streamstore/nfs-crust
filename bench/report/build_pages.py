#!/usr/bin/env python3
"""Build a static GitHub Pages site from sanitized benchmark reports."""

from __future__ import annotations

import argparse
import html
import importlib.util
import json
import re
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from urllib.parse import quote


REPOSITORY_URL = "https://github.com/s2-streamstore/nfs-crust"
RUN_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def load_publisher(root: Path) -> ModuleType:
    module_path = root / "bench" / "aws" / "publish_results.py"
    spec = importlib.util.spec_from_file_location("benchmark_publisher", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load benchmark publisher from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def document(title: str, body: str, parent_href: str | None = None) -> str:
    parent_link = ""
    if parent_href is not None:
        parent_link = f'<a href="{html.escape(parent_href)}">All benchmark runs</a>'
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="nfs-crust AWS EFS benchmark results">
<title>{html.escape(title)}</title>
<style>
:root {{ color-scheme: light; font-family: Inter, ui-sans-serif, system-ui, sans-serif; }}
body {{ margin: 0; background: #f5f7fb; color: #172033; }}
.site-nav {{ display: flex; justify-content: space-between; gap: 1rem; padding: .8rem clamp(1rem, 4vw, 3rem); background: #172033; color: #fff; }}
.site-nav div {{ display: flex; flex-wrap: wrap; gap: 1rem; }}
.site-nav a {{ color: inherit; text-decoration: none; }}
.site-nav a:hover {{ text-decoration: underline; }}
.site-index {{ width: min(72rem, calc(100% - 2rem)); margin: 3rem auto; }}
.site-index h1 {{ margin-bottom: .25rem; }}
.site-index > p {{ color: #5b6475; }}
.run-list {{ display: grid; gap: 1rem; padding: 0; list-style: none; }}
.run-card {{ display: block; padding: 1rem 1.2rem; border: 1px solid #d8dee9; border-radius: .75rem; background: #fff; color: inherit; text-decoration: none; box-shadow: 0 1px 2px rgb(15 23 42 / .06); }}
.run-card:hover {{ border-color: #2563eb; }}
.run-card strong {{ display: block; overflow-wrap: anywhere; }}
.run-meta {{ display: flex; flex-wrap: wrap; gap: .35rem 1rem; margin-top: .4rem; color: #5b6475; font-size: .9rem; }}
</style>
</head>
<body>
<nav class="site-nav" aria-label="Site navigation"><a href="{REPOSITORY_URL}">nfs-crust</a><div>{parent_link}<a href="{REPOSITORY_URL}/tree/main/bench/results">Source data</a></div></nav>
{body}
</body>
</html>
"""


def describe_run(run_dir: Path, publisher: ModuleType) -> dict[str, str]:
    if RUN_NAME.fullmatch(run_dir.name) is None:
        raise RuntimeError(f"unsafe benchmark run directory name: {run_dir.name}")
    publisher.assert_publication_safe(run_dir)
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    metadata = summary.get("metadata", {})
    if metadata.get("run_id") != run_dir.name:
        raise RuntimeError(f"run ID does not match directory name: {run_dir}")
    started_ms = metadata.get("started_at_unix_ms")
    if not isinstance(started_ms, int):
        raise RuntimeError(f"run has no valid start timestamp: {run_dir}")
    revision = metadata.get("source_revision")
    if not isinstance(revision, str) or not revision:
        raise RuntimeError(f"run has no source revision: {run_dir}")
    repetitions = metadata.get("repetitions")
    if not isinstance(repetitions, int):
        raise RuntimeError(f"run has no repetition count: {run_dir}")
    return {
        "name": run_dir.name,
        "date": datetime.fromtimestamp(started_ms / 1000, UTC).strftime(
            "%Y-%m-%d %H:%M UTC"
        ),
        "revision": revision[:12],
        "repetitions": str(repetitions),
        "fragment": (run_dir / "report.fragment.html").read_text(encoding="utf-8"),
    }


def index_body(runs: list[dict[str, str]]) -> str:
    cards = []
    for run in runs:
        name = html.escape(run["name"])
        href = f'./{quote(run["name"])}/'
        cards.append(
            f'<li><a class="run-card" href="{href}"><strong>{name}</strong>'
            f'<span class="run-meta"><span>{html.escape(run["date"])}</span>'
            f'<span>{html.escape(run["repetitions"])} repetitions</span>'
            f'<span>revision <code>{html.escape(run["revision"])}</code></span>'
            "</span></a></li>"
        )
    return (
        '<main class="site-index"><p>nfs-crust</p><h1>AWS EFS benchmark reports</h1>'
        "<p>Sanitized, reproducible comparisons with the Linux NFSv4.1 client.</p>"
        f'<ul class="run-list">{"".join(cards)}</ul></main>'
    )


def build_site(root: Path, results: Path, output: Path) -> None:
    publisher = load_publisher(root)
    run_dirs = sorted(
        (path for path in results.iterdir() if path.is_dir()),
        key=lambda path: path.name,
        reverse=True,
    )
    if not run_dirs:
        raise RuntimeError(f"no benchmark runs found under {results}")
    runs = [describe_run(run_dir, publisher) for run_dir in run_dirs]

    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    try:
        (stage / "index.html").write_text(
            document("nfs-crust benchmark reports", index_body(runs)),
            encoding="utf-8",
        )
        (stage / ".nojekyll").touch()
        for run in runs:
            run_output = stage / run["name"]
            run_output.mkdir()
            run_output.joinpath("index.html").write_text(
                document(
                    f'{run["name"]} | nfs-crust benchmarks',
                    run["fragment"],
                    "../",
                ),
                encoding="utf-8",
            )
        if output.exists():
            shutil.rmtree(output)
        stage.rename(output)
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", type=Path, default=Path("bench/results"))
    parser.add_argument("--output", type=Path, default=Path("_site"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    build_site(root, args.results.resolve(), args.output.resolve())
    print(args.output.resolve())


if __name__ == "__main__":
    main()
