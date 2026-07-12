import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("build_pages.py")
SPEC = importlib.util.spec_from_file_location("build_pages", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
PAGES = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PAGES)


def write_run(root: Path, name: str, started_ms: int, report: str) -> None:
    run = root / name
    run.mkdir()
    (run / "summary.json").write_text(
        json.dumps(
            {
                "metadata": {
                    "run_id": name,
                    "started_at_unix_ms": started_ms,
                    "source_revision": "abcdef0123456789",
                    "repetitions": 2,
                }
            }
        ),
        encoding="utf-8",
    )
    (run / "report.fragment.html").write_text(report, encoding="utf-8")


class BuildPagesTests(unittest.TestCase):
    def test_builds_index_and_html_only_run_pages(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results = root / "results"
            output = root / "site"
            results.mkdir()
            write_run(results, "run-a", 1_700_000_000_000, "<article>A</article>")
            write_run(results, "run-b", 1_800_000_000_000, "<article>B</article>")

            PAGES.build_site(Path(__file__).resolve().parents[2], results, output)

            index = (output / "index.html").read_text(encoding="utf-8")
            self.assertLess(index.index("run-b"), index.index("run-a"))
            self.assertIn('./run-a/', index)
            self.assertIn(
                "<article>A</article>",
                (output / "run-a/index.html").read_text(),
            )
            self.assertEqual(
                sorted(str(path.relative_to(output)) for path in output.rglob("*")),
                [
                    ".nojekyll",
                    "index.html",
                    "run-a",
                    "run-a/index.html",
                    "run-b",
                    "run-b/index.html",
                ],
            )

    def test_rejects_report_with_infrastructure_identifier(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results = root / "results"
            results.mkdir()
            write_run(results, "run-a", 1_700_000_000_000, "<p>10.0.0.1</p>")

            with self.assertRaisesRegex(RuntimeError, "infrastructure identifiers"):
                PAGES.build_site(
                    Path(__file__).resolve().parents[2], results, root / "site"
                )


if __name__ == "__main__":
    unittest.main()
