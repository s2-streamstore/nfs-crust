# Benchmark results

AWS benchmark runs are written to one immutable directory per run ID here.
Each directory contains only the sanitized structured summary and rendered
report. Raw samples and operational evidence stay under `.bench/state`.

Do not reuse or modify a completed run directory. Generate a new run ID for
every attempt, and commit only runs whose report marks the measurement valid.
Add runs through `bench/aws/publish_results.py`; it sanitizes infrastructure
identifiers and rejects unsafe output before making the result visible here.
Rendered reports are published at
<https://s2-streamstore.github.io/nfs-crust/>.
