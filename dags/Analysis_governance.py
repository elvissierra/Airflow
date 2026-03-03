
"""Repository governance (Airflow)

This file is intentionally NOT a DAG. It is a lightweight “project notes” anchor that makes
intent explicit when the repo is still evolving.

GOVERNANCE RULES (pragmatic + enforceable)
1) dags/ contains ONLY DAG definitions (thin orchestration).
2) No tutorial/scratch code in paths scanned for DAGs.
3) One DAG = one business object (or one cohesive pipeline). Keep responsibilities crisp.
4) Prefer: stage tables are transient and typed late; targets use stable snake_case schema.
5) Every pipeline should emit at least: rowcount staged, rowcount merged, and a basic quality check.

NEXT STEPS FOR THIS REPO
- Decide the next “object” to ingest (replace employees).
- Rename `OBJECT_CONFIG['target_table']`, `staging_table`, and `column_map` in the template DAG.
- Add data quality checks (null PK, duplicate PK in stage, schema drift).
- If this becomes more than 1–2 DAGs, extract reusable functions into a dedicated module
  (in a real repo: src/ or include/).
"""

# NOTE: kept as a .py file only because you currently have minimal project scaffolding.
# If you add a README.md later, you can delete this file.