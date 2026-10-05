# AI usage disclosure

Codex was used to:

- inspect the assignment README and the requirements in `docs/`;
- execute NB1–NB8 in the local workspace and verify their real outputs;
- add evidence-oriented output cells without changing the grading thresholds;
- explain schema enforcement/evolution, time travel/RESTORE, medallion checks,
  Iceberg partition evolution, maintenance, vector retrieval and provenance;
- draft the bonus architecture document from the supplied rubric and clearly
  label its capacity figures as planning assumptions;
- implement and execute the bonus fail-closed tokenization/idempotent-replay
  PoC, including its assertions and limitation notes;
- render the executed notebooks and capture local evidence screenshots with Playwright.

All reported measurements come from notebook executions in this repository.
No assertion was removed and no threshold was lowered. The student remains
responsible for reviewing, understanding and explaining the submitted work.

This disclosure is linked from `submission/REFLECTION.md`.
