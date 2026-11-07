# ADR-0006: Versioned, immutable workflow definitions validated as DAGs

## Context
Definitions evolve while executions are running. Changing a running
execution's steps silently would break its semantics.

## Decision
* A workflow has numbered versions; at most one `DRAFT` (partial unique
  index). `PUT` edits only the draft; `POST /versions` opens a new draft;
  `POST /publish` freezes it.
* Published versions are immutable in code and in the database (trigger).
* An execution stores `version_id`; tasks copy their handler, retry policy,
  timeout and compensation spec at creation.
* Specs are validated as DAGs (unknown/duplicate/self dependencies, cycles
  via Kahn's algorithm with the steps involved, limits). Publishing also
  checks that every handler is registered by some worker.

## Consequences
* New versions never alter running executions (`TestWorkflowDefinitionLifecycle`).
* Clients can start a specific version or the latest published one.
* There is no "migrate running executions to a new version" feature.
