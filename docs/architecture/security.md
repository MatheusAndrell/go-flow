# Security

## Authentication

* API keys (`gf_` + 32 random bytes, base64url) are created with
  `goflow apikey create -tenant NAME -permissions ...`; the secret is printed
  once. Only its SHA-256 hash is stored — keys are high-entropy, so a slow
  password hash adds nothing — and hashes are compared in constant time.
* `Authorization: Bearer <key>` is required on every `/api/v1` route.
  Successful lookups are cached for `GOFLOW_AUTH_CACHE_TTL` (30s by default):
  a revoked key (`goflow apikey revoke -id`) stops working within that time.

## Authorization and tenant isolation

* Permissions: `workflows:read`, `workflows:write`, `executions:read`,
  `executions:write`. Every use case starts with
  `principal.Require(permission)` (403 otherwise).
* Each key belongs to one tenant. The tenant id comes from the key, never from
  the request, and every repository query filters by `tenant_id`. Resources
  of another tenant are reported as **404** (not 403) so their existence is
  not disclosed. `TestTenantIsolation` checks every read and write route.
* Idempotency keys are scoped per tenant.

## Untrusted payloads and code execution

* GoFlow **never executes user-supplied code**. A step only names a handler;
  handlers are Go code compiled into a worker binary and registered in a
  `handler.Registry`. Workflows referencing a handler that no worker has
  registered cannot be published.
* Request bodies are limited (`GOFLOW_MAX_REQUEST_BYTES`, default 1 MiB, 413
  beyond), decoded strictly (unknown fields and trailing data rejected).
  Execution input must be a JSON object ≤ 256 KiB; step params ≤ 64 KiB;
  at most 100 steps; timeouts ≤ 24h; ≤ 100 attempts; schedules ≤ 1 year
  ahead. Handler output must be valid JSON.
* Error messages stored from handlers are truncated (4000 chars); internal
  errors return a generic 500 without details.

## Abuse protection

* Token-bucket rate limiting per tenant (per client IP before
  authentication, which also throttles key guessing): `GOFLOW_RATE_LIMIT_RPS`
  / `GOFLOW_RATE_LIMIT_BURST`, 429 with `Retry-After`.
* Duplicate execution protection: `Idempotency-Key` header (replayed
  response, 422 on reuse with a different body, 409 while in progress) and a
  unique constraint on `(tenant_id, idempotency_key)`.
* HTTP server timeouts (`ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout`,
  `IdleTimeout`) against slow clients; `X-Content-Type-Options: nosniff`,
  `Cache-Control: no-store`, `X-Frame-Options: DENY`.

## Secrets and sensitive data

* Configuration comes from environment variables
  ([`configs/goflow.env.example`](../../configs/goflow.env.example)); `.env`
  is git-ignored. The database URL is logged only with the password redacted.
* Logs never include request bodies, workflow input/output or API keys; they
  contain ids, statuses and error messages.
* Containers run as non-root on a distroless base image.

## Known gaps

See the README's "Known limitations": no API for key management (CLI only),
no field-level encryption of payloads, and workers are trusted components
with direct database access.
