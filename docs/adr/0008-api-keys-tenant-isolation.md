# ADR-0008: API-key authentication with tenant-scoped data access

## Context
GoFlow is called by other services (machine-to-machine) and must isolate
tenants.

## Decision
* Bearer API keys, 256-bit random, stored as SHA-256 hashes, created/revoked
  via CLI, each bound to one tenant with explicit permissions.
* The principal is derived from the key; use cases check permissions; every
  query filters by `tenant_id`; cross-tenant access returns 404.
* Short TTL cache for key lookups (default 30 s).
* Per-tenant token-bucket rate limiting in process.

## Consequences
* Simple and adequate for service-to-service use; no external identity
  provider needed to run the project.
* OAuth2/OIDC (JWT validation) can replace the `Authenticator` port without
  touching use cases.
* Rate limits are per API instance (N replicas ⇒ N× limit). A shared limiter
  (Redis or PostgreSQL) is the documented upgrade path.
