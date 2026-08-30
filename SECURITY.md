# Security Policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately via
[GitHub private vulnerability reporting](https://github.com/ezgamehost/celld-operator/security/advisories/new).
Do not open a public issue for security reports. We aim to acknowledge
reports within a week.

## Supported versions

Only the latest release receives security fixes. This mirrors the policy of
[celld](https://github.com/denoland/celld) itself, whose alpha releases are
fixed at head only — pinning an old operator against a new celld (or the
reverse) is unsupported.

## Scope notes for operators of this operator

Two properties of the underlying system are worth restating when assessing
exposure:

- **Bucket credentials are fleet-admin authority.** Anyone holding a
  fleet's object-store credentials controls that fleet: deployments, cell
  state, ownership records. Scope one credential per fleet to that fleet's
  prefix, and rotate on suspicion of disclosure. The operator reads these
  credentials (for config-hash rotation tracking and deploy-pointer
  tracking) but never logs or re-exports them.
- **celld's internal listener (:8081) is unauthenticated upstream** (only
  the D1 SQL route checks the fleet secret; `/cell/` and `/do/` run
  application code for any caller). The operator fences it with
  NetworkPolicy and (where Istio is present) AuthorizationPolicy. Cell
  activation, eviction and shutdown rights are available only to callers
  that pass every effective policy layer, or to any caller that can reach
  the pod network when both policy controls are removed.
- celld itself is not safe for hostile multi-tenant use; the isolation
  model is fleet-per-application. See
  [docs/celld-behaviors.md](docs/celld-behaviors.md) (F3) before hosting
  untrusted code.
