# Contributing

Thanks for considering a contribution. This project is an alpha operator for
an alpha runtime; the bar for changes is that they keep the operational
invariants documented in [DESIGN.md](DESIGN.md) intact.

## Development

Standard kubebuilder project. You need Go, and `make` fetches the rest
(controller-gen, envtest binaries, golangci-lint) into `bin/`.

```sh
make test     # envtest suite + unit tests
make lint     # golangci-lint (CI enforces this)
make run      # run the operator against your current kubeconfig
```

The e2e suite (`make test-e2e`) expects a kind cluster and runs in CI on
every push.

## Pull requests

- Keep changes focused; one behavior per PR.
- `make test lint` must pass; CI runs both plus e2e and a Helm chart check.
- New behavior needs a test — envtest specs live in
  `internal/controller/*_test.go`.
- Anything that touches the rollout state machine, the fencing-adjacent
  paths (partition stepping, `/state` gating), or the guardrails in
  `fleet_resources.go` should cite the celld behavior it depends on, the
  way the existing code comments do (DESIGN.md's F1–F11 table is the
  index). If upstream celld changed, update the table.
- The `breakingBoundaries` table in `internal/controller/rollout.go` must
  track celld release notes: if upstream flags a version pair as not
  rolling-safe, add it in the same PR that bumps supported versions.

## Sign-off

By contributing you certify the
[Developer Certificate of Origin](https://developercertificate.org/); add a
`Signed-off-by` trailer (`git commit -s`) to your commits.

## Releases

Pushing a `v*` tag publishes the operator image and the Helm chart with
matching versions; main-branch pushes publish `sha-*` images and
`0.1.0-main.N` prerelease charts continuously.
