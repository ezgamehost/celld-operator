# Contributing

Thanks for considering a contribution. This project is an alpha operator for
an alpha runtime; the bar for changes is that they keep the operational
invariants documented in [docs/celld-behaviors.md](docs/celld-behaviors.md)
intact.

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
  way the existing code comments do (the F1–F14 table in
  [docs/celld-behaviors.md](docs/celld-behaviors.md) is the index). If
  upstream celld changed, update the table.
- The `breakingBoundaries` table in `internal/controller/rollout.go` must
  track celld release notes: if upstream flags a version pair as not
  rolling-safe, add it in the same PR that bumps supported versions.

## Sign-off

By contributing you certify the
[Developer Certificate of Origin](https://developercertificate.org/); add a
`Signed-off-by` trailer (`git commit -s`) to your commits.

## Commit messages and releases

Commits on main follow [Conventional Commits](https://www.conventionalcommits.org):
`feat:` cuts a minor release, `fix:` a patch, and `chore:`/`docs:`/`ci:` cut
nothing. While the project is pre-1.0, a breaking change (`!` or a
`BREAKING CHANGE:` footer) also cuts a *minor* release — still mark it, so it
lands in the release notes; graduating to 1.0 means dropping that rule from
`.releaserc.json`. semantic-release runs on
every main push after CI: it derives the version from the commits since the
last tag, creates the tag and the GitHub Release with generated notes, and
the same workflow then publishes the matching `v<version>` operator image
(a retag of that commit's `sha-*` image, so the released image is
bit-identical to the tested one) and the `<version>` Helm chart.

Main-branch pushes also publish `sha-*` images and `0.1.0-main.N`
prerelease charts continuously, release or not.
