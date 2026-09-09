# fold

Go library for ordered webhook delivery. See `AGENTS.md` and `DECISIONS.md`.

## Tooling

### Git hooks (lefthook)

Hooks are configured in `lefthook.yml` (version-controlled). After clone:

```bash
brew install lefthook   # or: go install github.com/evilmartians/lefthook@latest
lefthook install
```

Pre-commit runs `./scripts/check-invariants.sh` (~250ms) whenever anything is staged. Manual run with an empty index: `lefthook run pre-commit --force`. Bypass only when you mean it: `LEFTHOOK=0 git commit ...`.

### Lint

```bash
golangci-lint run ./...
```

Config: `.golangci.yml` (errcheck, bodyclose, govet, staticcheck). Also runs in CI.
