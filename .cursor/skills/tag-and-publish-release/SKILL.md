---
name: tag-and-publish-release
description: >-
  Creates an annotated semver git tag from Cargo.toml on main, publishes a
  GitHub release with gh, then runs cargo publish to crates.io. Use only when
  the user runs /tag-and-publish-release after make-release is merged: current
  branch must be main, main must match origin/main on github.com, and the
  working tree must be clean.
---

# Tag and publish release

Run **after** the merge to `main` of the release PR that make-release opened (e.g. `release/x.y.z` → `main`). Do not use this instead of make-release.

**Prerequisites:** `git`, `gh` (GitHub CLI, authenticated to GitHub.com), `cargo` (Rust toolchain), crates.io credentials (`cargo login` or `CARGO_REGISTRY_TOKEN`), `origin` pointing at a github.com repository.

## 0. When to apply

- Apply **only** when the user explicitly invokes `/tag-and-publish-release` (or clearly asks for this same workflow by name).
- If any gate in §1 fails, **stop** with a short explanation; do not tag or publish.

## 1. Gates (must all pass)

1. **Branch:** `git branch --show-current` must be exactly `main`.
2. **Sync with origin:** Run `git fetch origin main` and `git fetch origin --tags`. Then require `HEAD` and `origin/main` to be the **same commit**:

   ```bash
   test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
   ```

   If not equal, stop (user must pull/rebase or push so local `main` matches GitHub).
3. **Remote host:** `git remote get-url origin` should reference `github.com` (ssh or https). If not, stop.
4. **Clean tree:** `git status --porcelain` must be empty.

## 2. Resolve version **V**

- From `Cargo.toml`, read `[package]` `version = "x.y.z"`. Call this **V** (no leading `v`).
- Tag name **T** = `v` + **V** (e.g. `0.3.4` → `v0.3.4`), matching the tag pattern used in make-release.

## 3. Idempotency / conflicts

- If **T** already exists on `origin` (`git ls-remote --tags origin "refs/tags/${T}"`), stop and report; do not create a duplicate release.

## 4. Create and push the tag

- Create an **annotated** tag (release metadata):

  ```bash
  git tag -a "${T}" -m "Release ${T}"
  git push origin "${T}"
  ```

## 5. Publish the GitHub release

- Create the release from the new tag (generated release notes are acceptable unless the user asked for custom notes):

  ```bash
  gh release create "${T}" --title "Release ${T}" --generate-notes
  ```

- If the project expects notes from `CHANGELOG.md` instead, use `--notes-file` with the relevant section or `--notes` with a one-line pointer to the changelog; default to `--generate-notes` when unspecified.

## 6. Publish to crates.io

- From the repository root (where the root `Cargo.toml` for the crate lives), run:

  ```bash
  cargo publish
  ```

- If publish fails because **V** is already on crates.io, stop and report (do not bump versions here; use make-release for a new version).
- If authentication fails, stop; the user must run `cargo login` or set `CARGO_REGISTRY_TOKEN` per [crates.io publishing](https://doc.rust-lang.org/cargo/reference/publishing.html).

## 7. Report

- Echo the tag name, the pushed ref, the GitHub release URL, and confirm `cargo publish` succeeded (crate name and version **V**).

## Quick reference

| Check | Action if failed |
|-------|------------------|
| Not on `main` | Stop |
| `HEAD` ≠ `origin/main` after fetch | Stop |
| `origin` not github.com | Stop |
| Dirty working tree | Stop |
| Tag **T** already on origin | Stop |
| `gh` not authenticated | Stop; user runs `gh auth login` |
| `cargo publish` fails (duplicate version, etc.) | Stop; report error; version bumps belong in make-release |
| crates.io auth missing | Stop; user runs `cargo login` or sets token |
