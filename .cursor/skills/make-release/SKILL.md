---
name: make-release
description: >-
  Prepares a semver release branch from main: compares Cargo.toml to the latest
  git tag, bumps the patch version when needed, updates CLI version tests,
  writes CHANGELOG.md, pushes release/{version}, then runs make-pr. Use when
  cutting a release, bumping versions, or when the user says make-release.
---

# Make Release

End-to-end release prep for this repo. Execute steps in order using the shell; stop with a clear error if any check fails.

**Prerequisites:** `git`, `gh` (GitHub CLI, authenticated), clean `main` (stash or commit unrelated work first).

## 1. Verify branch

- Run `git branch --show-current`. Must be exactly `main`. If not, stop.
- Recommended: `git fetch origin main` and `git status` — if behind `origin/main`, stop and ask to pull/rebase first.

## 2. Read crate version

- From `Cargo.toml`, read the `[package]` `version = "x.y.z"` value. Call this **V₀**.

## 3. Fetch tags and resolve latest release tag

- Run `git fetch origin --tags` (and ensure `origin` exists).
- Resolve the highest SemVer release tag:

```bash
git tag --list --sort=-v:refname | grep -E '^v?[0-9]+\.[0-9]+\.[0-9]+$' | head -1
```

Use the matched ref string as **T** (e.g. `v0.3.2`) for `git log`/`git diff`. Strip a leading `v` from **T** for numeric comparison only; call that **L**. If there is no match, set **T** empty, **L** = `0.0.0`, and treat the changelog range as the full history on `main` (state that in the notes).

## 4. Decide target version **V**

Compare **V₀** and **L** as SemVer triples (string sort with `sort -V` is acceptable for simple `x.y.z`):

- If **V₀** is strictly greater than **L** → set **V** = **V₀** (no version bump in Cargo.toml).
- Otherwise → set **V** = **V₀** with **patch** incremented by 1. If still not greater than **L** (edge case), increment patch again until **V** > **L**.

When bumping:

1. Set `version = "V"` in `Cargo.toml` under `[package]`.
2. Search for hard-coded crate versions (e.g. `grep -r 'datu [0-9]' features/` or the CLI feature file). Update expectations such as `Then the first line of the output should be: datu X.Y.Z` in `features/cli/cli.feature` to match **V**.
3. Run `cargo test` (and `cargo clippy --all-targets -- -D warnings` if the project uses it) and fix failures.

## 5. Create release branch

- Run `git checkout -b "release/$(echo "$V" | tr -d '\n')"` (branch name `release/x.y.z`).
- If version/tests changed on main before branching: ensure those edits are included in the branch (either commit on branch in next steps, or branch was created after edits — working tree should contain all release changes).

## 6. Changelog (always)

Compare current `main` (the commit you branched from — use `main` as the ref if it still points there, or `origin/main`) against **T**:

- If **T** is set: `git log T..main --oneline`, `git diff T..main --stat`, and `git diff T..main --shortstat` for stats.
- If **T** is empty: summarize `git log main --oneline` and note first release or missing prior tag.

Do this **before** or **after** creating the branch, but use the same **T** so the notes describe everything merged to `main` since the last tag.

Write a new section at the **top** of `CHANGELOG.md` (after the title block), matching existing style in that file:

- Title: `## vX.Y.Z` (use **V**).
- Sections like **Highlights**, **Improvements**, **Changelog Stats** (commit count, files changed, insertions/deletions from `git diff --shortstat T..main` or equivalent).

Keep prose consistent with prior entries in `CHANGELOG.md`.

## 7. Commit and push

- Stage: `Cargo.toml`, `CHANGELOG.md`, any test/feature files, and `Cargo.lock` if it changed.
- Commit with a clear message, e.g. `chore(release): prepare vX.Y.Z`.
- Push: `git push -u origin "release/$V"`.

## 8. Open the release PR (make-pr)

- Read and run the workflow in `.cursor/commands/make-pr.md` for the current branch (`release/$V` → `main`).
- PR title example: `Release vX.Y.Z`.
- PR body: short summary + pointer to the new CHANGELOG section + link to full diff vs `main`.

Ensure the reported PR URL is the full GitHub URL (clickable).

## Quick reference

| Condition | Action |
|-----------|--------|
| Not on `main` | Stop |
| V₀ > L | Keep Cargo.toml version; still branch + changelog + PR |
| V₀ ≤ L | Bump patch to **V**, fix CLI version tests, then continue |

**Files often touched:** `Cargo.toml`, `Cargo.lock`, `features/cli/cli.feature`, `CHANGELOG.md`.
