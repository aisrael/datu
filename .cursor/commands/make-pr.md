# Make PR

Run this workflow to prepare or update a pull request for the current repo (github.com/aisrael/dtfu). Execute each step in order; use terminal commands and interpret their output to decide what to do next.

## 1. Check for staged changes

Run `git status` and check whether anything is staged.

- If there are **no** staged changes, go to **step 2**.
- If there **are** staged changes and we're on **main**, go to **step 3**.
- If there are staged changes but we're **not** on main (we're on a branch), you may suggest committing them first, then go to **step 4**.

## 2. No staged changes: check if we're on a branch

Run `git branch --show-current`. If the output is empty or we're in detached HEAD, say so and stop—there is no branch to make a PR from.

If we're on a branch (e.g. `feat/foo`), skip to **step 4** (compare branch with main, then PR steps). Do not create a new branch or commit.

## 3. On `main` with staged changes: branch and commit

Only do this when we're on `main` and there are staged changes.

1. **Analyze staged changes**  
   Run `git diff --cached` (and if useful `git diff --cached --stat`). From the diff, infer a short, descriptive branch name (e.g. `feat/add-xlsx-support`, `fix/parquet-schema`, `docs/readme`). Use lowercase, hyphens, and a conventional prefix like `feat/`, `fix/`, `docs/`, or `chore/`.

2. **Create the branch**  
   Run `git checkout -b <branch-name>` with that name.

3. **Commit**  
   Run `git commit -m "<message>"` with a clear one-line message that summarizes the staged changes.

Then go to **step 4** and continue (compare with main, then PR steps).

## 4. Compare current branch with main

We're on a branch (either from step 2 or step 3). Run:

- `git fetch origin main` (if needed)
- `git diff origin/main...HEAD` (three dots) to get the diff of this branch vs main
- Optionally `git log origin/main..HEAD --oneline` for a short commit summary

Use this diff and log to write the PR title and description in the next steps.

## 5. Check for an existing PR

Using GitHub CLI, check if there is already an open PR that has **this branch** as the head and **main** as the base on the remote `github.com/aisrael/dtfu`.

Run:

- `gh pr list --head $(git branch --show-current) --base main --repo aisrael/dtfu --state open`

If the output lists a PR, note its number and go to **step 6**.  
If no PR is listed, go to **step 7**.

## 6. Update existing PR

There is an open PR for this branch → main. Update it using the diff from step 4.

1. **Title**  
   One short line summarizing the changes (e.g. "Add XLSX read support", "Fix Parquet schema handling").

2. **Description**  
   A few sentences or bullets describing what changed and why. Optionally include a short "Key changes" or "Summary" section. You may paste a truncated or summarized diff if it helps.

Run:

- `gh pr edit <PR_NUMBER> --repo aisrael/dtfu --title "Your title" --body "Your description"`

Confirm the PR was updated and show the user the PR URL.

## 7. Create new PR

There is no open PR. Create one using the same repo and the diff from step 4.

1. **Title**  
   One short line summarizing the changes (same style as step 6).

2. **Description**  
   A clear, detailed description: what changed, why, and any important notes. Optionally include "Key changes" or "Summary" and a brief diff summary.

Run:

- `gh pr create --repo aisrael/dtfu --base main --head $(git branch --show-current) --title "Your title" --body "Your description"`

If the repo is the origin remote, you can omit `--repo aisrael/dtfu` and use `--base main` only. Confirm the PR was created and show the user the PR URL.

---

**Summary**

- No staged + not on a branch → stop and explain.
- No staged + on a branch → diff vs main, then existing PR → update, else create PR.
- Staged + on main → analyze diff, create branch, commit, then same PR flow (diff vs main, update or create PR).
