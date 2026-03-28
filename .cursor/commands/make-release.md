# Make Release

Prepare a release PR from `main`: validate version against the latest tag, bump patch if needed, update CLI version tests, add a `CHANGELOG.md` section, push `release/{version}`, then open a PR using **make-pr**.

**Instructions:** Follow every step in [.cursor/skills/make-release/SKILL.md](../skills/make-release/SKILL.md) in order.

After the release branch is pushed, run the **Make PR** workflow from [.cursor/commands/make-pr.md](make-pr.md) so the release PR is created or updated against `main`.
