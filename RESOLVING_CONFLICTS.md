# Resolving Pull Request Merge Conflicts

GitHub shows the "This branch has conflicts that must be resolved" banner when the branch behind a pull request (PR)
has diverged from the base branch. Those markers do **not** automatically appear in the repository—you must pull the PR
branch, resolve the conflicts, and push the cleaned history yourself. This guide walks through both the GitHub web
editor and the fully local workflow so you can pick the option that fits your setup.

---

## Option A – Resolve Conflicts in the GitHub UI

1. Open the PR in your browser and click **Resolve conflicts** (GitHub only exposes the button when your account has
   permission to push to the branch).
2. For each conflicting file:
   - Review the sections split by `<<<<<<<`, `=======`, and `>>>>>>>`.
   - Delete the markers and adjust the code so the remaining content is exactly what should ship.
   - Click **Mark as resolved** once you finish a file.
3. When every file is marked resolved, click **Commit merge**. GitHub will create a merge commit on the PR branch and
   rerun status checks automatically.
4. If checks fail, fix the underlying issue locally, push new commits, and repeat the UI conflict flow if new conflicts
   appear.

The UI editor is convenient for small conflicts, but it hides your local test environment. For anything non-trivial,
prefer the local workflow below so you can run the CLI and automated tests before pushing.

---

## Option B – Resolve Conflicts Locally (Recommended)

1. **Update your local clone**
   ```bash
   git fetch origin
   ```
2. **Check out the PR branch**
   ```bash
   git checkout <pr-branch>
   ```
   Replace `<pr-branch>` with the branch name listed at the top of the pull request.
3. **Bring in the latest base branch changes**
   *If the PR targets `main`:*
   ```bash
   git merge origin/main
   ```
   *Or rebase instead of merge if that is your workflow:*
   ```bash
   git rebase origin/main
   ```
4. **Resolve each conflict locally**
   * Git will stop on files that conflict. Open them in your editor.
   * Look for sections that look like:
     ```
     <<<<<<< HEAD
     ...your branch's content...
     =======
     ...base branch content...
     >>>>>>> origin/main
     ```
   * Edit the file so that it contains the desired final content, removing the conflict markers.
5. **Mark conflicts as resolved and continue**
   ```bash
   git add <resolved-file>
   ```
   Repeat for every file you edited.
   *If you are rebasing*, continue with:
   ```bash
   git rebase --continue
   ```
6. **Run the project tests locally** (this repo uses Node’s built-in test runner):
   ```bash
   cd jupiter-swap-tool
   npm test
   ```
   Use the CLI (`node cli_trader.js`) if you need to sanity-check runtime behaviour.
7. **Commit the merge (or finish the rebase) and push**
   *If you merged:*
   ```bash
   git commit
   git push origin <pr-branch>
   ```
   *If you rebased:* you may need to force push:
   ```bash
   git push --force-with-lease origin <pr-branch>
   ```

After pushing, refresh the PR page; GitHub should no longer report conflicts. If conflicts persist, repeat the steps to
ensure you picked up the latest base branch updates.

---

## Quick Checklist

- [ ] Pull the PR branch locally or open the **Resolve conflicts** UI.
- [ ] Remove every `<<<<<<<` / `=======` / `>>>>>>>` marker and keep the correct code.
- [ ] Run `npm test` (and manual CLI smoke tests if necessary).
- [ ] Push or commit via the UI so GitHub reruns checks.

Keeping this checklist handy ensures each PR lands cleanly with working tests.
