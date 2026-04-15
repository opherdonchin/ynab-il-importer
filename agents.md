# Agent Instructions

## First Steps

- Read `documents/project_context.md` and `documents/plan.md` first.
- Use `project_context.md` for general project goals and priorities.
- Use `plan.md` to understand the current step, what is done, and what remains.

## Plan Discipline

- All work should follow `documents/plan.md` unless explicitly instructed otherwise.
- Update `documents/plan.md` before every commit.
- Archive the previous plan in `documents/archive/` with a timestamped filename before editing it.
- Keep `plan.md` focused on current state and next steps, not long historical notes.

## Working Style

- Prefer explicit, strict boundaries over compatibility layers.
- Do not preserve old behavior, formats, or compatibility paths unless they clearly serve the active plan.
- Do not “hedge” for incomplete or malformed inputs unless explicitly requested; prefer failing fast and fixing the real boundary.
- Validate once, use often: don't write code that is defensive about its inputs if you can see that its inputs are guaranteed by the calling layer. If not, check if object validity can be ensured earlier in some way and then utilized. In general, avoid defensive, over-engineered programming or code that is ignorant of its context.
- Centralize construction, normalization, flattening, explosion, and validation at clear boundaries instead of spreading them through the codebase.
- Prefer dataframe-level operations over Python loops, dict unpacking, and ad hoc row logic whenever practical.
- Be honest in naming. Do not use names like `precompute_*` for plain full-dataframe passes. In general, be wary of paying attention to promises in variable or function names when the actual code doesn't support the naming that is provided. In such cases, work to make 'honest' code.

## Architecture Preferences

- Keep hierarchy only where it is semantically real.
- Prefer flat working dataframes for ordinary app logic.
- Keep nested structures only where they materially clarify the model, especially split transactions.
- Keep validation in schema or boundary layers where possible, not mixed into ordinary UI rendering.
- Avoid duplicating mutable state when immutable originals plus a working projection will do.
- If the current implementation direction starts conflicting with these principles, stop, reassess, and realign before continuing.

## Communication

- Use repo-relative markdown links when referencing files, for example `[app.py](src/ynab_il_importer/review_app/app.py)`.
- When making architectural choices with non-obvious consequences, explain the tradeoff briefly and choose the simpler, less state-heavy design unless instructed otherwise.
- If you encounter a meaningful architectural uncertainty, ask only after checking the code and existing plans carefully.

## Autonomy

- Default to acting without asking for permission for ordinary repo work.
- Automatically proceed with routine reads, searches, local file edits, focused refactors, debugging, artifact inspection, and normal workflow commands.
- Automatically run ordinary `pixi`, `python`, `pytest`, and git status/diff/log commands needed to complete the current task.
- Do not pause to ask before rerunning a command, restarting a local workflow step, or validating a likely fix when the action is low-risk and within the repo workspace.
- Only stop to ask when a choice has meaningful product or accounting consequences, when the action is destructive or hard to undo, or when external side effects are non-obvious.
- Prefer doing the next reasonable thing over asking a procedural question the user has already implicitly answered.

## Execution

- Use `pixi` for project commands.
- Commit after each successful sub-step.
- Keep changes small, testable, and easy to review.
