# Agent instructions

##
CLAUDE.md is a soft link to AGENTS.md so you only need to edit one of them if you need to edit them.

## General coding practices

Write readable code. Readable code is always preferred more than verbose comments. To ensure readability make use of "hoisting" capabilities of languages like python, javascript, etc. Write top-level logic first and progressively declare helper methods. It helps in review as well. While abstractions help, do not over-engineer either. Too many levels of abstractions also make it very hard to review and reason about the code.

Write tests that matter. Follow seperation of concerns in testing as well, when using a library there is no need to test the functionalities of the library itself since we can assume it has been tested already. Do not write tests that are fragile and require frequent changes.


## Python environments

Use **[uv](https://docs.astral.sh/uv/)** for all Python tooling in this project: virtual environments, dependency installation, and running Python commands (for example `uv run`, `uv sync`, `uv pip`). Do not rely on plain `pip`, `venv`, or ad-hoc interpreters when project work expects a managed environment.

## Git behaviour

Do not commit/push commits unless explicitly asked by the user in previous turn.
