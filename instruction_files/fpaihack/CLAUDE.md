# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository purpose

This is the **Finity People AI Hackathon** setup repository (`FinityPeople/fpaihack`). It contains documentation and setup guides — no application code. The audience ranges from experienced developers to complete beginners.

## Key resources

- **Databricks workspace:** `https://adb-7405616827368959.19.azuredatabricks.net`
- **Shared catalog:** `fp_hack` (read everything, create your own schemas)
- **Marketplace (free data):** `https://adb-7405616827368959.19.azuredatabricks.net/marketplace?isFree=true&o=7405616827368959&sortBy=popularity`

## Document map

| File | Purpose |
|---|---|
| `README.md` | Entry point — automated one-liner setup + manual guides |
| `setup-claude.md` | Claude Desktop + Claude Code CLI install (Mac/Windows) |
| `setup-codex.md` | OpenAI Codex web app + CLI install (Mac/Windows) |
| `setup-cursor.md` | Cursor editor install — VS Code fork with bundled AI subscription (Mac/Windows) |
| `setup-python.md` | Python install via uv |
| `databricks-access-instructions.md` | Workspace access, CLI auth, marketplace data |
| `mcp-databricks-claude.md` | MCP bridge between Claude Code and Databricks (AI-executable) |
| `mcp-databricks-codex.md` | MCP bridge between Codex CLI and Databricks (AI-executable) |
| `mcp-databricks-cursor.md` | MCP bridge between Cursor and Databricks (AI-executable) |

## When helping users set up

1. Read all the setup guides listed above before starting.
2. Check what is already installed (`databricks --version`, `uv --version`, `claude --version`, `codex --version`, `cursor --version`, `node --version`, `code --version`).
3. Skip what's already done.
4. Some steps require user interaction (OAuth sign-in for Databricks CLI, Claude Code / Codex / Cursor first-run auth) — prompt the user to run those commands themselves.
5. The MCP setup guides (`mcp-databricks-claude.md` for Claude Code, `mcp-databricks-codex.md` for Codex CLI, `mcp-databricks-cursor.md` for Cursor) are AI-executable runbooks with exact commands, expected outputs, and failure handling — follow them step by step.

## Git workflow

All changes go through feature branches and PRs. Branch naming convention: `docs/<short-description>`. Merge to `main` via `gh pr merge`.
