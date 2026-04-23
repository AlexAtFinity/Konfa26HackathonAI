# Finity People AI Hackathon

Welcome to the AI Development Hackathon!

This hackathon is **free and open** — you can build whatever you want, with whatever tools and data you choose. This repo is here to support you with setup and getting started, but you're not limited to anything listed here. The goal is to explore, learn, and build something with AI. In the end, it's up to your group to decide what data to use and what to build during the hackathon.

## What this repo is for

This repo helps you get set up with **tools and data** so you can focus on building. It covers:

- **AI coding assistants** (Claude Code, OpenAI Codex, Cursor) to help you write code faster
- **Databricks workspace** with shared catalogs and free Marketplace datasets
- **Python environment** setup

### Bring any data you want

You're free to use any dataset for your project:

- **[Databricks Marketplace](https://adb-7405616827368959.19.azuredatabricks.net/marketplace?isFree=true&o=7405616827368959&sortBy=popularity)** — free datasets already available in our workspace (see [instructions](databricks-access-instructions.md))
- **[Kaggle Datasets](https://www.kaggle.com/datasets)** — thousands of free datasets across every domain
- **[Hugging Face Datasets](https://huggingface.co/datasets)** — ML-ready datasets, great for NLP and AI projects
- **Your own data** — bring anything you want to work with

The Databricks setup in this repo is there if you want an easy way to access and query shared data, but it's completely optional. You can run everything locally with downloaded datasets if you prefer.

---

## Getting started

There are two ways to get set up — pick whichever feels more comfortable:

### Before you start — you need a paid subscription

The AI coding tools require a paid plan. **Sign up and log in first**, then come back here.

| Tool | Sign up | Required plan |
|---|---|---|
| **Claude Code** | [claude.ai](https://claude.ai) | Pro ($20/mo), Max ($100–200/mo), Teams, or Enterprise. The free plan does **not** include Claude Code. |
| **Codex CLI** | [chatgpt.com](https://chatgpt.com) | Plus, Pro, Business, Edu, or Enterprise. |
| **Cursor** | [cursor.com](https://cursor.com) | Pro ($20/mo) or higher. Cursor is **editor + AI in one** — no separate Claude or ChatGPT plan needed. |

You only need one — pick whichever you prefer. The first time you run the tool, it will open a browser window for you to sign in. No API keys or tokens to copy around.

---

### Option A: Let the AI set up everything for you

The fastest path. Install one AI tool, then let it read the setup guides and handle the rest.

#### Step 1 — Install the AI tool

Pick **one** and run the install command:

**Claude Code — Mac / Linux:**

```bash
curl -fsSL https://claude.ai/install.sh | bash
```

**Claude Code — Windows PowerShell:**

```powershell
irm https://claude.ai/install.ps1 | iex
```

**Codex CLI — Mac / Linux / Windows** (requires Node.js 18+ — install from [nodejs.org](https://nodejs.org/) first if needed):

```bash
npm install -g @openai/codex
```

**Cursor — Mac / Windows:**

Download and install the app from [cursor.com/download](https://cursor.com/download). No terminal commands and no PATH setup — Cursor is a GUI app.

> **Important (Claude Code and Codex only):** After installing, **close and reopen your terminal** so the command is available on your PATH. This is especially common on Windows. If you skip this, the next step will fail with "command not found". Cursor users can skip this — the app doesn't rely on a shell command.

#### Step 2 — Clone this repo

```bash
git clone https://github.com/FinityPeople/fpaihack.git
cd fpaihack
```

> **Don't have git?** On Windows, download [Git for Windows](https://git-scm.com/downloads/win) first. On Mac, running `git` in the terminal will prompt you to install the Xcode Command Line Tools automatically.

#### Step 3 — Start the AI and paste the setup prompt

**Claude Code or Codex:** In your terminal, start the AI:

```bash
claude
```

or:

```bash
codex
```

The first time you run it, a browser window will open for you to sign in. Complete the sign-in.

**Cursor:** Launch the Cursor app, open the cloned repo folder (`File → Open Folder…`), sign in when prompted, then open Chat (`Cmd+L` / `Ctrl+L`) or Agent mode (`Cmd+I` / `Ctrl+I`).

Then paste the prompt for your chosen tool:

**If you're using Claude Code:**

```
Read all the setup guides in this repo (setup-python.md, setup-codex.md,
setup-cursor.md, databricks-access-instructions.md, mcp-databricks-claude.md).
Check what is already installed on this machine and help me install and
configure everything that is missing. Walk me through anything that needs
my input (like signing in). Skip what is already done.
```

**If you're using Codex CLI:**

```
Read all the setup guides in this repo (setup-python.md, setup-claude.md,
setup-cursor.md, databricks-access-instructions.md, mcp-databricks-codex.md).
Check what is already installed on this machine and help me install and
configure everything that is missing. Walk me through anything that needs
my input (like signing in). Skip what is already done.
```

**If you're using Cursor:**

```
Read all the setup guides in this repo (setup-python.md, setup-claude.md,
setup-codex.md, databricks-access-instructions.md, mcp-databricks-cursor.md).
Check what is already installed on this machine and help me install and
configure everything that is missing. Walk me through anything that needs
my input (like signing in). Skip what is already done.
```

#### What happens next

1. The AI reads the setup guides in this repo
2. It checks what's already installed on your machine
3. It installs and configures everything that's missing
4. It asks you for input when needed (e.g. signing in to Databricks)

---

### Option B: Follow the guides manually

Follow the setup guides below in order. Each guide is written step-by-step for all experience levels.

#### 0. Install VS Code (recommended editor)

[Visual Studio Code](https://code.visualstudio.com/) is a free code editor that works great with all the tools in this hackathon.

- **Mac:** Download from [code.visualstudio.com](https://code.visualstudio.com/) or run `brew install --cask visual-studio-code`
- **Windows:** Download from [code.visualstudio.com](https://code.visualstudio.com/) or run `winget install Microsoft.VisualStudioCode`

VS Code has extensions for Databricks, Python, and more — the setup guides will point you to the right ones.

#### 1. Set up your AI coding tools

Pick one — these are the AI assistants you'll use to write code during the hackathon:

| Guide | What it covers |
|---|---|
| [Setting up Claude](setup-claude.md) | Claude Desktop app + Claude Code VS Code extension + CLI (Anthropic) |
| [Setting up Codex](setup-codex.md) | Codex web app + VS Code extension + CLI (OpenAI) |
| [Setting up Cursor](setup-cursor.md) | Cursor editor — VS Code fork with AI built in (editor + AI subscription bundled) |

#### 2. Set up Python

If your project needs Python (most will):

| Guide | What it covers |
|---|---|
| [Setting up Python with uv](setup-python.md) | Installing Python and managing packages using uv |

#### 3. Access the Databricks workspace

Our shared data platform for the hackathon:

| Guide | What it covers |
|---|---|
| [Databricks access instructions](databricks-access-instructions.md) | Browser access, CLI setup, and workspace permissions |

**Workspace URL:** https://adb-7405616827368959.19.azuredatabricks.net/

#### 4. Connect your AI tool to Databricks via MCP (optional)

Want your AI coding tool to query Databricks data directly from the terminal? These guides set up the MCP (Model Context Protocol) bridge so the AI can browse catalogs, run SQL, and interact with your workspace data.

| Guide | What it covers |
|---|---|
| [Databricks MCP + Claude Code setup](mcp-databricks-claude.md) | MCP config for Claude Code — `.mcp.json`, endpoint selection, verification |
| [Databricks MCP + Codex setup](mcp-databricks-codex.md) | MCP config for Codex CLI — `codex mcp add`, config.toml, verification |
| [Databricks MCP + Cursor setup](mcp-databricks-cursor.md) | MCP config for Cursor — `.cursor/mcp.json`, Settings → MCP panel, verification |

---

## What you'll need

At minimum:

- A laptop (Mac or Windows)
- A browser (Chrome, Edge, Safari, or Firefox)
- Your corporate credentials (for Databricks access)
- A **paid** subscription for **one** of these AI tools:
  - **Anthropic** — [claude.ai](https://claude.ai) — Pro, Max, Teams, or Enterprise
  - **OpenAI** — [chatgpt.com](https://chatgpt.com) — Plus, Pro, Business, Edu, or Enterprise
  - **Cursor** — [cursor.com](https://cursor.com) — Pro ($20/mo) or higher (editor + AI bundled)

---

## Quick links

| Resource | Link |
|---|---|
| VS Code download | https://code.visualstudio.com/ |
| Databricks workspace | https://adb-7405616827368959.19.azuredatabricks.net/ |
| Claude Desktop download | https://claude.com/download |
| Claude Code docs | https://code.claude.com/docs/en/setup |
| Codex web app | https://chatgpt.com/codex |
| Codex CLI on GitHub | https://github.com/openai/codex |
| Cursor download | https://cursor.com/download |
| Cursor docs | https://cursor.com/docs |
| uv (Python toolchain) | https://docs.astral.sh/uv/ |

---

## Repo structure

```
├── README.md                          ← You are here
├── setup-claude.md                    ← Claude Desktop + Claude Code setup
├── setup-codex.md                     ← OpenAI Codex web app + CLI setup
├── setup-cursor.md                    ← Cursor editor setup
├── setup-python.md                    ← Python installation with uv
├── databricks-access-instructions.md  ← Databricks workspace access
├── mcp-databricks-claude.md           ← Connect Claude Code to Databricks via MCP
├── mcp-databricks-codex.md            ← Connect Codex CLI to Databricks via MCP
└── mcp-databricks-cursor.md           ← Connect Cursor to Databricks via MCP
```

---

## Questions or problems?

Ping one of the mentors on Teams with:

- What you tried
- The exact error message (a screenshot helps)
