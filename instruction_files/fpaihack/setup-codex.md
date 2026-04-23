# Setting Up OpenAI Codex (Web App + VS Code + CLI)

[Codex](https://openai.com/codex) is OpenAI's coding agent. It comes in three flavors:

- **Codex Web App** — a browser-based interface at [chatgpt.com/codex](https://chatgpt.com/codex)
- **Codex VS Code extension** — Codex integrated directly into VS Code (easiest for most people)
- **Codex Desktop App** — a standalone desktop app for Mac and Windows
- **Codex CLI** — a terminal-based coding agent (power users)

---

## Prerequisites

- A **ChatGPT Plus, Pro, Business, Edu, or Enterprise** plan (Codex is included with these plans)
- For the CLI: **Node.js 18+** is required (see below for install instructions)

---

## Part 1 — Codex Web App

The web app is the easiest way to get started. No installation needed.

1. Go to [chatgpt.com/codex](https://chatgpt.com/codex).
2. Sign in with your OpenAI / ChatGPT account.
3. Connect a GitHub repository when prompted.
4. Send a coding task — Codex will read your code, make changes, and create a PR.

### What it can do

- Read and understand your GitHub repos
- Write code and open pull requests
- Run tests in a sandboxed environment
- You can also tag `@codex` in GitHub PR comments to invoke it directly

> **Tip:** Create a git checkpoint before each task so you can easily revert if needed.

---

## Part 2 — Codex in VS Code (recommended)

If you use VS Code, this is the easiest way to get Codex. No Node.js or CLI install needed.

### Install

1. Open VS Code.
2. Go to the Extensions panel (`Cmd+Shift+X` on Mac / `Ctrl+Shift+X` on Windows).
3. Search for **"Codex"** and install the official extension by OpenAI.
4. Alternatively, follow the setup guide at [developers.openai.com/codex/ide](https://developers.openai.com/codex/ide).

### First run

1. Open a project folder in VS Code.
2. Open the Codex panel (look for the Codex icon in the sidebar).
3. Sign in with your ChatGPT account when prompted.
4. You're ready — type coding tasks directly in the panel.

---

## Part 3 — Codex CLI (power users)

The standalone CLI runs in your terminal. This is optional if you already have the VS Code extension.

### Step 1 — Make sure you have Node.js

Codex CLI requires **Node.js 18 or later**.

Check if you already have it:

```bash
node --version
```

If you see `v18.x.x` or higher, you're good — skip to Step 2.

If not, install Node.js:

**Mac (Homebrew):**

```bash
brew install node
```

**Mac (without Homebrew):**

Download the installer from [nodejs.org](https://nodejs.org/) — pick the LTS version.

**Windows:**

```powershell
winget install OpenJS.NodeJS.LTS
```

Or download the installer from [nodejs.org](https://nodejs.org/) — pick the LTS version. **Make sure "Add to PATH" is checked during installation.**

After installing, close and reopen your terminal, then verify:

```bash
node --version
```

### Step 2 — Install Codex CLI

**Using npm (Mac, Windows, Linux):**

```bash
npm install -g @openai/codex
```

**Using Homebrew (Mac only):**

```bash
brew install codex
```

Verify it worked:

```bash
codex --version
```

### Step 3 — Sign in

Run:

```bash
codex
```

You'll be prompted to sign in. Choose **"Sign in with ChatGPT"** if you have a ChatGPT plan (recommended). You can also use an OpenAI API key if you prefer.

### Step 4 — Use it

Navigate to a project folder and start Codex:

```bash
cd my-project
codex
```

Then type natural language requests, just like you would with Claude Code.

---

## Windows notes

- Codex CLI works natively on Windows, but macOS and Linux are the most tested platforms.
- If you run into issues on Windows, consider using **WSL** (Windows Subsystem for Linux). Install WSL, then follow the Mac/Linux instructions inside your WSL terminal.
- Your Windows files are accessible inside WSL at `/mnt/c/`.

---

## Part 4 — Codex Desktop App

A standalone desktop app that runs Codex on your local project folders — no browser, no terminal, no VS Code required.

### Install

**Mac:**

- [Download for Apple Silicon (M1/M2/M3/M4)](https://persistent.oaistatic.com/codex-app-prod/Codex.dmg)
- [Download for Intel](https://persistent.oaistatic.com/codex-app-prod/Codex-latest-x64.dmg)

Open the `.dmg` file and drag Codex to your Applications folder.

**Windows:**

- [Download from Microsoft Store](https://get.microsoft.com/installer/download/9PLM9XGG6VKS?cid=website_cta_psi)

Or visit the [Codex app page](https://developers.openai.com/codex/app) for the latest links.

### First run

1. Launch the Codex app.
2. Sign in with your ChatGPT account or OpenAI API key.
3. Choose a project folder for Codex to work in.
4. Send a coding task — Codex reads your code and makes changes locally.

> **Tip:** Create a git commit before each task so you can easily revert changes if needed.

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `codex: command not found` | Close and reopen your terminal. Make sure npm's global bin folder is in your PATH. Run `npm config get prefix` to find it. |
| `npm: command not found` | You need Node.js installed first. See Step 1 above. |
| Node version too old | Run `node --version`. If below 18, update Node.js using the instructions in Step 1. |
| Sign-in fails | Make sure you have an active ChatGPT Plus, Pro, Business, Edu, or Enterprise plan. |
| Permission error with npm install | Don't use `sudo`. Instead, fix npm permissions: [npm docs](https://docs.npmjs.com/resolving-eacces-permissions-errors-when-installing-packages-globally). |

---

## Docs and resources

- [Codex VS Code extension](https://developers.openai.com/codex/ide)
- [Codex quickstart](https://developers.openai.com/codex/quickstart)
- [Codex CLI docs](https://developers.openai.com/codex/cli)
- [Codex CLI on GitHub](https://github.com/openai/codex)
- [Codex web app](https://chatgpt.com/codex)
- [Codex desktop app](https://developers.openai.com/codex/app)
