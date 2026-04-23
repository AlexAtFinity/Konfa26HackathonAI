# Accessing the Databricks Workspace

Welcome! This guide walks you through getting into our Databricks workspace. The browser steps are identical on Windows and Mac.

**Workspace URL:** <https://adb-7405616827368959.19.azuredatabricks.net/>

---

## 1. Browser access (everyone starts here)

This is all most people need.

1. Open any modern browser — Chrome, Edge, Safari, or Firefox.
2. Go to <https://adb-7405616827368959.19.azuredatabricks.net/>.
3. You'll be redirected to the Microsoft / Entra ID sign-in page. Use your **normal corporate credentials** (the same ones you use for Outlook, Teams, etc.).
4. Complete MFA if prompted.
5. You're in. There's no separate Databricks password.

**Tip:** Bookmark the workspace URL directly. If you Google "Databricks login" you may land on the wrong tenant.

### Troubleshooting

| Symptom | What to do |
|---|---|
| "You don't have access to this workspace" | Your account isn't assigned to the workspace yet. Message the admin. |
| Stuck in a redirect loop | Clear cookies for `login.microsoftonline.com` and `azuredatabricks.net`, then try an incognito window. |
| MFA prompt never arrives | Check the Microsoft Authenticator app or try a different MFA method from the sign-in page. |
| Sign-in succeeds but you see no catalogs/clusters | You're in, but you need data permissions. Message the admin. |

---

## 2. CLI/API and IDE access

Needed for AI to access Databricks Data. You can skip this section if you only need the web UI and dont plan AI to inteact with your data.

### Install the Databricks CLI

> **No Python required.** The modern Databricks CLI is a single standalone binary. Don't use `pip install databricks-cli` — that's the old, deprecated version.

Pick whichever install method works on your machine.

**Mac — option A: Homebrew (recommended if you have it)**

```bash
brew tap databricks/tap
brew install databricks
```

**Mac — option B: one-line installer (no Homebrew needed)**

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

**Windows — option A: winget (recommended)**

```powershell
winget install Databricks.DatabricksCLI
```

**Windows — option B: WSL / Git Bash**

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

**Windows — option C: manual download (if winget isn't available)**

1. Go to <https://github.com/databricks/cli/releases/latest>.
2. Download `databricks_cli_<version>_windows_amd64.zip`.
3. Unzip it and put `databricks.exe` somewhere on your `PATH` (e.g. `C:\Users\<you>\bin`, then add that folder to your user PATH environment variable).

Verify the install (any OS):

```bash
databricks --version
```

You should see `v0.2xx.x` or higher. If it shows a `0.17.x` version or says "Python", you've got the old deprecated CLI — uninstall it with `pip uninstall databricks-cli` and use one of the methods above.

### Configure authentication (OAuth U2M — no token needed)

Run:

```bash
databricks configure
```

When prompted:

- **Databricks Host:** `https://adb-7405616827368959.19.azuredatabricks.net`
- **Auth type:** pick `databricks-cli` (OAuth user-to-machine)

A browser window opens for SSO sign-in. Complete it and you're done — the CLI stores a refreshable OAuth token locally. No personal access token (PAT) required.

Test it:

```bash
databricks current-user me
```

You should see your user info.

---

## 3. Python setup

If you need Python locally (for the Databricks SDK, dbt, MCP servers, etc.), follow the **[Python setup guide](setup-python.md)**.

Once Python is set up, install the Databricks-specific packages in your project:

```bash
uv add databricks-sdk databricks-connect==16.4.* dbt-databricks
```

> `databricks-connect` version must match your cluster's DBR version (e.g. DBR 16.4 LTS → `databricks-connect==16.4.*`). Ask the admin if unsure.

### Quick sanity check

With the CLI configured (section 2) and Python set up, this should print your user info:

```bash
uv run python -c "from databricks.sdk import WorkspaceClient; print(WorkspaceClient().current_user.me().user_name)"
```

If it works, the SDK picked up your OAuth credentials from the CLI config automatically — no tokens in code, ever.

---

## 4. Get free data from the Databricks Marketplace

The Databricks Marketplace has free datasets you can use for the hackathon — weather, finance, demographics, geospatial, and more. No downloads or imports needed; the data lands directly in your workspace as a catalog.

### How to add a free dataset

1. Go to the **[Databricks Marketplace — free listings](https://adb-7405616827368959.19.azuredatabricks.net/marketplace?isFree=true&o=7405616827368959&sortBy=popularity)**.
2. Browse or search for a dataset you want to use.
3. Click on the dataset to open its detail page.
4. Click **Get Instant Access** (top-right corner of the page).
5. **Important:** Expand **More options** in the dialog that appears.
6. Change the **Catalog name** to something unique with your team prefix, e.g. `teamalpha_weather_data` or `mike_nyc_taxi`.
   - If you don't rename it, it may conflict with someone else who added the same dataset.
   - Use a prefix that identifies your team so everyone knows whose it is.
7. Click confirm. The dataset is now available as a catalog in your workspace.

You can now query it in notebooks, SQL editor, or from your local tools (CLI, SDK, AI coding agents).

### What you can do with the data

Once you have data in your workspace, the sky's the limit:

- **Use AI coding tools** (Claude Code, Codex CLI, Cursor) to explore, query, and build on the data
- **Use Genie** inside Databricks to ask questions about your data in natural language
- **Build ML models** using notebooks, MLflow, or your local Python environment
- **Create dashboards** and visualizations in Databricks SQL
- **Combine datasets** — join marketplace data with your own data or other marketplace datasets
- **Build AI/LLM applications** that use the data as context

You're free to build whatever you want — ML models, data pipelines, dashboards, AI apps, analysis notebooks, or anything else you can think of.

---

## 5. What you can do in the workspace

Once you're in, you have access to the `fp_hack` catalog where:

- You can **read everything** anyone else has created (tables, volumes, models, functions).
- You can **create your own schema** and put your own tables, volumes, models, and functions inside it.
- Inside your own schema you're the owner — you can modify, drop, and share with others as you like.
- You **cannot** modify or drop other people's objects. That's by design.

Convention: name your schema something recognizable, e.g. `mike_sandbox` or `team_xyz_experiments`, so others know whose it is.

---

## Questions or problems?

Ping one of the mentors on Teams with:

- What you tried
- The exact error message (a screenshot helps)
