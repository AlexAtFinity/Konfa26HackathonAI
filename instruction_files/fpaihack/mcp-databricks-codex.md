# Databricks MCP + Codex — AI-Executable Setup Guide

> **Audience:** Codex users who want OpenAI Codex CLI to query Databricks catalogs, schemas, and tables directly from the terminal.
>
> This guide uses the **Databricks CLI auth + `uc-mcp-proxy`** path because it is the simplest setup for hackathon users. It does **not** require creating a separate OAuth app.

---

## What this gives you

After setup, Codex can use Databricks MCP tools to:

- list catalogs
- list schemas
- list tables
- describe tables
- run read-only SQL queries

This guide was tested against the hackathon workspace on **April 16, 2026**.

---

## Inputs Required

Before starting, collect these values:

| Variable | Description | Example |
|---|---|---|
| `WORKSPACE_URL` | Full Databricks workspace URL | `https://adb-7405616827368959.19.azuredatabricks.net` |
| `PROFILE_NAME` | Databricks CLI profile name | `MHTEST` |
| `MCP_ENDPOINT` | Databricks MCP endpoint to use | `sql` |

For the hackathon workspace, use:

- `WORKSPACE_URL=https://adb-7405616827368959.19.azuredatabricks.net`
- `MCP_ENDPOINT=sql`

For most users, the SQL endpoint is the right choice because it supports catalog, schema, and table discovery with normal Databricks SQL commands.

---

## Step 1: Verify Prerequisites

### 1a. Check Databricks CLI

```bash
databricks --version
```

- **Expected:** `Databricks CLI v0.2xx.x` or newer
- **If missing:** follow [databricks-access-instructions.md](databricks-access-instructions.md)

### 1b. Check `uvx`

```bash
uvx --version
```

- **Expected:** version string such as `uvx 0.11.7`
- **If missing:** follow [setup-python.md](setup-python.md) and install `uv`

### 1c. Check Codex CLI

```bash
codex --version
```

- **Expected:** version string such as `codex-cli 0.121.0`
- **If missing:** follow [setup-codex.md](setup-codex.md)

### 1d. Check the Databricks MCP proxy launcher

```bash
uvx uc-mcp-proxy --help
```

- **Expected:** help text showing `--url`, `--profile`, and `--auth-type`
- **If this fails:** `uvx` cannot launch the proxy package yet. Fix `uv` first.

---

## Step 2: Verify Databricks CLI Authentication

### 2a. Check whether your Databricks profile exists

```bash
grep -A4 "\[PROFILE_NAME\]" ~/.databrickscfg
```

Replace `PROFILE_NAME` with your actual profile name, for example:

```bash
grep -A4 "\[MHTEST\]" ~/.databrickscfg
```

- **If the profile exists:** continue
- **If not found:** configure the Databricks CLI first using [databricks-access-instructions.md](databricks-access-instructions.md)

### 2b. Authenticate if needed

```bash
databricks auth login --profile PROFILE_NAME
```

This opens a browser window for OAuth sign-in. The user must complete this step themselves.

### 2c. Verify the profile works

```bash
databricks current-user me --profile PROFILE_NAME
```

- **Expected:** JSON with your Databricks user information
- **If this fails:** Codex MCP setup will also fail, so fix CLI auth first

---

## Step 3: Add the Databricks MCP Server to Codex

Codex stores MCP server configuration in `~/.codex/config.toml`. The easiest way to add a server is the built-in CLI command `codex mcp add`.

Run:

```bash
codex mcp add databricks-sql -- \
  uvx uc-mcp-proxy \
  --url https://adb-7405616827368959.19.azuredatabricks.net/api/2.0/mcp/sql \
  --auth-type databricks-cli \
  --profile MHTEST
```

Replace:

- `databricks-sql` if you want a different server name
- `MHTEST` with your Databricks CLI profile
- the workspace URL if you are not using the hackathon workspace

### What this does

It tells Codex to launch a local stdio MCP server using:

- `uvx`
- the `uc-mcp-proxy` package
- the Databricks SQL MCP endpoint
- your existing Databricks CLI credentials

No extra OAuth client setup is required in this flow.

---

## Step 4: Verify the Codex MCP Configuration

### 4a. List configured MCP servers

```bash
codex mcp list
```

You should see a row for `databricks-sql`.

### 4b. Inspect the server config

```bash
codex mcp get databricks-sql
```

You should see:

- `command: uvx`
- `args: uc-mcp-proxy --url ... --auth-type databricks-cli --profile ...`

### Important note about Auth status

If `codex mcp list --json` shows:

```json
"auth_status": "unsupported"
```

that is **normal** for this setup.

Why: `uc-mcp-proxy` is a local stdio server that reuses Databricks CLI credentials. Codex is **not** doing MCP OAuth directly here, so `unsupported` does **not** mean the Databricks connection is broken.

---

## Step 5: Start a Fresh Codex Session

Codex loads MCP servers when a session starts.

If Codex is already running:

1. Exit the current session
2. Start Codex again in the same project folder

```bash
cd your-project
codex
```

Inside the Codex TUI, run:

```text
/mcp
```

You should see the active MCP servers for the session.

---

## Step 6: Test the Databricks MCP Connection

Once Codex is running, give it a small discovery task.

### Test 1: List catalogs

Prompt:

```text
Use the databricks-sql MCP tools to run SHOW CATALOGS and tell me the result.
```

### Test 2: List schemas in the shared hackathon catalog

Prompt:

```text
Use the databricks-sql MCP tools to run SHOW SCHEMAS IN fp_hack and tell me the result.
```

### Test 3: List tables in your schema

Prompt:

```text
Use the databricks-sql MCP tools to run SHOW TABLES IN fp_hack.<your_schema_name> and tell me the result.
```

Replace `<your_schema_name>` with your actual schema, for example `mh_test_hack`.

---

## Useful Discovery Queries

These are the most useful first queries:

```sql
SHOW CATALOGS
SHOW SCHEMAS IN fp_hack
SHOW TABLES IN fp_hack.your_schema
DESCRIBE TABLE fp_hack.your_schema.your_table
SELECT * FROM fp_hack.your_schema.your_table LIMIT 5
```

Always use fully qualified names:

```text
catalog.schema.table
```

---

## Optional: Configure Codex Manually with `config.toml`

If you prefer to edit config directly instead of using `codex mcp add`, add this to `~/.codex/config.toml`:

```toml
[mcp_servers.databricks-sql]
command = "uvx"
args = [
  "uc-mcp-proxy",
  "--url",
  "https://adb-7405616827368959.19.azuredatabricks.net/api/2.0/mcp/sql",
  "--auth-type",
  "databricks-cli",
  "--profile",
  "MHTEST",
]
```

Codex also supports project-scoped MCP config in:

```text
.codex/config.toml
```

That can be useful if you want Databricks MCP enabled only for one project instead of globally.

---

## Troubleshooting

### Problem: `codex mcp add ...` works, but Codex does not appear to use the tools

- Start a **fresh** Codex session
- In the TUI, run `/mcp`
- Verify the server name is present

### Problem: `uvx uc-mcp-proxy --help` fails

- `uv` is missing or broken
- fix `uv` first using [setup-python.md](setup-python.md)

### Problem: Databricks CLI works, but Codex MCP queries fail

Check these in order:

1. Does this work?

```bash
databricks current-user me --profile PROFILE_NAME
```

2. Does this work?

```bash
codex mcp get databricks-sql
```

3. Does the URL include the **full workspace host** and the SQL endpoint?

```text
https://<workspace-host>/api/2.0/mcp/sql
```

4. Did you include both of these arguments?

```text
--auth-type databricks-cli
```

5. Did you use the correct Databricks CLI profile?

### Problem: Query returns permission or resource errors

- Verify that you can access the catalog or schema in Databricks
- Verify that the target schema exists
- If querying tables, make sure you own or can read them

### Problem: You want catalogs/schemas/tables, but used the wrong endpoint

Use:

```text
/api/2.0/mcp/sql
```

Do **not** use the `functions`, `genie`, or `vector-search` endpoints for this basic discovery workflow.

---

## Reference: Verified Working Setup in the Hackathon Workspace

This exact Codex MCP command was tested successfully:

```bash
codex mcp add databricks-sql -- \
  uvx uc-mcp-proxy \
  --url https://adb-7405616827368959.19.azuredatabricks.net/api/2.0/mcp/sql \
  --auth-type databricks-cli \
  --profile MHTEST
```

These Databricks MCP queries also succeeded through a fresh Codex session:

```sql
SHOW CATALOGS
SHOW SCHEMAS IN fp_hack
SHOW TABLES IN fp_hack.mh_test_hack
```

---

## References

- OpenAI Codex MCP docs: <https://developers.openai.com/codex/mcp>
- OpenAI Codex config docs: <https://developers.openai.com/codex/config-advanced>
- Databricks external MCP client docs: <https://learn.microsoft.com/en-us/azure/databricks/generative-ai/mcp/connect-external-services>
- Databricks managed MCP overview: <https://learn.microsoft.com/en-us/azure/databricks/generative-ai/mcp/managed-mcp>
