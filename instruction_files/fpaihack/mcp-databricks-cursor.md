# Databricks MCP + Cursor — AI-Executable Setup Guide

> **Audience:** Cursor users who want Cursor's AI (Chat / Composer / Agent) to query Databricks catalogs, schemas, and tables directly from the editor.
>
> This guide uses the **Databricks CLI auth + `uc-mcp-proxy`** path because it is the simplest setup for hackathon users. It does **not** require creating a separate OAuth app.

---

## What this gives you

After setup, Cursor can use Databricks MCP tools to:

- list catalogs
- list schemas
- list tables
- describe tables
- run read-only SQL queries

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

For most users the SQL endpoint is the right choice — it supports catalog, schema, and table discovery with normal Databricks SQL commands.

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

### 1c. Check Cursor is installed

On Mac, verify Cursor.app is in `/Applications`:

```bash
ls /Applications/Cursor.app
```

On Windows, verify Cursor appears in the Start menu.

- **If missing:** follow [setup-cursor.md](setup-cursor.md)

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
- **If this fails:** MCP setup will also fail, so fix CLI auth first

---

## Step 3: Create the `.cursor/mcp.json` Config File

Cursor reads MCP configuration from one of two locations:

| Scope | Path |
|---|---|
| **Project-only** (recommended) | `<PROJECT_ROOT>/.cursor/mcp.json` |
| **Global (all projects)** | `~/.cursor/mcp.json` |

For the hackathon, project-scoped config is cleaner — it only activates MCP when you open this project.

### 3a. Create the `.cursor` directory in your project root

```bash
mkdir -p .cursor
```

### 3b. Write the config file

Create `.cursor/mcp.json` with this exact structure:

```json
{
  "mcpServers": {
    "databricks-sql": {
      "command": "uvx",
      "args": [
        "uc-mcp-proxy",
        "--url", "https://adb-7405616827368959.19.azuredatabricks.net/api/2.0/mcp/sql",
        "--auth-type", "databricks-cli",
        "--profile", "MHTEST"
      ]
    }
  }
}
```

Replace:

- `MHTEST` with your Databricks CLI profile name
- the workspace URL if you are not using the hackathon workspace

**CRITICAL:** Always include `"--auth-type", "databricks-cli"` as two separate strings in the `args` array. Without it, the proxy may fail to authenticate even when the CLI profile specifies the auth type.

---

## Step 4: Enable the MCP Server in Cursor

1. Open the project folder in Cursor (`File → Open Folder…` or `cursor ./my-project` in the terminal).
2. Open **Cursor Settings** (`Cmd+,` on Mac / `Ctrl+,` on Windows) and go to **MCP** in the sidebar.
   - Alternative: Command Palette (`Cmd+Shift+P` / `Ctrl+Shift+P`) → "MCP".
3. You should see `databricks-sql` listed. If the toggle next to it is off, switch it on.
4. Wait a few seconds — Cursor will spawn `uvx uc-mcp-proxy` and show a green indicator when connected.

### What you should see

- Server name: `databricks-sql`
- Status: **green / connected**
- Tools discovered: three (`execute_sql`, `execute_sql_read_only`, `poll_sql_result`)

### If the server shows red / failed

1. Check the URL is complete (starts with `https://` and includes the full workspace host).
2. Check `--auth-type` is in the args.
3. Run `databricks auth login --profile PROFILE_NAME` to refresh the OAuth token.
4. Confirm `uvx uc-mcp-proxy --help` works at the command line.

---

## Step 5: Restart Cursor (if needed)

Cursor usually picks up `.cursor/mcp.json` changes live, but if tools don't appear within 30 seconds:

1. Fully quit Cursor (`Cmd+Q` on Mac / close all windows on Windows).
2. Relaunch and reopen the project folder.

---

## Step 6: Test the Databricks MCP Connection

Open Chat (`Cmd+L` / `Ctrl+L`) or Agent mode (`Cmd+I` then toggle "Agent") and give it a discovery task.

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

When Cursor prompts for permission to call the MCP tool, approve it. You can set "always allow" for this server to skip the prompt on repeat calls.

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

## Optional: Global (Cross-Project) Config

If you want Databricks MCP available in every Cursor project, put the same config at:

```text
~/.cursor/mcp.json
```

instead of (or in addition to) the per-project file. The project-level file takes precedence when both exist.

---

## Troubleshooting

### Problem: Server shows up but status is red / failed

Check these in order:

1. Does this work?

```bash
databricks current-user me --profile PROFILE_NAME
```

2. Is the URL complete, including workspace host and `/api/2.0/mcp/sql`?

```text
https://<workspace-host>/api/2.0/mcp/sql
```

3. Are both of these args present?

```text
--auth-type databricks-cli
```

4. Did you use the correct Databricks CLI profile name?

### Problem: `uvx uc-mcp-proxy --help` fails

- `uv` is missing or broken
- Fix `uv` first using [setup-python.md](setup-python.md)

### Problem: Server is connected but Cursor never calls the tool

- Explicitly mention "databricks-sql" or "MCP tools" in your prompt (e.g., "Use the databricks-sql MCP tools to…").
- In Cursor Settings → MCP, confirm the server toggle is on.
- In Chat/Agent mode, approve the tool permission prompt when it appears.

### Problem: Query returns permission or resource errors

- Verify you can read the catalog/schema in the Databricks UI.
- Verify the target schema exists.
- Use fully qualified `catalog.schema.table` names.

### Problem: You want catalogs/schemas/tables but used the wrong endpoint

Use:

```text
/api/2.0/mcp/sql
```

Do **not** use the `functions`, `genie`, or `vector-search` endpoints for this basic discovery workflow.

---

## Reference: Verified Working Config

A working `.cursor/mcp.json` for the hackathon workspace:

```json
{
  "mcpServers": {
    "databricks-sql": {
      "command": "uvx",
      "args": [
        "uc-mcp-proxy",
        "--url", "https://adb-7405616827368959.19.azuredatabricks.net/api/2.0/mcp/sql",
        "--auth-type", "databricks-cli",
        "--profile", "MHTEST"
      ]
    }
  }
}
```

Matching `~/.databrickscfg` profile:

```ini
[MHTEST]
host         = https://adb-7405616827368959.19.azuredatabricks.net
auth_type    = databricks-cli
account_id   = 06ddeeae-1d28-4371-91f3-a3c83e501698
workspace_id = 7405616827368959
```

These Databricks MCP queries should succeed from Cursor Chat/Agent:

```sql
SHOW CATALOGS
SHOW SCHEMAS IN fp_hack
SHOW TABLES IN fp_hack.<your_schema>
```

---

## References

- Cursor MCP docs: <https://cursor.com/docs/mcp>
- Databricks external MCP client docs: <https://learn.microsoft.com/en-us/azure/databricks/generative-ai/mcp/connect-external-services>
- Databricks managed MCP overview: <https://learn.microsoft.com/en-us/azure/databricks/generative-ai/mcp/managed-mcp>

---

## Questions or problems?

Ping one of the mentors on Teams with:

- What you tried
- The exact error message (a screenshot helps)
