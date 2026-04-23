# Databricks MCP + Claude Code — AI-Executable Setup Guide

> **Audience:** An AI agent (Claude Code or similar) that needs to set up Databricks MCP connectivity from scratch.
> Each step includes the exact command to run, what output to expect, how to verify success, and what to do if it fails.

---

## Inputs Required

Before starting, collect these values from the user:

| Variable | Description | Example |
|---|---|---|
| `WORKSPACE_URL` | Full Databricks workspace URL | `https://adb-7405616827368959.19.azuredatabricks.net` |
| `PROFILE_NAME` | Name for the Databricks CLI profile | `MHTEST` |
| `ACCOUNT_ID` | Databricks account ID (optional, for Azure) | `06ddeeae-1d28-4371-91f3-a3c83e501698` |
| `WORKSPACE_ID` | Databricks workspace ID (optional, for Azure) | `7405616827368959` |
| `MCP_ENDPOINT` | Which MCP endpoint to use (default: `sql`) | `sql` |

The workspace URL pattern is:
- **Azure:** `https://adb-{workspace_id}.{region_index}.azuredatabricks.net`
- **AWS/GCP:** `https://{workspace}.cloud.databricks.com`

---

## Step 1: Verify Prerequisites

### 1a. Check Databricks CLI is installed

```bash
databricks --version
```

- **Expected:** `Databricks CLI v0.290.0` or higher
- **If missing:** Run `brew install databricks` (macOS) or see https://docs.databricks.com/dev-tools/cli/install.html

### 1b. Check uv/uvx is installed

```bash
uvx --version
```

- **Expected:** Version string like `uvx 0.11.7`
- **If missing:** Run `brew install uv` (macOS) or `curl -LsSf https://astral.sh/uv/install.sh | sh`

### 1c. Check Claude Code is available

```bash
claude --version
```

- **Expected:** Version string
- **If missing:** Run `npm install -g @anthropic-ai/claude-code`

---

## Step 2: Configure Databricks CLI Authentication

### 2a. Check if a profile already exists

```bash
grep -A4 "\[PROFILE_NAME\]" ~/.databrickscfg
```

- **If profile exists:** Skip to step 2c.
- **If not found:** Continue to 2b.

### 2b. Add the profile to `~/.databrickscfg`

Append the following block to `~/.databrickscfg`:

```ini
[PROFILE_NAME]
host         = WORKSPACE_URL
auth_type    = databricks-cli
account_id   = ACCOUNT_ID
workspace_id = WORKSPACE_ID
```

Replace all placeholders with actual values. The `account_id` and `workspace_id` lines are optional but recommended for Azure.

### 2c. Authenticate (requires user interaction)

```bash
databricks auth login --profile PROFILE_NAME
```

**IMPORTANT:** This opens a browser for OAuth. The user must complete this step themselves. Prompt the user to run the command interactively.

### 2d. Verify authentication works

```bash
databricks catalogs list --profile PROFILE_NAME
```

- **Expected:** JSON array of catalog objects (even if empty `[]` is OK)
- **If error "No credentials found":** Auth token expired — repeat step 2c
- **If error "No API found":** Wrong host URL — check `WORKSPACE_URL`

---

## Step 3: Choose the MCP Endpoint

Databricks exposes four managed MCP endpoints:

| Key | URL Path | Use When |
|---|---|---|
| `sql` | `/api/2.0/mcp/sql` | **Default choice.** Browse catalogs/schemas/tables, run any SQL query |
| `genie` | `/api/2.0/mcp/genie/{space_id}` | User has a Genie space and wants natural language queries |
| `vector-search` | `/api/2.0/mcp/vector-search/{catalog}/{schema}/{index}` | User wants to query a vector search index |
| `functions` | `/api/2.0/mcp/functions/{catalog}/{schema}` | User has pre-defined UC functions to call |

### Decision logic:

1. If the user says "I want to query data" or "browse tables" or gives no preference → use `sql`
2. If the user mentions a Genie space → use `genie`, ask for the space ID
3. If the user mentions vector search → use `vector-search`, ask for catalog/schema/index
4. If the user mentions UC functions → use `functions`, but **first verify functions exist:**

```bash
databricks functions list CATALOG SCHEMA --profile PROFILE_NAME
```

If the result is `[]` (empty), warn the user and fall back to `sql`.

**CRITICAL:** The `functions` endpoint exposes zero tools if no functions exist in the target schema. The MCP server will connect successfully but Claude will have nothing to call. This is the most common setup pitfall.

---

## Step 4: Create the `.mcp.json` Config File

### 4a. Determine the project root

The `.mcp.json` file goes in the root of the project directory where Claude Code is launched.

### 4b. Construct the full MCP URL

Build the URL by concatenating `WORKSPACE_URL` + the endpoint path:

```
WORKSPACE_URL/api/2.0/mcp/sql
```

For example: `https://adb-7405616827368959.19.azuredatabricks.net/api/2.0/mcp/sql`

**CRITICAL:** The `--url` flag requires the FULL URL including the workspace host. Do NOT use `--endpoint` with just the path — this will silently fail to connect.

### 4c. Write the `.mcp.json` file

Create `PROJECT_ROOT/.mcp.json` with this exact structure:

```json
{
  "mcpServers": {
    "databricks-sql": {
      "command": "uvx",
      "args": [
        "uc-mcp-proxy",
        "--url", "WORKSPACE_URL/api/2.0/mcp/sql",
        "--auth-type", "databricks-cli",
        "--profile", "PROFILE_NAME"
      ]
    }
  }
}
```

Replace `WORKSPACE_URL` and `PROFILE_NAME` with actual values.

**CRITICAL:** Always include `--auth-type`, `databricks-cli` as two separate args. Without it, the proxy may fail to authenticate even when the CLI profile specifies the auth type.

### 4d. For multiple endpoints, add more entries

Each endpoint gets its own key in `mcpServers`. Example with SQL + Genie:

```json
{
  "mcpServers": {
    "databricks-sql": {
      "command": "uvx",
      "args": [
        "uc-mcp-proxy",
        "--url", "WORKSPACE_URL/api/2.0/mcp/sql",
        "--auth-type", "databricks-cli",
        "--profile", "PROFILE_NAME"
      ]
    },
    "databricks-genie": {
      "command": "uvx",
      "args": [
        "uc-mcp-proxy",
        "--url", "WORKSPACE_URL/api/2.0/mcp/genie/GENIE_SPACE_ID",
        "--auth-type", "databricks-cli",
        "--profile", "PROFILE_NAME"
      ]
    }
  }
}
```

---

## Step 5: Verify the MCP Server Connects

```bash
claude mcp list
```

- **Expected:** `databricks-sql: uvx uc-mcp-proxy ... - ✓ Connected`
- **If `✗ Failed to connect`:**
  1. Check the URL is complete (includes workspace host)
  2. Check `--auth-type` is included
  3. Run `databricks auth login --profile PROFILE_NAME` to refresh the token
  4. Check that `uvx uc-mcp-proxy --help` runs without error (package is installable)

---

## Step 6: Restart Claude Code

**CRITICAL:** Claude Code loads MCP tools only at session startup. After creating or modifying `.mcp.json`, the user MUST restart Claude Code:

1. Tell the user: "The MCP config is ready. Please exit and restart Claude Code for the tools to load."
2. The user runs `/exit` and relaunches `claude` in the same project directory.

---

## Step 7: Verify Tools Are Available

After restart, search for the MCP tools:

```
ToolSearch query: "databricks sql"
```

For the SQL endpoint, three tools should appear:

| Tool Name | Purpose |
|---|---|
| `mcp__databricks-sql__execute_sql` | Run SQL queries (read + write) |
| `mcp__databricks-sql__execute_sql_read_only` | Run read-only SQL (SELECT, SHOW, DESCRIBE) |
| `mcp__databricks-sql__poll_sql_result` | Poll for async query results |

If tools don't appear:
1. Check `claude mcp list` shows `✓ Connected`
2. If the server name differs from `databricks-sql`, the tool prefix changes accordingly
3. Ensure the endpoint exposes tools (the `functions` endpoint won't if the schema is empty)

---

## Step 8: Test with a Query

Load the read-only tool first, then run a test query:

```
ToolSearch: "select:mcp__databricks-sql__execute_sql_read_only"
```

Then call:

```
mcp__databricks-sql__execute_sql_read_only(query="SHOW CATALOGS")
```

- **If status is `SUCCEEDED`:** Setup is complete.
- **If status is `PENDING`:** Load and call `poll_sql_result` with the returned `statement_id`.
- **If error:** Check auth, check that a SQL warehouse is available in the workspace.

### Useful discovery queries

```sql
SHOW CATALOGS
SHOW SCHEMAS IN catalog_name
SHOW TABLES IN catalog_name.schema_name
DESCRIBE TABLE catalog_name.schema_name.table_name
SELECT * FROM catalog_name.schema_name.table_name LIMIT 5
```

Always use fully qualified three-part names (`catalog.schema.table`). This is Databricks SQL (DBSQL) syntax.

---

## Troubleshooting Decision Tree

```
Problem: MCP server shows "✗ Failed to connect"
├── Is the URL complete (starts with https://)? 
│   └── NO → Fix: use --url with full URL, not --endpoint with path only
├── Is --auth-type included in args?
│   └── NO → Fix: add "--auth-type", "databricks-cli" to args array
├── Does `databricks auth token --profile PROFILE_NAME` return a token?
│   └── NO → Fix: run `databricks auth login --profile PROFILE_NAME`
└── Does `uvx uc-mcp-proxy --help` work?
    └── NO → Fix: install uv (`brew install uv`)

Problem: Server connected but no tools appear
├── Are you using the `functions` endpoint?
│   └── YES → Check: `databricks functions list CATALOG SCHEMA --profile PROFILE_NAME`
│       └── Empty result → Switch to the `sql` endpoint instead
├── Did you restart Claude Code after config change?
│   └── NO → Restart required. /exit and relaunch.
└── Is ToolSearch finding the tools?
    └── Search for the exact server name used in .mcp.json

Problem: Query returns PENDING but never SUCCEEDED
├── Is there an active SQL warehouse in the workspace?
│   └── Check Databricks UI → SQL Warehouses → must have at least one running/auto-start
└── Use poll_sql_result with the statement_id (may take 30-60s for warehouse cold start)
```

---

## Reference: Working Configuration for This Workspace

This is the exact config that is verified working:

**File:** `/Users/mikher/dev/mhtestdbxmcp/.mcp.json`

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

**File:** `~/.databrickscfg` (relevant profile)

```ini
[MHTEST]
host         = https://adb-7405616827368959.19.azuredatabricks.net
auth_type    = databricks-cli
account_id   = 06ddeeae-1d28-4371-91f3-a3c83e501698
workspace_id = 7405616827368959
```

**Verified tools available after setup:**
- `mcp__databricks-sql__execute_sql`
- `mcp__databricks-sql__execute_sql_read_only`
- `mcp__databricks-sql__poll_sql_result`
