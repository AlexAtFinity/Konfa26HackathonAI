# Setting Up Python with uv

This guide walks you through installing Python using [uv](https://docs.astral.sh/uv/) — a fast, modern tool that handles both Python installation and package management in one place. No prior Python experience needed.

---

## What is uv?

`uv` replaces the traditional combo of downloading Python + using `pip` + creating virtual environments. It does all three in a single tool, and it's much faster.

---

## Step 1 — Install uv

### Mac / Linux

Open **Terminal** and paste:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Or if you have Homebrew:

```bash
brew install uv
```

### Windows

Open **PowerShell** (search for "PowerShell" in the Start menu) and paste:

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Or if you have winget:

```powershell
winget install astral-sh.uv
```

### After installing

**Close and reopen your terminal**, then verify it worked:

```bash
uv --version
```

You should see a version number. If you get "command not found", try closing and reopening your terminal once more.

---

## Step 2 — Install Python

With uv installed, getting Python is a single command:

```bash
uv python install 3.12
```

That's it. No downloading from websites, no installers, no PATH headaches. Python **3.12** is a solid default for the hackathon.

Verify:

```bash
uv run python --version
```

You should see something like `Python 3.12.x`.

---

## Step 3 — Create a project

Navigate to where you want your project and run:

```bash
mkdir my-project
cd my-project
uv init --python 3.12
```

This creates:
- A `pyproject.toml` file (your project config)
- A `.venv` folder (your isolated Python environment)

### Add packages

```bash
uv add requests pandas
```

This installs the packages and records them in `pyproject.toml` so anyone else can reproduce your setup.

### Run your code

Prefix any command with `uv run` and it automatically uses your project's environment:

```bash
uv run python my_script.py
```

No need to "activate" anything. `uv run` handles it.

---

## Quick reference

| Task | Command |
|---|---|
| Install Python | `uv python install 3.12` |
| Start a new project | `uv init --python 3.12` |
| Add a package | `uv add <package-name>` |
| Remove a package | `uv remove <package-name>` |
| Run a script | `uv run python script.py` |
| Run Jupyter | `uv run jupyter lab` |
| See installed packages | `uv pip list` |

---

## Optional: activate the virtual environment manually

If you prefer the traditional workflow or a tool requires it:

**Mac / Linux:**

```bash
source .venv/bin/activate
```

**Windows PowerShell:**

```powershell
.venv\Scripts\Activate.ps1
```

When activated, you can run `python` and `pip` directly without the `uv run` prefix. Type `deactivate` to leave the environment.

---

## Prefer classic Python without uv?

If you'd rather install Python the traditional way:

1. Download Python 3.12 from [python.org](https://www.python.org/downloads/).
2. Run the installer. **On Windows, check "Add Python to PATH"** during installation.
3. Then set up a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate          # Mac / Linux
.venv\Scripts\Activate.ps1         # Windows PowerShell
pip install --upgrade pip
pip install <your-packages>
```

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `uv: command not found` | Close and reopen your terminal. If still not found, check that `~/.cargo/bin` (Mac/Linux) or `%USERPROFILE%\.cargo\bin` (Windows) is in your PATH. |
| `uv python install` hangs | Check your internet connection. uv downloads Python from the web. |
| Permission errors on Mac | Don't use `sudo`. If you get permission errors, reinstall uv with the curl command above. |
| VS Code doesn't see the venv | In VS Code, press `Cmd+Shift+P` (Mac) or `Ctrl+Shift+P` (Windows), type "Python: Select Interpreter", and pick the `.venv` interpreter in your project folder. |
