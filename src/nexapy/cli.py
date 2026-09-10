import os
import sys
from pathlib import Path
from typing import Optional
import typer
import uvicorn
from rich.console import Console
from rich.table import Table
from nexapy import __version__

app = typer.Typer(
    name="nexapy",
    help="NexaPy Framework CLI - Fast, simple Python framework with AI integration.",
    add_completion=False,
)
console = Console()


def version_callback(value: bool):
    if value:
        typer.echo(f"NexaPy {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        "-v",
        help="Show NexaPy version.",
        callback=version_callback,
        is_eager=True,
    )
):
    pass


@app.command()
def init(
    name: str = typer.Argument(..., help="Name of the project directory to initialize")
):
    """Initialize a new NexaPy project directory with complete starter files."""
    target_dir = Path.cwd() / name
    if target_dir.exists():
        typer.echo(f"Error: Directory '{name}' already exists.")
        raise typer.Exit(code=1)

    target_dir.mkdir(parents=True, exist_ok=True)

    # 1. app.py
    app_file = target_dir / "app.py"
    starter_code = '''from nexapy import NexaPy

app = NexaPy(title="Hello NexaPy App")


@app.get("/")
def read_root():
    return {"message": "Hello from NexaPy!"}


@app.ai("/ai/chat")
async def chat(prompt: str):
    # Automatically routes prompt to FreeModel or Gemini cloud
    pass
'''
    app_file.write_text(starter_code, encoding="utf-8")

    # 2. framework.yaml
    yaml_code = f'''app:
  name: {name}

cors:
  enabled: true
  origins:
    - http://localhost:3000
    - http://localhost:5173

ai:
  provider: auto
  priority:
    - freemodel
    - gemini
  freemodel:
    model: auto
  gemini:
    model: gemini-3.8-flash
'''
    (target_dir / "framework.yaml").write_text(yaml_code, encoding="utf-8")

    # 3. .env.example (with placeholders) and .env (with empty keys)
    env_example = '''# NexaPy Environment Variables Example
FREEMODEL_API_KEY=your_freemodel_api_key_here
GEMINI_API_KEY=your_gemini_api_key_here
NEXAPY_AI_PROVIDER=auto
NEXAPY_CORS_ENABLED=true
'''
    env_actual = '''# NexaPy Environment Variables
FREEMODEL_API_KEY=
GEMINI_API_KEY=
NEXAPY_AI_PROVIDER=auto
NEXAPY_CORS_ENABLED=true
'''
    (target_dir / ".env.example").write_text(env_example, encoding="utf-8")
    (target_dir / ".env").write_text(env_actual, encoding="utf-8")

    # 4. .gitignore
    gitignore_code = '''.env
.venv/
env/
venv/
__pycache__/
*.pyc
.pytest_cache/
.coverage
build/
dist/
*.egg-info/
'''
    (target_dir / ".gitignore").write_text(gitignore_code, encoding="utf-8")

    typer.echo(f"Initialized NexaPy project in '{name}/'")
    typer.echo(f"Created file: {name}/app.py")
    typer.echo(f"Created file: {name}/framework.yaml")
    typer.echo(f"Created file: {name}/.env")
    typer.echo(f"Created file: {name}/.env.example")
    typer.echo(f"Created file: {name}/.gitignore")
    typer.echo(f"\nNext steps:\n  cd {name}\n  nexapy dev")


@app.command()
def dev(
    app_import: str = typer.Option("app:app", "--app", "-a", help="ASGI app import string"),
    host: str = typer.Option("127.0.0.1", "--host", "-h", help="Host address"),
    port: int = typer.Option(8000, "--port", "-p", help="Port number"),
    reload: bool = typer.Option(True, "--reload/--no-reload", help="Enable live auto-reload"),
):
    """Run NexaPy development server."""
    display_host = "localhost" if host in ("127.0.0.1", "0.0.0.0") else host

    typer.echo("")
    typer.echo("NexaPy Server Running")
    typer.echo("")
    typer.echo(f"API  : http://{display_host}:{port}")
    typer.echo(f"Docs : http://{display_host}:{port}/docs")
    typer.echo("")

    uvicorn.run(app_import, host=host, port=port, reload=reload)


def _is_valid_key(key: Optional[str]) -> bool:
    if not key or not key.strip():
        return False
    k = key.strip().lower()
    return not (k.startswith("your_") or "placeholder" in k or "key_here" in k)


@app.command()
def doctor():
    """Diagnose local environment, dependencies, and AI provider status."""
    console.print("\n[bold cyan]NexaPy Environment Doctor[/bold cyan]\n")

    table = Table(title="Diagnostic Checks", show_header=True, header_style="bold magenta")
    table.add_column("Component", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Details", style="dim")

    # 1. Python Version
    py_ver = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    py_status = "[green]OK[/green]" if sys.version_info >= (3, 11) else "[red]FAIL[/red]"
    table.add_row("Python Version", py_status, f"Python {py_ver} (Minimum >= 3.11)")

    # 2. Installed Dependencies
    deps = ["fastapi", "uvicorn", "typer", "rich", "pydantic", "httpx", "yaml"]
    missing = []
    for dep in deps:
        try:
            __import__(dep)
        except ImportError:
            missing.append(dep)
    dep_status = "[green]OK[/green]" if not missing else "[red]FAIL[/red]"
    dep_detail = "All required packages installed" if not missing else f"Missing: {', '.join(missing)}"
    table.add_row("Dependencies", dep_status, dep_detail)

    # 3. Environment Config Files
    has_env = Path(".env").exists()
    has_yaml = Path("framework.yaml").exists() or Path("framework.yml").exists()
    cfg_status = "[green]OK[/green]" if (has_env or has_yaml) else "[yellow]WARNING[/yellow]"
    cfg_detail = f".env: {'Found' if has_env else 'Missing'} | framework.yaml: {'Found' if has_yaml else 'Missing'}"
    table.add_row("Config Files", cfg_status, cfg_detail)

    # 4. FreeModel Provider Key
    fm_key = os.getenv("FREEMODEL_API_KEY")
    fm_valid = _is_valid_key(fm_key)
    fm_status = "[green]CONFIGURED[/green]" if fm_valid else "[yellow]NOT SET[/yellow]"
    fm_detail = "FREEMODEL_API_KEY is present" if fm_valid else "Set FREEMODEL_API_KEY in .env"
    table.add_row("FreeModel Provider", fm_status, fm_detail)

    # 5. Gemini Provider Key
    gemini_key = os.getenv("GEMINI_API_KEY")
    gemini_valid = _is_valid_key(gemini_key)
    gemini_status = "[green]CONFIGURED[/green]" if gemini_valid else "[yellow]NOT SET[/yellow]"
    gemini_detail = "GEMINI_API_KEY is present" if gemini_valid else "Set GEMINI_API_KEY in .env"
    table.add_row("Gemini Provider", gemini_status, gemini_detail)

    console.print(table)
    console.print("\n[dim]Run 'nexapy dev' to start your NexaPy application server.[/dim]\n")


# ---------------------------------------------------------------------------
# SDK Generator Commands
# ---------------------------------------------------------------------------

sdk_app = typer.Typer(name="sdk", help="Generate NexaPy Client SDKs for web applications.")
app.add_typer(sdk_app, name="sdk")


@sdk_app.command("generate")
def sdk_generate(
    lang: str = typer.Option("javascript", "--lang", "-l", help="SDK Language ('javascript'/'js', 'typescript'/'ts', 'react')"),
    output: str = typer.Option("nexapy_sdk", "--output", "-o", help="Output directory path"),
    base_url: str = typer.Option("http://localhost:8000", "--base-url", "-b", help="Default API base URL"),
    ai_path: str = typer.Option("/ai/chat", "--ai-path", "-p", help="Target AI route path (default: '/ai/chat')"),
):
    """Generate JavaScript, TypeScript, or React Hook client SDK for NexaPy backend."""
    from nexapy.sdk import generate_sdk

    out_path = Path(output)
    try:
        generate_sdk(lang=lang, output_dir=out_path, base_url=base_url, ai_path=ai_path)
        console.print(f"\n[bold green]Successfully generated {lang} SDK in '{output}/'[/bold green]\n")
        l_clean = lang.lower()
        if l_clean in ("javascript", "js"):
            console.print(f"Created file: [cyan]{output}/client.js[/cyan]")
        elif l_clean in ("typescript", "ts"):
            console.print(f"Created file: [cyan]{output}/client.ts[/cyan]")
            console.print(f"Created file: [cyan]{output}/types.ts[/cyan]")
            console.print(f"Created file: [cyan]{output}/index.ts[/cyan]")
        elif l_clean in ("react", "react-hooks"):
            console.print(f"Created file: [cyan]{output}/client.ts[/cyan]")
            console.print(f"Created file: [cyan]{output}/types.ts[/cyan]")
            console.print(f"Created file: [cyan]{output}/index.ts[/cyan]")
            console.print(f"Created file: [cyan]{output}/react/useNexaPy.ts[/cyan]")
            console.print(f"Created file: [cyan]{output}/react/index.ts[/cyan]")
        console.print("")
    except ValueError as err:
        console.print(f"[bold red]Error:[/bold red] {err}")
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# Workflow Commands (v0.3 Automation Core)
# ---------------------------------------------------------------------------

workflow_app = typer.Typer(name="workflow", help="Manage and execute NexaPy automation workflows.")
app.add_typer(workflow_app, name="workflow")


def _import_local_workflows():
    """Helper to auto-import app.py or local workflow files if present."""
    sys.path.insert(0, str(Path.cwd()))
    for target in ["app.py", "workflow.py", "workflows.py", "main.py"]:
        p = Path.cwd() / target
        if p.exists():
            mod_name = p.stem
            try:
                __import__(mod_name)
            except Exception:
                pass


@workflow_app.command("list")
def workflow_list():
    """List all registered automation workflows."""
    from nexapy.automation import workflow_registry

    _import_local_workflows()

    workflows = workflow_registry.list_all()

    if not workflows:
        console.print("\n[yellow]No workflows registered.[/yellow]\n")
        console.print("[dim]Register a workflow using @workflow('name') in your python application.[/dim]\n")
        return

    console.print("\n[bold cyan]Available Workflows[/bold cyan]\n")
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Workflow Name", style="cyan")
    table.add_column("Description", style="dim")

    for wf in workflows:
        table.add_row(wf.name, wf.description or "")

    console.print(table)
    console.print("")


@workflow_app.command("run")
def workflow_run(
    name: str = typer.Argument(..., help="Name of the workflow to run"),
    input_str: Optional[str] = typer.Option(None, "--input", "-i", help="Input data as JSON string or file path (e.g. '{\"key\":\"val\"}' or @data.json)"),
):
    """Execute a registered automation workflow by name."""
    import json
    from nexapy.automation import workflow_registry, runner

    _import_local_workflows()

    wf = workflow_registry.get(name)
    if not wf:
        console.print(f"\n[bold red]Error:[/bold red] Workflow '{name}' is not registered.\n")
        workflows = workflow_registry.list_all()
        if workflows:
            names = ", ".join([f"'{w.name}'" for w in workflows])
            console.print(f"Available workflows: {names}\n")
        raise typer.Exit(code=1)

    input_data = {}
    if input_str:
        s = input_str.strip()
        if s.startswith("@") or s.endswith(".json"):
            file_path = Path(s.lstrip("@"))
            if not file_path.exists():
                console.print(f"\n[bold red]Error:[/bold red] Input file '{file_path}' not found.\n")
                raise typer.Exit(code=1)
            try:
                input_data = json.loads(file_path.read_text(encoding="utf-8"))
            except Exception as err:
                console.print(f"\n[bold red]Error:[/bold red] Failed to parse JSON from file '{file_path}': {err}\n")
                raise typer.Exit(code=1)
        else:
            try:
                input_data = json.loads(s)
            except Exception as err:
                console.print(f"\n[bold red]Error:[/bold red] Invalid JSON string provided for --input: {err}\n")
                raise typer.Exit(code=1)

    try:
        result = runner.run(wf, input_data=input_data)
        console.print(f"\n[bold green]Workflow '{name}' executed successfully:[/bold green]\n")
        if isinstance(result, (dict, list)):
            console.print_json(json.dumps(result))
        else:
            console.print(result)
        console.print("")
    except Exception as err:
        console.print(f"\n[bold red]Workflow Execution Error:[/bold red] {err}\n")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()

