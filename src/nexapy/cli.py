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
            except Exception as err:
                console.print(f"\n[bold red]Failed to load workflows from '{target}':[/bold red]\n{type(err).__name__}: {err}\n")
                raise typer.Exit(code=1)


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


@workflow_app.command("info")
def workflow_info(
    name: str = typer.Argument(..., help="Name of the workflow to inspect")
):
    """Show detailed metadata and info for a registered workflow."""
    import inspect
    from nexapy.automation import workflow_registry

    _import_local_workflows()

    wf = workflow_registry.get(name)
    if not wf:
        console.print(f"\n[bold red]Error:[/bold red] Workflow '{name}' is not registered.\n")
        raise typer.Exit(code=1)

    sig = inspect.signature(wf.func)
    is_async = inspect.iscoroutinefunction(wf.func)

    console.print(f"\n[bold cyan]Workflow Info: {wf.name}[/bold cyan]\n")
    table = Table(show_header=False, box=None)
    table.add_column("Property", style="bold magenta")
    table.add_column("Value", style="cyan")

    table.add_row("Name", wf.name)
    table.add_row("Description", wf.description or "N/A")
    table.add_row("Schedule", wf.schedule or "None (On-demand)")
    table.add_row("Execution Type", "Async (Coroutine)" if is_async else "Sync (Threaded)")
    table.add_row("Parameters", str(len(sig.parameters)))
    table.add_row("Function", f"{wf.func.__module__}.{wf.func.__qualname__}")

    console.print(table)
    console.print("")


# ---------------------------------------------------------------------------
# n8n Integration Commands
# ---------------------------------------------------------------------------

n8n_app = typer.Typer(name="n8n", help="Export NexaPy workflows for n8n automation orchestrator.")
app.add_typer(n8n_app, name="n8n")


@n8n_app.command("export")
def n8n_export(
    name: Optional[str] = typer.Argument(None, help="Name of workflow to export (optional if --all is specified)"),
    all_workflows: bool = typer.Option(False, "--all", "-A", help="Export all registered workflows"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output JSON file path (for single export)"),
    output_dir: Optional[str] = typer.Option(None, "--output-dir", "-d", help="Directory path to save exported JSON files"),
    host: str = typer.Option("http://localhost:8000", "--host", "-h", help="NexaPy API server host URL"),
    prefix: str = typer.Option("/workflows", "--prefix", "-p", help="Workflow HTTP endpoint prefix"),
    auth_key: Optional[str] = typer.Option(None, "--auth-key", "-k", help="Optional X-NexaPy-Key header value"),
):
    """Export NexaPy workflows into n8n node format JSON."""
    import json
    from nexapy.automation import workflow_registry
    from nexapy.integrations.n8n import export_all_n8n_workflows, export_n8n_workflow

    _import_local_workflows()

    if all_workflows:
        workflows = workflow_registry.list_all()
        if not workflows:
            console.print("\n[yellow]No workflows registered to export.[/yellow]\n")
            return

        out_directory = Path(output_dir or "./n8n_exports")
        out_directory.mkdir(parents=True, exist_ok=True)

        for wf in workflows:
            n8n_data = export_n8n_workflow(wf.name, host=host, prefix=prefix, auth_key=auth_key)
            file_path = out_directory / f"{wf.name}.json"
            file_path.write_text(json.dumps(n8n_data, indent=2), encoding="utf-8")
            console.print(f"Exported: [cyan]{file_path}[/cyan]")

        console.print(f"\n[bold green]Successfully exported {len(workflows)} n8n workflow(s) to '{out_directory}/'[/bold green]\n")
        return

    if not name:
        console.print("\n[bold red]Error:[/bold red] Specify a workflow name or pass '--all'.\n")
        raise typer.Exit(code=1)

    try:
        n8n_data = export_n8n_workflow(name, host=host, prefix=prefix, auth_key=auth_key)
    except ValueError as err:
        console.print(f"\n[bold red]Error:[/bold red] {err}\n")
        raise typer.Exit(code=1)

    json_str = json.dumps(n8n_data, indent=2)

    if output:
        out_path = Path(output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json_str, encoding="utf-8")
        console.print(f"\n[bold green]Successfully exported n8n workflow to '{output}'[/bold green]\n")
    else:
        console.print(json_str)


@n8n_app.command("node-generate")
def n8n_node_generate(
    output: str = typer.Option("n8n-nodes-nexapy", "--output", "-o", help="Output directory path for TypeScript package"),
    package_name: str = typer.Option("n8n-nodes-nexapy", "--package-name", "-n", help="NPM package name"),
    host: str = typer.Option("http://localhost:8000", "--host", "-h", help="Default NexaPy host URL"),
):
    """Generate a complete TypeScript n8n Native Community Node package for NexaPy."""
    from nexapy.integrations.node_generator import generate_n8n_community_node

    try:
        out_path = generate_n8n_community_node(output_dir=output, package_name=package_name, default_host=host)
        console.print(f"\n[bold green]Successfully generated n8n Community Node package in '{output}/'[/bold green]\n")
        console.print(f"Created package: [cyan]{out_path}/package.json[/cyan]")
        console.print(f"Created node:    [cyan]{out_path}/src/nodes/NexaPy/NexaPy.node.ts[/cyan]")
        console.print(f"Created creds:   [cyan]{out_path}/src/credentials/NexaPyApi.credentials.ts[/cyan]")
        console.print("")
    except Exception as err:
        console.print(f"\n[bold red]Error generating n8n node package:[/bold red] {err}\n")
        raise typer.Exit(code=1)


@n8n_app.command("validate")
def n8n_validate(
    file_path: str = typer.Argument(..., help="Path to n8n workflow JSON file"),
):
    """Validate structure and schema of an n8n JSON workflow file."""
    import json
    from nexapy.integrations.n8n import validate_n8n_workflow

    p = Path(file_path)
    if not p.exists():
        console.print(f"\n[bold red]Error:[/bold red] File '{file_path}' not found.\n")
        raise typer.Exit(code=1)

    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception as err:
        console.print(f"\n[bold red]Error parsing JSON:[/bold red] {err}\n")
        raise typer.Exit(code=1)

    valid = validate_n8n_workflow(data)
    if valid:
        console.print(f"\n[bold green]VALID:[/bold green] '{file_path}' is a valid n8n workflow JSON schema.\n")
    else:
        console.print(f"\n[bold red]INVALID:[/bold red] '{file_path}' failed n8n workflow JSON validation.\n")
        raise typer.Exit(code=1)


@n8n_app.command("import")
def n8n_import(
    file_path: str = typer.Argument(..., help="Path to n8n workflow JSON file to inspect"),
    codegen: Optional[str] = typer.Option(None, "--codegen", "-c", help="Output Python code file path (e.g. 'imported_workflow.py')"),
):
    """Import, inspect, and generate Python code from an n8n workflow JSON definition."""
    import json
    from nexapy.integrations.n8n import generate_python_code_from_n8n, import_n8n_workflow

    p = Path(file_path)
    if not p.exists():
        console.print(f"\n[bold red]Error:[/bold red] File '{file_path}' not found.\n")
        raise typer.Exit(code=1)

    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        info = import_n8n_workflow(data)
        console.print(f"\n[bold cyan]n8n Workflow Summary: {info['workflow_name']}[/bold cyan]\n")
        console.print(f"Total Nodes: {info['nodes_count']}")
        console.print(f"Target Endpoints: {len(info['endpoints'])}\n")
        for ep in info["endpoints"]:
            console.print(f" - Node: [magenta]{ep['node_name']}[/magenta] ({ep['type']}) -> {ep['url'] or ep['path']}")
        console.print("")

        if codegen:
            py_code = generate_python_code_from_n8n(data)
            out_p = Path(codegen)
            out_p.parent.mkdir(parents=True, exist_ok=True)
            out_p.write_text(py_code, encoding="utf-8")
            console.print(f"[bold green]Successfully generated Python workflow code in '{codegen}'[/bold green]\n")
    except Exception as err:
        console.print(f"\n[bold red]Import Error:[/bold red] {err}\n")
        raise typer.Exit(code=1)


@n8n_app.command("sync")
def n8n_sync(
    n8n_url: str = typer.Option(..., "--url", "-u", help="Target n8n instance URL (e.g. http://localhost:5678)"),
    api_key: str = typer.Option(..., "--api-key", "-k", help="n8n REST API key"),
    workflow_name: Optional[str] = typer.Argument(None, help="Name of workflow to sync (or all if omitted)"),
    host: str = typer.Option("http://localhost:8000", "--host", "-h", help="NexaPy API server host URL"),
    activate: bool = typer.Option(True, "--activate/--no-activate", help="Activate workflow on n8n after creation"),
):
    """Synchronize registered NexaPy workflows directly to a live n8n instance via REST API."""
    import asyncio
    from nexapy.automation import workflow_registry
    from nexapy.integrations.n8n import N8nClient, export_n8n_workflow

    _import_local_workflows()

    target_names = [workflow_name] if workflow_name else [w.name for w in workflow_registry.list_all()]
    if not target_names:
        console.print("\n[yellow]No workflows to sync.[/yellow]\n")
        return

    client = N8nClient(n8n_url=n8n_url, api_key=api_key)

    async def _sync():
        console.print(f"\n[bold cyan]Syncing NexaPy workflows to n8n instance at '{n8n_url}'...[/bold cyan]\n")
        for name in target_names:
            wf_data = export_n8n_workflow(name, host=host)
            res = await client.create_workflow(wf_data)
            wf_id = res.get("id") or res.get("data", {}).get("id")
            console.print(f"Created workflow: [green]{name}[/green] (ID: {wf_id})")
            if activate and wf_id:
                try:
                    await client.activate_workflow(wf_id)
                    console.print(f"  └─ Activated: [cyan]Active[/cyan]")
                except Exception as act_err:
                    console.print(f"  └─ Activation skipped: {act_err}")
        console.print("\n[bold green]Sync completed successfully![/bold green]\n")

    try:
        asyncio.run(_sync())
    except Exception as err:
        console.print(f"\n[bold red]Sync Error:[/bold red] {err}\n")
        raise typer.Exit(code=1)


@n8n_app.command("monitor")
def n8n_monitor(
    limit: int = typer.Option(20, "--limit", "-l", help="Number of recent executions to display"),
):
    """Display real-time dashboard of NexaPy workflow execution telemetry."""
    from nexapy.automation import tracker

    logs = tracker.storage.list_recent(limit=limit)

    console.print("\n[bold cyan]NexaPy Workflow Execution Telemetry Monitor[/bold cyan]\n")
    if not logs:
        console.print("[yellow]No recent workflow executions recorded in telemetry storage.[/yellow]\n")
        return

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Execution ID", style="cyan")
    table.add_column("Workflow", style="bold")
    table.add_column("Status", style="bold")
    table.add_column("Duration (ms)", justify="right")
    table.add_column("Error / Details", style="dim")

    for log in logs:
        status_style = "[green]SUCCESS[/green]" if log.status == "SUCCESS" else "[red]FAILED[/red]" if log.status == "FAILED" else "[yellow]RUNNING[/yellow]"
        table.add_row(
            log.execution_id,
            log.workflow_name,
            status_style,
            str(log.duration_ms or "N/A"),
            log.error or f"Steps: {len(log.steps)}",
        )

    console.print(table)
    console.print("")


sdk_app = typer.Typer(name="sdk", help="Generate multi-language client SDKs for NexaPy.")
app.add_typer(sdk_app, name="sdk")


@sdk_app.command("generate")
def sdk_generate(
    lang: str = typer.Option("typescript", "--lang", "-l", help="Language: javascript, typescript, react, php, kotlin, flutter, nodejs"),
    output: str = typer.Option("nexapy_sdk", "--output", "-o", help="Target output directory"),
    host: str = typer.Option("http://localhost:8000", "--host", "-h", help="NexaPy server base URL"),
    path: str = typer.Option("/ai/chat", "--path", "-p", help="Custom AI completion endpoint path"),
):
    """Generate client SDK for frontend or mobile integration."""
    from nexapy.sdk import generate_sdk

    try:
        out_path = generate_sdk(lang, output_dir=output, base_url=host, ai_path=path)
        console.print(f"[bold green]Successfully generated {lang} SDK in:[/bold green] {out_path}")
    except Exception as err:
        console.print(f"[bold red]SDK Generation Error:[/bold red] {err}")
        raise typer.Exit(code=1)



if __name__ == "__main__":
    app()



