"""n8n Workflow Exporter, Importer, Schema Validator, and Live REST Sync Integration."""

import inspect
import json
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Union
import httpx
from nexapy.automation import DAGWorkflow, workflow_registry


def export_n8n_workflow(
    workflow_name: str,
    host: str = "http://localhost:8000",
    prefix: str = "/workflows",
    auth_key: Optional[str] = None,
    response_mode: str = "onReceived",
    use_credentials: bool = False,
    credential_name: Optional[str] = None,
    credential_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generate an n8n workflow JSON configuration for a registered NexaPy workflow.
    Uses n8n credentials binding (httpHeaderAuth) as the single source of truth for secrets,
    preventing plaintext API key leakage.
    """
    wf = workflow_registry.get(workflow_name)
    if not wf:
        raise ValueError(f"Workflow '{workflow_name}' is not registered.")

    if isinstance(wf, DAGWorkflow):
        return export_dag_to_n8n(
            wf,
            host=host,
            prefix=prefix,
            auth_key=auth_key,
            use_credentials=use_credentials,
            credential_name=credential_name,
            credential_id=credential_id,
        )

    base_host = host.rstrip("/")
    route_prefix = prefix.strip("/")
    endpoint_url = f"{base_host}/{route_prefix}/{workflow_name}"

    nodes = []

    if getattr(wf, "schedule", None):
        trigger_node = {
            "parameters": {
                "rule": {
                    "mode": "custom",
                    "cronExpression": wf.schedule,
                }
            },
            "name": "Schedule Trigger",
            "type": "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.2,
            "position": [250, 300],
        }
        trigger_name = "Schedule Trigger"
    else:
        trigger_params: Dict[str, Any] = {
            "httpMethod": "POST",
            "path": workflow_name,
            "options": {"responseMode": response_mode},
        }
        trigger_node = {
            "parameters": trigger_params,
            "name": "Webhook Trigger",
            "type": "n8n-nodes-base.webhook",
            "typeVersion": 1,
            "position": [250, 300],
        }
        trigger_name = "Webhook Trigger"

    nodes.append(trigger_node)

    cred_id = credential_id or f"cred_{workflow_name.lower().replace('-', '_')}"
    http_params: Dict[str, Any] = {
        "method": "POST",
        "url": endpoint_url,
        "sendBody": True,
        "specifyBody": "json",
        "jsonBody": "={{ $json.body || $json }}",
    }

    # P0 Finding 6 & 7: Use credentials as single source of secrets; no plaintext headerParameters
    if auth_key or use_credentials or credential_name:
        http_params["authentication"] = "genericCredentialType"
        http_params["genericAuthType"] = "httpHeaderAuth"

    http_node_name = f"NexaPy {workflow_name}"
    http_node: Dict[str, Any] = {
        "parameters": http_params,
        "name": http_node_name,
        "type": "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position": [500, 300],
    }

    if auth_key or use_credentials or credential_name:
        http_node["credentials"] = {
            "httpHeaderAuth": {
                "id": cred_id,
                "name": credential_name or "NexaPy Header Auth",
            },
            "nexapyApi": {
                "id": cred_id,
                "name": credential_name or "NexaPy API",
            },
        }

    nodes.append(http_node)

    connections = {
        trigger_name: {
            "main": [
                [
                    {
                        "node": http_node_name,
                        "type": "main",
                        "index": 0,
                    }
                ]
            ]
        }
    }

    return {
        "name": f"NexaPy - {wf.name}",
        "nodes": nodes,
        "connections": connections,
        "active": False,
        "settings": {"executionOrder": "v1"},
        "tags": [{"name": "NexaPy"}],
    }


def export_dag_to_n8n(
    dag: DAGWorkflow,
    host: str = "http://localhost:8000",
    prefix: str = "/workflows",
    auth_key: Optional[str] = None,
    use_credentials: bool = False,
    credential_name: Optional[str] = None,
    credential_id: Optional[str] = None,
    use_merge_nodes: bool = False,
) -> Dict[str, Any]:
    """
    Export a NexaPy DAG Workflow into a multi-node n8n Visual Workflow JSON.
    Each HTTP request node executes only its specific step rather than the full DAG,
    and node connections preserve exact DAG node dependencies.
    Propagates run_id across steps to correlate multi-step execution.
    When use_merge_nodes is True, steps with multiple parent dependencies are synchronized
    via an n8n Merge node before executing the dependent step.
    """
    base_host = host.rstrip("/")
    route_prefix = prefix.strip("/")
    endpoint_url = f"{base_host}/{route_prefix}/{dag.name}"

    trigger_name = f"Webhook - {dag.name}"
    nodes = [
        {
            "parameters": {"httpMethod": "POST", "path": dag.name, "options": {}},
            "name": trigger_name,
            "type": "n8n-nodes-base.webhook",
            "typeVersion": 1,
            "position": [250, 300],
        }
    ]

    connections: Dict[str, Any] = {trigger_name: {"main": [[]]}}

    levels = dag._topological_levels() if hasattr(dag, "_topological_levels") else [[node for node in dag.nodes.values()]]

    x_pos = 500
    cred_id = credential_id or f"cred_{dag.name.lower().replace('-', '_')}"
    for level in levels:
        y_pos = 200
        for dag_node in level:
            node_name = f"Step: {dag_node.name}"
            step_endpoint_url = f"{endpoint_url}?step={dag_node.name}"
            http_params: Dict[str, Any] = {
                "method": "POST",
                "url": step_endpoint_url,
                "sendBody": True,
                "specifyBody": "json",
                # P1 Finding 17: propagate run_id to correlate cross-step outputs
                "jsonBody": f'={{"step": "{dag_node.name}", "run_id": $json.run_id || $execution.id, "payload": $json}}',
            }

            if auth_key or use_credentials or credential_name:
                http_params["authentication"] = "genericCredentialType"
                http_params["genericAuthType"] = "httpHeaderAuth"

            step_node: Dict[str, Any] = {
                "parameters": http_params,
                "name": node_name,
                "type": "n8n-nodes-base.httpRequest",
                "typeVersion": 4.2,
                "position": [x_pos, y_pos],
            }

            if auth_key or use_credentials or credential_name:
                step_node["credentials"] = {
                    "httpHeaderAuth": {
                        "id": cred_id,
                        "name": credential_name or "NexaPy Header Auth",
                    },
                    "nexapyApi": {
                        "id": cred_id,
                        "name": credential_name or "NexaPy API",
                    },
                }

            nodes.append(step_node)

            # Preserve dependencies:
            if not dag_node.depends_on:
                connections[trigger_name]["main"][0].append({"node": node_name, "type": "main", "index": 0})
            elif len(dag_node.depends_on) == 1 or not use_merge_nodes:
                for dep in dag_node.depends_on:
                    dep_node_name = f"Step: {dep}"
                    if dep_node_name not in connections:
                        connections[dep_node_name] = {"main": [[]]}
                    connections[dep_node_name]["main"][0].append({"node": node_name, "type": "main", "index": 0})
            else:
                # Multi-dependency: insert n8n Merge node to synchronize multiple branches
                merge_node_name = f"Merge for {dag_node.name}"
                nodes.append({
                    "parameters": {
                        "mode": "combine",
                        "combinationMode": "mergeByPosition",
                    },
                    "name": merge_node_name,
                    "type": "n8n-nodes-base.merge",
                    "typeVersion": 2.1,
                    "position": [x_pos - 100, y_pos],
                })
                for idx, dep in enumerate(dag_node.depends_on):
                    dep_node_name = f"Step: {dep}"
                    if dep_node_name not in connections:
                        connections[dep_node_name] = {"main": [[]]}
                    connections[dep_node_name]["main"][0].append({
                        "node": merge_node_name,
                        "type": "main",
                        "index": idx if idx < 2 else 1,
                    })
                connections[merge_node_name] = {"main": [[{"node": node_name, "type": "main", "index": 0}]]}

            y_pos += 150
        x_pos += 250

    return {
        "name": f"NexaPy DAG - {dag.name}",
        "nodes": nodes,
        "connections": connections,
        "active": False,
        "settings": {"executionOrder": "v1"},
        "tags": [{"name": "NexaPy-DAG"}],
    }


def export_all_n8n_workflows(
    host: str = "http://localhost:8000",
    prefix: str = "/workflows",
    auth_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Export all registered NexaPy workflows to a list of n8n JSON workflow objects."""
    workflows = workflow_registry.list_all()
    results = []
    for wf in workflows:
        data = export_n8n_workflow(wf.name, host=host, prefix=prefix, auth_key=auth_key)
        results.append(data)
    return results


def validate_n8n_workflow(n8n_data: Dict[str, Any]) -> bool:
    """
    Validate structure of an n8n workflow JSON object.
    Checks for required keys: name, nodes, connections, and node types.
    """
    if not isinstance(n8n_data, dict):
        return False
    required_keys = ["name", "nodes", "connections"]
    if not all(k in n8n_data for k in required_keys):
        return False
    if not isinstance(n8n_data["nodes"], list) or not isinstance(n8n_data["connections"], dict):
        return False
    for node in n8n_data["nodes"]:
        if not isinstance(node, dict) or "name" not in node or "type" not in node:
            return False
    return True


def import_n8n_workflow(n8n_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse an n8n workflow JSON and extract metadata, triggers, HTTP nodes, and supported actions.
    Returns Python workflow spec definition.
    """
    if not validate_n8n_workflow(n8n_data):
        raise ValueError("Invalid n8n workflow JSON structure.")

    wf_name = n8n_data.get("name", "imported_n8n_workflow")
    extracted_endpoints = []

    for node in n8n_data.get("nodes", []):
        node_type = node.get("type", "")
        params = node.get("parameters", {})
        url = params.get("url", "")
        path = params.get("path", "")
        method = params.get("httpMethod") or params.get("method", "GET")

        extracted_endpoints.append({
            "node_name": node.get("name"),
            "type": node_type,
            "url": url,
            "path": path,
            "method": method,
            "parameters": params,
        })

    return {
        "workflow_name": wf_name,
        "nodes_count": len(n8n_data.get("nodes", [])),
        "endpoints": extracted_endpoints,
        "raw": n8n_data,
    }


def generate_python_code_from_n8n(n8n_data: Dict[str, Any]) -> str:
    """
    Generate executable Python NexaPy workflow code from an imported n8n workflow JSON definition.
    Safe against JSON null/boolean literals, quotes, newlines, and invalid identifiers.
    Uses repr() for dynamic strings and sanitizes function identifiers (P0 Finding 8).
    """
    import pprint
    import re

    info = import_n8n_workflow(n8n_data)
    raw_name = info["workflow_name"]

    # Sanitize function name: must be a valid Python identifier
    sanitized_id = re.sub(r"\W+", "_", str(raw_name).lower()).strip("_")
    if not sanitized_id or sanitized_id[0].isdigit():
        sanitized_id = f"wf_{sanitized_id}"
    clean_func_name = sanitized_id

    meta_formatted = pprint.pformat(info, indent=4)
    indented_meta = "\n".join("    " + line for line in meta_formatted.splitlines())

    clean_comment_name = str(raw_name).replace('"""', "'''").replace("\r", " ").replace("\n", " ")
    desc_repr = repr(f"Imported from n8n: {raw_name}")
    wf_name_repr = repr(clean_func_name)

    lines = [
        '"""',
        'Auto-generated NexaPy Workflow imported from n8n JSON.',
        f'Original Workflow Name: {clean_comment_name}',
        '"""',
        '',
        'from nexapy import workflow, WorkflowContext',
        '',
        f'@workflow({wf_name_repr}, description={desc_repr})',
        f'async def {clean_func_name}(ctx: WorkflowContext):',
        '    input_data = ctx.input',
        '    results = {}',
        f'    workflow_info = {indented_meta.strip()}',
        '',
    ]

    for ep in info["endpoints"]:
        node_name = ep["node_name"] or "unnamed_node"
        node_type = ep["type"]
        safe_comment_step = str(node_name).replace("\r", " ").replace("\n", " ")
        lines.append(f'    # Step: {safe_comment_step} ({node_type})')
        node_name_repr = repr(node_name)
        if ep["url"]:
            url_repr = repr(ep["url"])
            method_repr = repr(ep["method"])
            lines.append(f'    results[{node_name_repr}] = {{"target_url": {url_repr}, "method": {method_repr}}}')
        elif ep["path"]:
            path_repr = repr(ep["path"])
            lines.append(f'    results[{node_name_repr}] = {{"webhook_path": {path_repr}}}')
        else:
            type_repr = repr(node_type)
            lines.append(f'    results[{node_name_repr}] = {{"type": {type_repr}}}')
        lines.append('')

    lines.append('    return {"status": "success", "n8n_import": workflow_info, "step_results": results}')
    lines.append('')

    return "\n".join(lines)


class N8nClient:
    """Client for direct integration with n8n instance REST API (v1) with upsert sync support."""

    def __init__(
        self,
        n8n_url: str,
        api_key: str,
        timeout: float = 15.0,
        cache_file: Optional[Union[str, Path]] = None,
    ):
        self.n8n_url = n8n_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.headers = {
            "X-N8N-API-KEY": api_key,
            "Content-Type": "application/json",
        }
        self.cache_file = Path(cache_file) if cache_file else None
        self._id_cache: Dict[str, str] = {}
        if self.cache_file and self.cache_file.exists():
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    self._id_cache = json.load(f)
            except Exception:
                self._id_cache = {}

    def _save_cache(self) -> None:
        if self.cache_file:
            try:
                self.cache_file.parent.mkdir(parents=True, exist_ok=True)
                with open(self.cache_file, "w", encoding="utf-8") as f:
                    json.dump(self._id_cache, f, indent=2)
            except Exception:
                pass

    async def list_workflows(self, max_pages: int = 10) -> List[Dict[str, Any]]:
        """Fetch list of workflows from live n8n instance supporting cursor pagination (P1 Finding 21)."""
        all_workflows: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        page = 0
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            while page < max_pages:
                url = f"{self.n8n_url}/api/v1/workflows"
                params: Dict[str, Any] = {"limit": 100}
                if cursor:
                    params["cursor"] = cursor
                res = await client.get(url, params=params, headers=self.headers)
                res.raise_for_status()
                data = res.json()
                if inspect.iscoroutine(data):
                    data = await data
                if isinstance(data, dict):
                    items = data.get("data", [])
                    all_workflows.extend(items)
                    cursor = data.get("nextCursor")
                    if not cursor or not items:
                        break
                elif isinstance(data, list):
                    all_workflows.extend(data)
                    break
                else:
                    break
                page += 1
        return all_workflows

    async def create_workflow(self, n8n_workflow_json: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new workflow on live n8n instance."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            res = await client.post(f"{self.n8n_url}/api/v1/workflows", json=n8n_workflow_json, headers=self.headers)
            res.raise_for_status()
            data = res.json()
            if inspect.iscoroutine(data):
                data = await data
            if isinstance(data, dict) and "id" in data:
                self._id_cache[n8n_workflow_json.get("name", "")] = str(data["id"])
                self._save_cache()
            return data if isinstance(data, dict) else {}

    async def update_workflow(self, workflow_id: str, n8n_workflow_json: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing workflow on live n8n instance using PUT per n8n Public API specification."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            res = await client.put(
                f"{self.n8n_url}/api/v1/workflows/{workflow_id}",
                json=n8n_workflow_json,
                headers=self.headers,
            )
            res.raise_for_status()
            data = res.json()
            if inspect.iscoroutine(data):
                data = await data
            if isinstance(data, dict):
                self._id_cache[n8n_workflow_json.get("name", "")] = str(workflow_id)
                self._save_cache()
            return data if isinstance(data, dict) else {}

    async def sync_workflow(self, n8n_workflow_json: Dict[str, Any], upsert: bool = True) -> Dict[str, Any]:
        """
        Synchronize workflow to n8n instance.
        If upsert is True, matches existing workflow by cached ID or by name and updates it.
        If workflow lookup fails with an error, the operation aborts to prevent creating accidental duplicates.
        """
        target_name = n8n_workflow_json.get("name")
        if upsert:
            # 1. Check persistent ID cache first
            if target_name and target_name in self._id_cache:
                return await self.update_workflow(self._id_cache[target_name], n8n_workflow_json)

            # 2. Lookup existing workflows via n8n API
            existing = await self.list_workflows()
            match = next((w for w in existing if w.get("name") == target_name), None)
            if match and match.get("id"):
                wf_id = str(match["id"])
                if target_name:
                    self._id_cache[target_name] = wf_id
                    self._save_cache()
                return await self.update_workflow(wf_id, n8n_workflow_json)

        return await self.create_workflow(n8n_workflow_json)

    async def activate_workflow(self, workflow_id: str) -> Dict[str, Any]:
        """Activate a workflow on live n8n instance."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            res = await client.post(f"{self.n8n_url}/api/v1/workflows/{workflow_id}/activate", headers=self.headers)
            res.raise_for_status()
            data = res.json()
            if inspect.iscoroutine(data):
                data = await data
            return data if isinstance(data, dict) else {}

