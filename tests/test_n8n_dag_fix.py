import json
import tempfile
from pathlib import Path
import pytest
from unittest.mock import AsyncMock, patch
from nexapy.automation import DAGWorkflow, workflow_registry
from nexapy.integrations.n8n import (
    N8nClient,
    export_dag_to_n8n,
    generate_python_code_from_n8n,
    import_n8n_workflow,
)
from nexapy.integrations.node_generator import generate_n8n_community_node


@pytest.fixture(autouse=True)
def clean_registry():
    workflow_registry.clear()
    yield
    workflow_registry.clear()


def test_export_dag_exact_dependencies_and_step_url():
    """Verify DAG export preserves exact node dependencies and sets ?step= query on HTTP nodes."""
    dag = DAGWorkflow("order_dag", description="Order DAG Pipeline")

    @dag.node("stepA")
    def step_a():
        return "A"

    @dag.node("stepB", depends_on=["stepA"])
    def step_b():
        return "B"

    @dag.node("stepC", depends_on=["stepA"])
    def step_c():
        return "C"

    @dag.node("stepD", depends_on=["stepB", "stepC"])
    def step_d():
        return "D"

    n8n_data = export_dag_to_n8n(dag, host="http://localhost:8000")
    nodes = {node["name"]: node for node in n8n_data["nodes"]}
    connections = n8n_data["connections"]

    # 1. Verify ?step= query in URL
    assert "url" in nodes["Step: stepA"]["parameters"]
    assert "step=stepA" in nodes["Step: stepA"]["parameters"]["url"]
    assert "step=stepD" in nodes["Step: stepD"]["parameters"]["url"]

    # 2. Verify trigger only connects to stepA (not stepB or stepC or stepD)
    trigger_targets = [item["node"] for item in connections["Webhook - order_dag"]["main"][0]]
    assert trigger_targets == ["Step: stepA"]

    # 3. Verify stepA connects to stepB and stepC
    step_a_targets = [item["node"] for item in connections["Step: stepA"]["main"][0]]
    assert set(step_a_targets) == {"Step: stepB", "Step: stepC"}

    # 4. Verify stepB and stepC connect to stepD (and not to each other)
    step_b_targets = [item["node"] for item in connections["Step: stepB"]["main"][0]]
    assert step_b_targets == ["Step: stepD"]
    step_c_targets = [item["node"] for item in connections["Step: stepC"]["main"][0]]
    assert step_c_targets == ["Step: stepD"]


def test_n8n_code_generator_no_undefined_info():
    """Verify generated Python code from n8n JSON defines metadata inside function scope without NameError."""
    sample_n8n = {
        "name": "Sync Customer CRM",
        "nodes": [
            {"name": "Trigger", "type": "n8n-nodes-base.webhook", "parameters": {"path": "cust_in"}},
            {"name": "Call NexaPy", "type": "n8n-nodes-base.httpRequest", "parameters": {"url": "http://api.local/workflows/crm"}},
        ],
        "connections": {"Trigger": {"main": [[{"node": "Call NexaPy"}]]}},
    }

    py_code = generate_python_code_from_n8n(sample_n8n)
    assert "workflow_info = {" in py_code
    assert '"n8n_import": workflow_info' in py_code

    # Verify the code compiles and can be executed dynamically without NameError: name 'info' is not defined
    local_env = {}
    exec(py_code, local_env)
    assert "sync_customer_crm" in local_env
    func = local_env["sync_customer_crm"]
    assert callable(func)


def test_n8n_community_node_complete_build_files():
    """Verify generated Community Node package includes gulpfile, svg icon, and devDependencies."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_path = generate_n8n_community_node(tmp_dir, package_name="n8n-nodes-nexapy")

        # Files exist
        assert (out_path / "package.json").exists()
        assert (out_path / "gulpfile.js").exists()
        assert (out_path / "src" / "nodes" / "NexaPy" / "nexapy.svg").exists()
        assert (out_path / "src" / "nodes" / "NexaPy" / "NexaPy.node.ts").exists()

        pkg = json.loads((out_path / "package.json").read_text(encoding="utf-8"))
        assert "gulp" in pkg["devDependencies"]
        assert pkg["scripts"]["build"] == "tsc && gulp copy:icons"

        node_ts = (out_path / "src" / "nodes" / "NexaPy" / "NexaPy.node.ts").read_text(encoding="utf-8")
        assert "this.helpers.httpRequest" in node_ts
        assert "chatCompletion" in node_ts
        assert "executeWorkflow" in node_ts
        assert "getExecution" in node_ts
        assert "listWorkflows" in node_ts


@pytest.mark.asyncio
async def test_n8n_sync_upsert_logic():
    """Verify N8nClient.sync_workflow updates existing workflow if name matches, or creates new."""
    client = N8nClient(n8n_url="http://n8n.test:5678", api_key="secret")

    with patch.object(client, "list_workflows", new_callable=AsyncMock) as mock_list, \
         patch.object(client, "update_workflow", new_callable=AsyncMock) as mock_update, \
         patch.object(client, "create_workflow", new_callable=AsyncMock) as mock_create:

        # Scenario 1: Workflow exists -> update
        mock_list.return_value = [{"id": "wf_existing_1", "name": "NexaPy - Flow 1"}]
        mock_update.return_value = {"id": "wf_existing_1", "status": "updated"}

        res_update = await client.sync_workflow({"name": "NexaPy - Flow 1", "nodes": []}, upsert=True)
        assert res_update["id"] == "wf_existing_1"
        mock_update.assert_called_once()
        mock_create.assert_not_called()

        # Scenario 2: Workflow does not exist -> create
        mock_update.reset_mock()
        mock_create.reset_mock()
        mock_create.return_value = {"id": "wf_new_2", "status": "created"}

        res_create = await client.sync_workflow({"name": "NexaPy - New Flow", "nodes": []}, upsert=True)
        assert res_create["id"] == "wf_new_2"
        mock_create.assert_called_once()
        mock_update.assert_not_called()
