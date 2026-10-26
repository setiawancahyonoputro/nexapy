import tempfile
from pathlib import Path
import pytest
from unittest.mock import AsyncMock, patch
from nexapy import workflow
from nexapy.automation import DAGWorkflow, workflow_registry
from nexapy.integrations.n8n import (
    N8nClient,
    export_all_n8n_workflows,
    export_dag_to_n8n,
    export_n8n_workflow,
    import_n8n_workflow,
    validate_n8n_workflow,
)
from nexapy.integrations.node_generator import generate_n8n_community_node


@pytest.fixture(autouse=True)
def reset_registry():
    workflow_registry.clear()
    yield
    workflow_registry.clear()


def test_export_n8n_webhook_workflow():
    @workflow("webhook-flow", description="Standard webhook workflow")
    async def webhook_flow(ctx):
        return "webhook"

    n8n_data = export_n8n_workflow("webhook-flow", host="http://localhost:8000", auth_key="secret-key")

    assert n8n_data["name"] == "NexaPy - webhook-flow"
    assert len(n8n_data["nodes"]) == 2

    trigger_node = n8n_data["nodes"][0]
    assert trigger_node["type"] == "n8n-nodes-base.webhook"
    assert trigger_node["name"] == "Webhook Trigger"
    assert trigger_node["parameters"]["path"] == "webhook-flow"

    http_node = n8n_data["nodes"][1]
    assert http_node["type"] == "n8n-nodes-base.httpRequest"
    assert http_node["parameters"]["url"] == "http://localhost:8000/workflows/webhook-flow"
    assert http_node["parameters"]["sendHeaders"] is True
    assert http_node["parameters"]["headerParameters"]["parameters"][0]["value"] == "secret-key"


def test_export_n8n_scheduled_workflow():
    @workflow("scheduled-report", description="Daily status report", schedule="0 8 * * *")
    def scheduled_report(ctx):
        return "report"

    n8n_data = export_n8n_workflow("scheduled-report", host="http://my-api.org:9000", prefix="/automation")

    assert n8n_data["name"] == "NexaPy - scheduled-report"
    trigger_node = n8n_data["nodes"][0]
    assert trigger_node["type"] == "n8n-nodes-base.scheduleTrigger"
    assert trigger_node["name"] == "Schedule Trigger"
    assert trigger_node["parameters"]["rule"]["cronExpression"] == "0 8 * * *"


def test_export_dag_to_n8n():
    dag = DAGWorkflow("pipeline_flow", description="Multi-step pipeline")

    @dag.node("step1")
    def step1():
        return "s1"

    @dag.node("step2", depends_on=["step1"])
    def step2():
        return "s2"

    workflow_registry.register("pipeline_flow", dag)

    n8n_data = export_n8n_workflow("pipeline_flow")
    assert n8n_data["name"] == "NexaPy DAG - pipeline_flow"
    assert len(n8n_data["nodes"]) == 3  # Trigger + step1 + step2
    assert "Webhook - pipeline_flow" in n8n_data["connections"]


def test_export_all_n8n_workflows():
    @workflow("flow1")
    def flow1():
        return 1

    @workflow("flow2")
    def flow2():
        return 2

    all_exported = export_all_n8n_workflows()
    assert len(all_exported) == 2
    names = [x["name"] for x in all_exported]
    assert "NexaPy - flow1" in names
    assert "NexaPy - flow2" in names


def test_validate_and_import_n8n_workflow():
    valid_json = {
        "name": "Test Workflow",
        "nodes": [
            {"name": "Webhook Trigger", "type": "n8n-nodes-base.webhook", "parameters": {"path": "test"}},
            {"name": "HTTP Request", "type": "n8n-nodes-base.httpRequest", "parameters": {"url": "http://localhost:8000/workflows/test"}},
        ],
        "connections": {"Webhook Trigger": {"main": [[{"node": "HTTP Request"}]]}},
    }

    assert validate_n8n_workflow(valid_json) is True
    assert validate_n8n_workflow({"invalid": "json"}) is False

    imported = import_n8n_workflow(valid_json)
    assert imported["workflow_name"] == "Test Workflow"
    assert imported["nodes_count"] == 2
    assert len(imported["endpoints"]) == 2


def test_generate_n8n_community_node():
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_path = generate_n8n_community_node(tmp_dir, package_name="n8n-nodes-test")
        assert (out_path / "package.json").exists()
        assert (out_path / "tsconfig.json").exists()
        assert (out_path / "src" / "nodes" / "NexaPy" / "NexaPy.node.ts").exists()
        assert (out_path / "src" / "credentials" / "NexaPyApi.credentials.ts").exists()


@pytest.mark.asyncio
async def test_n8n_client_api():
    client = N8nClient(n8n_url="http://n8n.test:5678", api_key="test-api-key")

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get, \
         patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:

        mock_get.return_value.json.return_value = {"data": [{"id": "1", "name": "wf1"}]}
        mock_get.return_value.raise_for_status = lambda: None

        mock_post.return_value.json.return_value = {"id": "123", "name": "NexaPy - wf"}
        mock_post.return_value.raise_for_status = lambda: None

        workflows = await client.list_workflows()
        assert len(workflows) == 1
        assert workflows[0]["id"] == "1"

        created = await client.create_workflow({"name": "NexaPy - wf"})
        assert created["id"] == "123"

        activated = await client.activate_workflow("123")
        assert activated["id"] == "123"
