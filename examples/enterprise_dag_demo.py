"""
Enterprise DAG Workflow & n8n Integration Demo for NexaPy Framework.

Demonstrates:
1. Multi-Step DAG Workflow definition with step dependencies & parallel steps
2. AI Structured Output parsing with Pydantic schema validation (with fallback)
3. JSON Data Transformation and Webhook Notification actions
4. Telemetry logging & execution persistence
5. Exporting DAG Workflows to n8n Visual JSON
6. Generating TypeScript n8n Native Community Node Package
"""

import asyncio
import os
from pydantic import BaseModel, Field
from nexapy import NexaPy
from nexapy.automation import DAGWorkflow, WorkflowContext, tracker
from nexapy.integrations import export_dag_to_n8n, generate_n8n_community_node


# 1. Define Pydantic Schema for Structured AI Ticket Extraction
class TicketAnalysis(BaseModel):
    category: str = Field(description="Category: billing, tech_support, sales, or general")
    priority: str = Field(description="Priority: low, medium, high, or urgent")
    summary: str = Field(description="One sentence summary of the issue")
    sentiment: str = Field(description="Sentiment: positive, neutral, or frustrated")


# 2. Construct Multi-Step DAG Workflow
dag_app = DAGWorkflow("customer_support_pipeline", description="Enterprise AI ticket triage DAG workflow")


@dag_app.node("ingest", description="Receive and validate raw ticket payload")
async def ingest_step(ctx: WorkflowContext):
    ticket_id = ctx.get("ticket_id", "T-1001")
    user_text = ctx.get("message", "Payment failed during checkout with code ERR_502")
    return {"ticket_id": ticket_id, "text": user_text, "status": "ingested"}


@dag_app.node("analyze_ticket", depends_on=["ingest"], max_retries=1)
async def analyze_step(ctx: WorkflowContext):
    ingest_data = ctx.state["steps"]["ingest"]
    prompt = f"Analyze customer support ticket: {ingest_data['text']}"

    # Try live AI structured extraction if keys are set, otherwise demo fallback
    if os.getenv("FREEMODEL_API_KEY") or os.getenv("GEMINI_API_KEY"):
        try:
            analysis: TicketAnalysis = await ctx.ai_structured(prompt, schema=TicketAnalysis)
            return analysis.model_dump()
        except Exception:
            pass

    # Demo fallback analysis
    analysis = TicketAnalysis(
        category="tech_support",
        priority="high" if "URGENT" in ingest_data["text"] else "medium",
        summary=f"Automated summary for: {ingest_data['text'][:40]}...",
        sentiment="frustrated",
    )
    return analysis.model_dump()


@dag_app.node("enrich_user_profile", depends_on=["ingest"])
async def enrich_step(ctx: WorkflowContext):
    # Parallel step executing concurrently with analyze_ticket!
    ticket_id = ctx.state["steps"]["ingest"]["ticket_id"]
    return {"user_tier": "VIP_Enterprise", "region": "US-East", "account_id": "ACC-9988"}


@dag_app.node("route_and_dispatch", depends_on=["analyze_ticket", "enrich_user_profile"])
async def route_step(ctx: WorkflowContext):
    analysis = ctx.state["steps"]["analyze_ticket"]
    profile = ctx.state["steps"]["enrich_user_profile"]

    # Transform payload for n8n/Slack notification using Transform action
    notification_payload = ctx.transform.pick(analysis, ["category", "priority", "summary"])
    notification_payload["user_tier"] = profile["user_tier"]
    notification_payload["action"] = "ESCALATED" if analysis["priority"] in ("high", "urgent") else "QUEUED"

    return notification_payload


async def main():
    print("==================================================================")
    print(" NexaPy Enterprise DAG Workflow & n8n Platform Demo")
    print("==================================================================\n")

    # A. Execute DAG Workflow locally via Runner
    print("1. Running Multi-Step DAG Workflow locally...")
    from nexapy.automation import runner

    input_payload = {
        "ticket_id": "T-2045",
        "message": "URGENT: Database connection timeout in production cluster!",
    }

    results = await runner.run_async(dag_app, input_data=input_payload)
    print("\n[OK] DAG Execution Completed Successfully!")
    print("Step Results:")
    for step_name, val in results.items():
        print(f"  -> [{step_name}]: {val}")

    # B. Telemetry Check
    logs = tracker.storage.list_recent(limit=1)
    if logs:
        last_log = logs[0]
        print(f"\n2. Telemetry Tracker Execution Log:")
        print(f"  Execution ID : {last_log.execution_id}")
        print(f"  Duration     : {last_log.duration_ms} ms")
        print(f"  Status       : {last_log.status}")

    # C. Export DAG to n8n Visual Workflow JSON
    print("\n3. Exporting DAG Workflow to n8n Visual Workflow JSON...")
    n8n_dag_json = export_dag_to_n8n(dag_app, host="http://my-api-server:8000", auth_key="secret-key-123")
    print(f"  Exported n8n Nodes ({len(n8n_dag_json['nodes'])} nodes):")
    for node in n8n_dag_json["nodes"]:
        print(f"   - {node['name']} ({node['type']})")

    # D. Generate n8n TypeScript Community Node Package
    print("\n4. Generating TypeScript n8n Native Community Node Package...")
    node_dir = generate_n8n_community_node("./demo_n8n_node_package", package_name="n8n-nodes-nexapy-demo")
    print(f"  [OK] Generated Community Node in '{node_dir}'")
    print("\n==================================================================")


if __name__ == "__main__":
    asyncio.run(main())
