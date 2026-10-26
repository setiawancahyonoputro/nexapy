# NexaPy Framework

[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Build Status](https://img.shields.io/badge/tests-120%20passed-brightgreen.svg)](#)
[![Version](https://img.shields.io/badge/version-0.3.0.dev0-orange.svg)](#)

**NexaPy** is a modern Python API Framework, Multi-Provider AI Gateway, and Workflow Automation Engine built on top of **FastAPI**. It is designed to bridge backend business logic, multi-model AI routing, asynchronous workflow orchestration, and cross-platform clients (React, Node.js, PHP, Kotlin, Flutter).

---

## 🏛️ System Architecture

```
                      ┌──────────────────────────────────────────────┐
                      │          Frontend / Backend Client           │
                      │  React · Node.js · PHP · Kotlin · Flutter    │
                      └──────────────────────┬───────────────────────┘
                                             │ HTTPS / Client SDKs
                                             ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                   NexaPy Framework                                     │
│                API Gateway · Auth (API Key/HMAC) · Rate Limiting · Validation          │
│                                                                                        │
│     ┌────────────────────────────────────┐    ┌──────────────────────────────────┐     │
│     │             AI Router              │    │         Workflow Engine          │     │
│     │ OpenAI · Gemini · Claude · DeepSeek│    │  DAG · Async Queue · Retry State │     │
│     │ Streaming SSE · Circuit Breaker    │    │  Scheduler · Step Execution      │     │
│     └────────────────────────────────────┘    └──────────────────────────────────┘     │
│                                                                                        │
│                      Job Storage + Observability & Data Masking                        │
└────────────────────────────────────┬───────────────────────────────────────────────────┘
                                     │ Webhooks / Events / Callbacks
                                     ▼
                      ┌──────────────────────────────────────────────┐
                      │                n8n Automation                │
                      │   Triggers · Scheduling · Approvals · Sync   │
                      └──────┬──────────────────┬─────────────────┬──┘
                             │                  │                 │
                             ▼                  ▼                 ▼
                      ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
                      │ Database/CRM │   │  Email/Chat  │   │ External Apps│
                      └──────────────┘   └──────────────┘   └──────────────┘
```
---

## 🚦 Feature Maturity Matrix

| Component | Status | Details |
| :--- | :--- | :--- |
| **FastAPI Core & ASGI Runtime** | 🟢 **Stable** | Production-ready HTTP/ASGI engine, OpenAPI docs, and lifespan management. |
| **Fail-Closed Security & HMAC** | 🟢 **Stable** | Mandatory authentication fail-closed, timestamp + nonce replay defense, rate limiting, SSRF guard. |
| **AI Router & Fallback Chain** | 🟢 **Stable** | Multi-provider fallback (OpenAI, Gemini, Claude, DeepSeek, FreeModel), circuit breaker state machine, SSE streaming. |
| **Workflow Engine & DAG** | 🟢 **Stable** | Topological parallel DAG execution, single-step execution with `run_id` state store. |
| **Job Queue & Recovery** | 🟢 **Stable** | Persistent SQLite job queue with server restart recovery, worker pool, idempotency key, and `/jobs` API. |
| **Cron & Interval Scheduler** | 🟢 **Stable** | In-process 5-field cron matcher (`minute hour day month day_of_week`) and interval parser. |
| **Node.js & TypeScript SDKs** | 🟢 **Stable** | Dual-module (CommonJS + ESM) Node.js package (`@nexapy/client`) and TypeScript fetch client. |
| **n8n DAG Export & Merge Nodes** | 🟡 **Experimental** | Visual DAG export with `n8n-nodes-base.merge` synchronization, PUT API upsert, and Credentials binding. |
| **n8n Community Node Generator**| 🟡 **Experimental** | Auto-generates TypeScript package with real HTTP requests, HMAC SHA-256 signing, and Gulp build tooling. |
| **PHP, Kotlin & Flutter SDKs** | 🟡 **Experimental** | PSR-18/cURL PHP client, Kotlin OkHttp client, and Dart Flutter packages. |
| **Multi-Tenancy & Tenant Quotas** | 🔵 **Planned** | Per-tenant token budget caps, dynamic key management, and billing dashboards. |
| **OpenTelemetry OTLP Exporter** | 🔵 **Planned** | Native OTLP span forwarding to Jaeger, Datadog, and Honeycomb. |

---

## 🚀 Key Framework Capabilities

### 1. Multi-Provider AI Gateway
- **Direct Providers**: Native adapters for **OpenAI** (GPT-4o), **Google Gemini** (Interactions API), **Anthropic** (Claude 3.5), **DeepSeek**, and **FreeModel**.
- **Dynamic Plugin Registry**: Register custom AI providers or internal enterprise models with `ai.register_provider("custom", CustomProvider())`.
- **Intelligent Fallback & Circuit Breaker**: Automatic failover based on provider priority list and health status with transient error retries.
- **End-to-End Streaming**: Server-Sent Events (SSE) streaming via `/ai/stream` and SDK `stream()` methods.

### 2. Workflow Engine & DAG Orchestration
- **DAG Pipelines**: Define multi-step Directed Acyclic Graphs with parallel branch execution and retry rules.
- **Single-Step Execution**: Execute individual DAG steps remotely (`?step=stepName`), preventing accidental whole-DAG re-runs.
- **Persistent Job Queue**: Asynchronous background queue (`JobQueue`) with worker pool and SQLite/memory state persistence.
- **Automated Scheduler**: Local interval & cron-based scheduling (`WorkflowScheduler`) for automated triggers.
- **Safe-by-Default Security**: Built-in API Key guards, HMAC SHA-256 webhook signatures, client rate limiting, and centralized payload size limits.
- **Telemetry & Data Redaction**: Execution logs with automatic sensitive field masking (`password`, `token`, `secret`, `api_key`, `authorization`).

### 3. Native n8n Workflow Orchestration
- **External Orchestrator Alignment**: Seamlessly connects NexaPy workflows to n8n triggers, business approvals, and scheduled triggers.
- **True Dependency Visual Export**: Exports NexaPy DAGs into visual n8n workflows preserving exact topological dependencies and step URLs.
- **Automated Workflow Upsert**: Synchronizes workflows with live n8n instances using idempotent update-or-create logic.
- **Production Community Node Generator**: Generates full TypeScript packages with real HTTP requests, gulp asset builders, SVG icon, and credentials.
- **n8n-to-Python Importer**: Converts n8n workflow definitions into valid, runnable NexaPy Python code.

### 4. Cross-Platform Client SDKs
Developers on frontend or external backends do not need Python installed. Generate typed client SDKs:
- **Node.js**: Standalone client package with TypeScript definitions and CommonJS/ESM exports.
- **PHP**: Modern PHP 8.1+ PSR-18 and cURL client SDK.
- **Kotlin**: Coroutine-friendly OkHttp client with typed serialization.
- **Flutter / Dart**: Native Dart SDK with model classes and async workflow triggers.
- **TypeScript & JavaScript**: Universal Fetch-based client with custom headers and error handling.
- **React**: Custom `useNexaPy` hook for reactive AI and workflow state management.

---

## 📦 Installation

```bash
pip install nexapy
```

For development and testing:
```bash
pip install -e ".[dev]"
```

---

## ⚡ Quick Start

### 1. Initialize a Project
```bash
nexapy init myapp
cd myapp
```

### 2. Configure Environment (`.env` or `framework.yaml`)
```env
OPENAI_API_KEY=your_openai_key
GEMINI_API_KEY=your_gemini_key
ANTHROPIC_API_KEY=your_anthropic_key
DEEPSEEK_API_KEY=your_deepseek_key
NEXAPY_API_KEY=your_secret_workflow_key
```

### 3. Application Definition (`app.py`)
```python
from nexapy import NexaPy, AI, DAGWorkflow, workflow

app = NexaPy(title="NexaPy Enterprise App")

# 1. AI Route
@app.ai("/ai/chat")
async def chat(prompt: str):
    pass  # Automatically routed with fallback

# 2. Standard Workflow
@workflow("sync_crm", description="Synchronize customer data")
async def sync_crm(ctx):
    user_id = ctx.input.get("id")
    return {"status": "synced", "user_id": user_id}

# 3. DAG Workflow
pipeline = DAGWorkflow("ml_pipeline")

@pipeline.node("extract")
def extract_data():
    return {"raw": [1, 2, 3]}

@pipeline.node("transform", depends_on=["extract"])
def transform_data(ctx):
    raw = ctx.steps.get("extract", {}).get("raw", [])
    return {"processed": [x * 10 for x in raw]}

# Mount workflows with API key authentication and rate limiting
app.mount_workflows(prefix="/workflows", api_key="my-secret-key")
```

### 4. Run Development Server
```bash
nexapy dev
```
Open **http://localhost:8000/docs** to view the interactive OpenAPI documentation.

---

## 🛠️ Multi-Platform Client Usage

### Node.js
```javascript
const { NexaPyClient } = require("@nexapy/client");
const client = new NexaPyClient({ baseURL: "http://localhost:8000", apiKey: "my-secret-key" });

const response = await client.chat("Summarize today's logs", { provider: "auto" });
console.log(response.text);

const result = await client.executeWorkflow("sync_crm", { id: 101 });
```

### PHP
```php
use NexaPy\NexaPyClient;

$client = new NexaPyClient(baseUrl: 'http://localhost:8000', apiKey: 'my-secret-key');
$res = $client->chat('Generate invoice description');
echo $res['text'];

$wf = $client->executeWorkflow('sync_crm', ['id' => 101]);
```

### Flutter / Dart
```dart
import 'package:nexapy_client/nexapy_client.dart';

final client = NexaPyClient(baseUrl: 'http://localhost:8000', apiKey: 'my-secret-key');
final res = await client.chat('Hello AI');
print(res.text);
```

### Kotlin
```kotlin
val client = NexaPyClient(baseUrl = "http://localhost:8000", apiKey = "my-secret-key")
val res = client.chat("Hello from Android/Kotlin", provider = "auto")
println(res.text)
```

---

## ⚙️ CLI Reference

```bash
nexapy --version                        # View version
nexapy doctor                           # Inspect environment and provider keys
nexapy dev                              # Start live server with hot reload
nexapy workflow list                    # List registered workflows
nexapy n8n export <workflow_name>       # Export workflow as n8n visual JSON
nexapy n8n sync <workflow_name>         # Upsert workflow directly to n8n instance
nexapy n8n generate-node                # Generate n8n Community Node package
nexapy sdk generate --lang php -o sdk   # Generate client SDK (php, kotlin, flutter, nodejs, ts)
```

---

## 🧪 Testing & Code Quality

NexaPy includes an automated test suite covering unit tests, multi-provider routing, HMAC byte verification, DAG dependencies, sensitive data masking, and multi-language SDK generators:

```bash
pytest
```
Result: **102 passed** across unit, security, integration, and concurrency test suites.

---

## 📄 License

Distributed under the **MIT License**.
