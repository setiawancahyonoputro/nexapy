# NexaPy Framework

[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**NexaPy** is a modern, high-performance Python framework built on top of **FastAPI** with seamless multi-provider AI routing (**FreeModel** & **Google Gemini**), standardized AI responses, and developer-friendly CLI tooling.

---

## Key Features

- ⚡ **FastAPI Core**: ASGI-native speed, automatic Swagger OpenAPI docs (`/docs`), and clean routing.
- 🤖 **Smart Priority AI Router**: Auto-routes completion prompts with fallback chain (e.g., **FreeModel** $\rightarrow$ **Gemini**).
- 📦 **Normalized `AIResponse`**: Unified response schema across all AI providers.
- 🎯 **Simple AI API**: Use high-level `AI()` client or `@app.ai("/chat")` decorator.
- 🌐 **Automatic CORS**: Built-in `CORSMiddleware` configured via `framework.yaml` or `.env`.
- 🩺 **CLI Health Doctor**: Run `nexapy doctor` to diagnose environment and AI provider configuration status.

---

## Installation

Install NexaPy from source in editable mode for local development:

```bash
pip install -e .
```

To install development dependencies (testing and build tools):

```bash
pip install -e ".[dev]"
```

---

## Quick Start

### 1. Check CLI Version & Environment
```bash
nexapy --version
nexapy doctor
```

### 2. Scaffold a New Project
```bash
nexapy init myapp
cd myapp
```

This generates:
```text
myapp/
├── app.py
├── framework.yaml
├── .env
├── .env.example
└── .gitignore
```

### 3. Starter Application (`app.py`)
```python
from nexapy import NexaPy, AI

app = NexaPy(title="My NexaPy App")

# 1. Standard FastAPI GET route
@app.get("/")
def home():
    return {"message": "Welcome to NexaPy!"}

# 2. NexaPy AI Route Decorator
@app.ai("/chat")
async def chat(prompt: str):
    # Automatically routes prompt to FreeModel or Gemini
    pass

# 3. Direct AI Usage
@app.get("/ask")
async def ask_ai(prompt: str):
    ai = AI()
    response = await ai.chat(prompt)
    return response.dict_response()
```

### 4. Run Development Server
```bash
nexapy dev
```

Server output:
```text
NexaPy Server Running

API  : http://localhost:8000
Docs : http://localhost:8000/docs
```

---

## Configuration

NexaPy supports configuration through `framework.yaml` and `.env` files.

### `framework.yaml`
```yaml
app:
  name: NexaPy Starter Application

cors:
  enabled: true
  origins:
    - http://localhost:3000
    - http://localhost:5173

ai:
  provider: auto      # Options: auto, freemodel, gemini
  priority:
    - freemodel
    - gemini
  freemodel:
    model: auto
  gemini:
    model: gemini-3.8-flash
```

### `.env`
```env
FREEMODEL_API_KEY=your_freemodel_api_key_here
GEMINI_API_KEY=your_gemini_api_key_here
NEXAPY_APP_NAME="My NexaPy App"
NEXAPY_AI_PROVIDER=auto
NEXAPY_CORS_ENABLED=true
```

---

## Supported AI Providers (v0.1)

| Provider | Base URL / Model | Authentication |
| :--- | :--- | :--- |
| **FreeModel** | `https://api.freemodel.dev/v1` (`model=auto`) | `FREEMODEL_API_KEY` (Bearer Header) |
| **Google Gemini** | `gemini-3.8-flash` | `GEMINI_API_KEY` (`x-goog-api-key` Header) |
| **Auto Router** | Priority Fallback Chain | Configurable Priority |

---

## Normalized `AIResponse` Schema

Every AI call in NexaPy returns a consistent JSON API structure (with `raw` excluded from API responses by default):

```json
{
  "text": "Hello! How can I assist you today?",
  "provider": "freemodel",
  "model": "auto",
  "success": true,
  "error": null
}
```

For internal Python debugging, the `raw` provider response payload remains fully accessible on the Python `AIResponse` object:

```python
response = await ai.chat("Hello")

print(response.text)     # "Hello! How can I assist you today?"
print(response.provider) # "freemodel"
print(response.raw)      # {"choices": [...], "usage": {...}}
```

---

## CLI Reference

- `nexapy --version`: Display current version.
- `nexapy init <name>`: Create project directory with starter code and configuration.
- `nexapy dev`: Launch live-reloading uvicorn development server.
- `nexapy doctor`: Diagnostic tool checking Python, dependencies, `.env`, and AI provider readiness.

---

## Project Roadmap

- [x] **v0.1 MVP**: CLI, FastAPI Core, Config YAML/.env, Priority AI Router (FreeModel + Gemini), Normalized AIResponse, CORS, `@app.ai()`, `nexapy doctor`.
- [ ] **v0.2**: React & JavaScript/TypeScript SDKs.
- [ ] **v0.3**: Workflow Engine & n8n Integration.

---

## License

This project is licensed under the [MIT License](LICENSE).
