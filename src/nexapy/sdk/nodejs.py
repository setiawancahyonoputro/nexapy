"""NexaPy Node.js Standalone Client SDK / npm Package Generator."""

import json
from pathlib import Path
from nexapy import __version__


def generate_nodejs_sdk(
    output_dir: Path,
    base_url: str = "http://localhost:8000",
    ai_path: str = "/ai/chat",
    package_name: str = "@nexapy/client",
) -> None:
    """Generate a complete, publishable Node.js / npm client package."""
    output_dir.mkdir(parents=True, exist_ok=True)
    clean_base_url = base_url.rstrip("/")
    clean_ai_path = "/" + ai_path.lstrip("/")

    # 1. package.json
    pkg_data = {
        "name": package_name,
        "version": __version__,
        "description": "Official Node.js / JavaScript Client SDK for NexaPy AI & Workflow Automation Framework",
        "main": "index.js",
        "module": "index.mjs",
        "types": "index.d.ts",
        "exports": {
            ".": {
                "import": "./index.mjs",
                "require": "./index.js",
                "types": "./index.d.ts",
            }
        },
        "keywords": ["nexapy", "fastapi", "ai", "gemini", "openai", "workflow", "automation"],
        "license": "MIT",
        "repository": {
            "type": "git",
            "url": "https://github.com/setiawancahyonoputro/nexapy.git",
        },
        "scripts": {
            "test": "node test.js",
        },
    }
    (output_dir / "package.json").write_text(json.dumps(pkg_data, indent=2), encoding="utf-8")

    # 2. index.js (CommonJS)
    index_js = f"""/**
 * NexaPy Node.js Client SDK (v{__version__})
 * Universal client for AI Router, Workflows, Telemetry, and Custom API endpoints.
 */

class NexaPyClient {{
  constructor(options = {{}}) {{
    this.baseURL = (options.baseURL || "{clean_base_url}").replace(/\\/+$/, "");
    this.apiKey = options.apiKey || process.env.NEXAPY_API_KEY || null;
    this.timeout = options.timeout !== undefined ? options.timeout : 30000;
    this.aiPath = options.aiPath || "{clean_ai_path}";
  }}

  _getHeaders(customHeaders = {{}}) {{
    const headers = {{
      "Content-Type": "application/json",
      "Accept": "application/json",
      ...customHeaders,
    }};
    if (this.apiKey) {{
      headers["X-NexaPy-Key"] = this.apiKey;
    }}
    return headers;
  }}

  async _fetchWithTimeout(url, options = {{}}) {{
    const timeout = options.timeout || this.timeout;
    const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
    const timer = controller && timeout > 0 ? setTimeout(() => controller.abort(), timeout) : null;

    try {{
      const response = await fetch(url, {{
        ...options,
        signal: controller ? controller.signal : undefined,
      }});
      return response;
    }} finally {{
      if (timer) clearTimeout(timer);
    }}
  }}

  /**
   * Send prompt to NexaPy AI Router.
   */
  async chat(prompt, options = {{}}) {{
    if (!prompt || typeof prompt !== "string" || !prompt.trim()) {{
      throw new Error("NexaPyClient: prompt parameter is required and cannot be empty.");
    }}

    const payload = {{ prompt: prompt.trim() }};
    if (options.provider) payload.provider = options.provider;
    if (options.reasoning) payload.reasoning = options.reasoning;
    if (options.temperature !== undefined) payload.temperature = options.temperature;
    if (options.maxTokens !== undefined) payload.max_tokens = options.maxTokens;

    const res = await this._fetchWithTimeout(`${{this.baseURL}}${{this.aiPath}}`, {{
      method: "POST",
      headers: this._getHeaders(),
      body: JSON.stringify(payload),
      timeout: options.timeout,
    }});

    return await res.json();
  }}

  /**
   * Execute registered automation workflow.
   */
  async executeWorkflow(workflowName, input = {{}}, step = null) {{
    const query = step ? `?step=${{encodeURIComponent(step)}}` : "";
    const res = await this._fetchWithTimeout(`${{this.baseURL}}/workflows/${{encodeURIComponent(workflowName)}}${{query}}`, {{
      method: "POST",
      headers: this._getHeaders(),
      body: JSON.stringify(input || {{}}),
    }});
    return await res.json();
  }}

  /**
   * Retrieve execution telemetry log.
   */
  async getExecution(executionId) {{
    const res = await this._fetchWithTimeout(`${{this.baseURL}}/workflows/executions/${{encodeURIComponent(executionId)}}`, {{
      method: "GET",
      headers: this._getHeaders(),
    }});
    return await res.json();
  }}

  /**
   * Discover registered workflow schemas.
   */
  async listWorkflows() {{
    const res = await this._fetchWithTimeout(`${{this.baseURL}}/workflows/_schema`, {{
      method: "GET",
      headers: this._getHeaders(),
    }});
    return await res.json();
  }}

  /**
   * Check NexaPy server health.
   */
  async health() {{
    const res = await this._fetchWithTimeout(`${{this.baseURL}}/health`, {{
      method: "GET",
    }});
    return await res.json();
  }}

  /**
   * Generic request helper for custom CRUD API endpoints.
   */
  async request(method, path, body = null, headers = {{}}) {{
    const cleanPath = "/" + path.replace(/^\\/+/, "");
    const options = {{
      method: method.toUpperCase(),
      headers: this._getHeaders(headers),
    }};
    if (body && ["POST", "PUT", "PATCH"].includes(options.method)) {{
      options.body = JSON.stringify(body);
    }}
    const res = await this._fetchWithTimeout(`${{this.baseURL}}${{cleanPath}}`, options);
    return await res.json();
  }}
}}

module.exports = {{ NexaPyClient }};
module.exports.default = NexaPyClient;
"""
    (output_dir / "index.js").write_text(index_js, encoding="utf-8")

    # 3. index.mjs (ECMAScript Modules)
    index_mjs = """/**
 * NexaPy Node.js Client SDK (ESM Module wrapper)
 */
import { createRequire } from "module";
const require = createRequire(import.meta.url);
const { NexaPyClient } = require("./index.js");

export { NexaPyClient };
export default NexaPyClient;
"""
    (output_dir / "index.mjs").write_text(index_mjs, encoding="utf-8")

    # 4. index.d.ts
    index_d_ts = f"""export interface AIResponse {{
  text: string;
  provider: string;
  model: string;
  success: boolean;
  error?: string | null;
}}

export interface AIChatOptions {{
  provider?: "auto" | "openai" | "gemini" | "anthropic" | "deepseek" | "freemodel" | string;
  reasoning?: "low" | "medium" | "high";
  temperature?: number;
  maxTokens?: number;
  timeout?: number;
}}

export interface NexaPyClientOptions {{
  baseURL?: string;
  apiKey?: string;
  timeout?: number;
  aiPath?: string;
}}

export class NexaPyClient {{
  constructor(options?: NexaPyClientOptions);
  chat(prompt: string, options?: AIChatOptions): Promise<AIResponse>;
  executeWorkflow(workflowName: string, input?: Record<string, any>, step?: string | null): Promise<any>;
  getExecution(executionId: string): Promise<any>;
  listWorkflows(): Promise<any>;
  health(): Promise<any>;
  request(method: string, path: string, body?: any, headers?: Record<string, string>): Promise<any>;
}}

export default NexaPyClient;
"""
    (output_dir / "index.d.ts").write_text(index_d_ts, encoding="utf-8")

    # 4. README.md
    readme_md = f"""# {package_name} (v{__version__})

Official Node.js / JavaScript Client for **NexaPy** AI Framework.

## Installation

```bash
npm install {package_name}
```

## Quick Start

```javascript
const {{ NexaPyClient }} = require("{package_name}");

const client = new NexaPyClient({{
  baseURL: "http://localhost:8000",
  apiKey: process.env.NEXAPY_API_KEY,
}});

// 1. AI Completion
const response = await client.chat("Explain microservices architecture", {{
  provider: "auto",
}});
console.log(response.text);

// 2. Execute Automation Workflow
const result = await client.executeWorkflow("order_processing", {{ orderId: 456 }});
console.log(result);
```
"""
    (output_dir / "README.md").write_text(readme_md, encoding="utf-8")
