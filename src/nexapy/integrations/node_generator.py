"""Generator for n8n Native Community Node (TypeScript Package)."""

import json
from pathlib import Path
from typing import Union


def generate_n8n_community_node(
    output_dir: Union[str, Path],
    package_name: str = "n8n-nodes-nexapy",
    default_host: str = "http://localhost:8000",
) -> Path:
    """
    Generate a complete, publishable TypeScript n8n Community Node package for NexaPy.
    Includes real HTTP request dispatching, build tooling (Gulp + TypeScript), and credentials.
    """
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # 1. package.json
    pkg_data = {
        "name": package_name,
        "version": "0.1.0",
        "description": "Native n8n Community Node integration for NexaPy AI Framework.",
        "keywords": ["n8n-community-node-package", "nexapy", "ai", "fastapi", "automation"],
        "license": "MIT",
        "homepage": "https://github.com/setiawancahyonoputro/nexapy",
        "main": "index.js",
        "scripts": {
            "build": "tsc && gulp copy:icons",
            "lint": "eslint src --ext .ts",
            "prepublishOnly": "npm run build",
        },
        "files": ["dist"],
        "n8n": {
            "n8nNodesApiVersion": 1,
            "credentials": ["dist/credentials/NexaPyApi.credentials.js"],
            "nodes": ["dist/nodes/NexaPy/NexaPy.node.js"],
        },
        "devDependencies": {
            "@types/express": "^4.17.17",
            "@types/node": "^18.16.0",
            "gulp": "^4.0.2",
            "n8n-workflow": "^1.0.0",
            "typescript": "~5.1.6",
        },
    }
    (out_path / "package.json").write_text(json.dumps(pkg_data, indent=2), encoding="utf-8")

    # 2. tsconfig.json
    tsconfig_data = {
        "compilerOptions": {
            "target": "ES2019",
            "module": "commonjs",
            "lib": ["es2019"],
            "declaration": True,
            "outDir": "./dist",
            "rootDir": "./src",
            "strict": True,
            "esModuleInterop": True,
            "skipLibCheck": True,
            "forceConsistentCasingInFileNames": True,
        },
        "include": ["src/**/*"],
    }
    (out_path / "tsconfig.json").write_text(json.dumps(tsconfig_data, indent=2), encoding="utf-8")

    # 3. gulpfile.js
    gulpfile_code = """const { src, dest } = require('gulp');

function copyIcons() {
	return src('src/nodes/**/*.{png,svg}')
		.pipe(dest('dist/nodes'));
}

exports['copy:icons'] = copyIcons;
exports.default = copyIcons;
"""
    (out_path / "gulpfile.js").write_text(gulpfile_code, encoding="utf-8")

    # Directories
    nodes_dir = out_path / "src" / "nodes" / "NexaPy"
    creds_dir = out_path / "src" / "credentials"
    nodes_dir.mkdir(parents=True, exist_ok=True)
    creds_dir.mkdir(parents=True, exist_ok=True)

    # 4. nexapy.svg (Icon)
    svg_code = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100" width="100" height="100">
  <defs>
    <linearGradient id="nexaGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#4F46E5" />
      <stop offset="100%" stop-color="#06B6D4" />
    </linearGradient>
  </defs>
  <circle cx="50" cy="50" r="45" fill="url(#nexaGrad)" />
  <path d="M 30 70 L 30 30 L 70 70 L 70 30" stroke="#FFFFFF" stroke-width="8" stroke-linecap="round" stroke-linejoin="round" fill="none" />
</svg>"""
    (nodes_dir / "nexapy.svg").write_text(svg_code, encoding="utf-8")

    # 5. Credentials (NexaPyApi.credentials.ts)
    creds_code = f"""import {{
	ICredentialType,
	INodeProperties,
}} from 'n8n-workflow';

export class NexaPyApi implements ICredentialType {{
	name = 'nexapyApi';
	displayName = 'NexaPy API';
	documentationUrl = 'https://github.com/setiawancahyonoputro/nexapy';
	properties: INodeProperties[] = [
		{{
			displayName: 'Base Host URL',
			name: 'baseUrl',
			type: 'string',
			default: '{default_host}',
			placeholder: 'http://localhost:8000',
			required: true,
		}},
		{{
			displayName: 'API Key',
			name: 'apiKey',
			type: 'string',
			typeOptions: {{ password: true }},
			default: '',
			description: 'API key for X-NexaPy-Key authentication',
		}},
		{{
			displayName: 'HMAC Secret',
			name: 'hmacSecret',
			type: 'string',
			typeOptions: {{ password: true }},
			default: '',
			description: 'Secret for HMAC SHA-256 webhook signatures',
		}},
	];
}}
"""
    (creds_dir / "NexaPyApi.credentials.ts").write_text(creds_code, encoding="utf-8")

    # 6. Node Class (NexaPy.node.ts)
    node_code = """import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	JsonObject,
} from 'n8n-workflow';

export class NexaPy implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'NexaPy AI Framework',
		name: 'nexapy',
		icon: 'file:nexapy.svg',
		group: ['transform'],
		version: 1,
		subtitle: '={{$parameter["operation"]}}',
		description: 'Execute NexaPy AI completion, workflows, and query telemetry',
		defaults: {
			name: 'NexaPy',
		},
		inputs: ['main'],
		outputs: ['main'],
		credentials: [
			{
				name: 'nexapyApi',
				required: false,
			},
		],
		properties: [
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'AI Chat Completion',
						value: 'chatCompletion',
						description: 'Execute AI completion via NexaPy Router',
						action: 'Execute AI chat completion',
					},
					{
						name: 'Execute Workflow',
						value: 'executeWorkflow',
						description: 'Trigger a registered NexaPy automation workflow',
						action: 'Trigger automation workflow',
					},
					{
						name: 'Get Workflow Execution',
						value: 'getExecution',
						description: 'Retrieve telemetry logs of a workflow run',
						action: 'Get execution telemetry',
					},
					{
						name: 'List Workflows',
						value: 'listWorkflows',
						description: 'Discover all registered workflows and their schemas',
						action: 'List registered workflows',
					},
					{
						name: 'Health Check',
						value: 'healthCheck',
						description: 'Check NexaPy server status',
						action: 'Server health check',
					},
				],
				default: 'chatCompletion',
			},
			{
				displayName: 'Prompt',
				name: 'prompt',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['chatCompletion'],
					},
				},
				default: '',
				placeholder: 'Enter your AI prompt here',
				required: true,
			},
			{
				displayName: 'Provider',
				name: 'provider',
				type: 'options',
				displayOptions: {
					show: {
						operation: ['chatCompletion'],
					},
				},
				options: [
					{ name: 'Auto (Priority Fallback)', value: 'auto' },
					{ name: 'OpenAI (GPT)', value: 'openai' },
					{ name: 'Google Gemini', value: 'gemini' },
					{ name: 'Anthropic Claude', value: 'anthropic' },
					{ name: 'DeepSeek', value: 'deepseek' },
					{ name: 'FreeModel', value: 'freemodel' },
				],
				default: 'auto',
			},
			{
				displayName: 'Workflow Name',
				name: 'workflowName',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['executeWorkflow'],
					},
				},
				default: '',
				placeholder: 'my_workflow_name',
				required: true,
			},
			{
				displayName: 'Input JSON Payload',
				name: 'inputData',
				type: 'json',
				displayOptions: {
					show: {
						operation: ['executeWorkflow'],
					},
				},
				default: '{}',
				description: 'JSON object payload passed to the workflow',
			},
			{
				displayName: 'Execution ID',
				name: 'executionId',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['getExecution'],
					},
				},
				default: '',
				placeholder: 'exec_abc123',
				required: true,
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		const operation = this.getNodeParameter('operation', 0) as string;

		const credentials = (await this.getCredentials('nexapyApi').catch(() => undefined)) as
			| { baseUrl?: string; apiKey?: string; hmacSecret?: string }
			| undefined;

		const baseUrl = (credentials?.baseUrl || 'http://localhost:8000').replace(/\/+$/, '');
		const apiKey = credentials?.apiKey || '';
		const hmacSecret = credentials?.hmacSecret || '';

		const buildHeaders = (bodyObj: any) => {
			const reqHeaders: Record<string, string> = {
				'Content-Type': 'application/json',
			};
			if (apiKey) {
				reqHeaders['X-NexaPy-Key'] = apiKey;
			}
			if (hmacSecret) {
				const crypto = require('crypto');
				const ts = Math.floor(Date.now() / 1000).toString();
				const nonce = Math.random().toString(36).substring(2, 12);
				const payloadStr = JSON.stringify(bodyObj || {});
				const sig = crypto.createHmac('sha256', hmacSecret)
					.update(`${ts}.${payloadStr}`)
					.digest('hex');
				reqHeaders['X-NexaPy-Signature'] = `sha256=${sig}`;
				reqHeaders['X-NexaPy-Timestamp'] = ts;
				reqHeaders['X-NexaPy-Nonce'] = nonce;
			}
			return reqHeaders;
		};

		for (let i = 0; i < items.length; i++) {
			try {
				let responseData: any;

				if (operation === 'chatCompletion') {
					const prompt = this.getNodeParameter('prompt', i) as string;
					const provider = this.getNodeParameter('provider', i) as string;
					const reqBody: any = { prompt };
					if (provider && provider !== 'auto') {
						reqBody.provider = provider;
					}

					responseData = await this.helpers.httpRequest({
						method: 'POST',
						url: `${baseUrl}/ai/chat`,
						headers: buildHeaders(reqBody),
						body: reqBody,
						json: true,
					});
				} else if (operation === 'executeWorkflow') {
					const workflowName = this.getNodeParameter('workflowName', i) as string;
					const inputRaw = this.getNodeParameter('inputData', i) as any;
					const reqBody = typeof inputRaw === 'string' ? JSON.parse(inputRaw) : (inputRaw || {});

					responseData = await this.helpers.httpRequest({
						method: 'POST',
						url: `${baseUrl}/workflows/${workflowName}`,
						headers: buildHeaders(reqBody),
						body: reqBody,
						json: true,
					});
				} else if (operation === 'getExecution') {
					const executionId = this.getNodeParameter('executionId', i) as string;
					responseData = await this.helpers.httpRequest({
						method: 'GET',
						url: `${baseUrl}/workflows/executions/${executionId}`,
						headers,
						json: true,
					});
				} else if (operation === 'listWorkflows') {
					responseData = await this.helpers.httpRequest({
						method: 'GET',
						url: `${baseUrl}/workflows/_schema`,
						headers,
						json: true,
					});
				} else if (operation === 'healthCheck') {
					responseData = await this.helpers.httpRequest({
						method: 'GET',
						url: `${baseUrl}/health`,
						json: true,
					});
				}

				if (responseData) {
					returnData.push({ json: responseData as JsonObject });
				}
			} catch (error: any) {
				if (this.continueOnFail()) {
					returnData.push({ json: { error: error.message, status: 'failed' } });
				} else {
					throw error;
				}
			}
		}

		return [returnData];
	}
}
"""
    (nodes_dir / "NexaPy.node.ts").write_text(node_code, encoding="utf-8")

    # 7. README.md
    readme_code = f"""# {package_name}

Native n8n Community Node package for integrating with **NexaPy** AI Framework.

## Features
- AI Chat Completion with multi-provider routing (OpenAI, Gemini, Claude, DeepSeek, FreeModel)
- Trigger and run NexaPy workflows directly from n8n
- Query execution telemetry and status
- Discover workflow schemas automatically

## Installation in n8n

```bash
npm install {package_name}
```

Or build and link locally:
```bash
npm install
npm run build
npm link
```
"""
    (out_path / "README.md").write_text(readme_code, encoding="utf-8")

    return out_path
