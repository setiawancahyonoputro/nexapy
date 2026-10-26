"""NexaPy PHP Client SDK Generator (PSR-18 and cURL compatible)."""

from pathlib import Path
from nexapy import __version__


def generate_php_sdk(
    output_dir: Path,
    base_url: str = "http://localhost:8000",
    ai_path: str = "/ai/chat",
) -> None:
    """Generate a clean, modern PHP 8.1+ NexaPyClient SDK."""
    output_dir.mkdir(parents=True, exist_ok=True)
    clean_base_url = base_url.rstrip("/")
    clean_ai_path = "/" + ai_path.lstrip("/")

    # 1. composer.json
    composer_json = f"""{{
  "name": "nexapy/client",
  "description": "Official PHP Client SDK for NexaPy AI Framework",
  "version": "{__version__}",
  "license": "MIT",
  "require": {{
    "php": ">=8.1"
  }},
  "suggest": {{
    "psr/http-client": "Allows using standard PSR-18 HTTP clients (e.g. Guzzle, Symfony)",
    "psr/http-factory": "Required when using PSR-18 client integration"
  }},
  "autoload": {{
    "psr-4": {{
      "NexaPy\\\\": "src/"
    }}
  }}
}}
"""
    (output_dir / "composer.json").write_text(composer_json, encoding="utf-8")

    src_dir = output_dir / "src"
    src_dir.mkdir(parents=True, exist_ok=True)

    # 2. NexaPyClient.php
    client_php = f"""<?php

declare(strict_types=1);

namespace NexaPy;

/**
 * NexaPy PHP Client SDK (v{__version__})
 * Supports both PSR-18 HTTP Client injection and native cURL transport.
 */
class NexaPyClient
{{
    private string $baseUrl;
    private ?string $apiKey;
    private int $timeout;
    private string $aiPath;
    private mixed $httpClient;
    private mixed $requestFactory;

    public function __construct(
        string $baseUrl = '{clean_base_url}',
        ?string $apiKey = null,
        int $timeout = 30,
        string $aiPath = '{clean_ai_path}',
        mixed $httpClient = null,
        mixed $requestFactory = null
    ) {{
        $this->baseUrl = rtrim($baseUrl, '/');
        $this->apiKey = $apiKey;
        $this->timeout = $timeout;
        $this->aiPath = '/' . ltrim($aiPath, '/');
        $this->httpClient = $httpClient;
        $this->requestFactory = $requestFactory;
    }}

    /**
     * Send prompt to NexaPy AI Router.
     *
     * @param string $prompt
     * @param array<string, mixed> $options ['provider' => 'auto', 'temperature' => 0.7, ...]
     * @return array<string, mixed>
     */
    public function chat(string $prompt, array $options = []): array
    {{
        $payload = array_merge(['prompt' => $prompt], $options);
        return $this->request('POST', $this->aiPath, $payload);
    }}

    /**
     * Execute a registered NexaPy automation workflow.
     *
     * @param string $workflowName
     * @param array<string, mixed> $input
     * @param string|null $step Optional specific DAG step to execute
     * @return array<string, mixed>
     */
    public function executeWorkflow(string $workflowName, array $input = [], ?string $step = null): array
    {{
        $path = '/workflows/' . rawurlencode($workflowName);
        if ($step !== null) {{
            $path .= '?step=' . urlencode($step);
        }}
        return $this->request('POST', $path, $input);
    }}

    /**
     * Retrieve execution telemetry log.
     *
     * @param string $executionId
     * @return array<string, mixed>
     */
    public function getExecution(string $executionId): array
    {{
        return $this->request('GET', '/workflows/executions/' . rawurlencode($executionId));
    }}

    /**
     * Discover registered workflow schemas.
     *
     * @return array<string, mixed>
     */
    public function listWorkflows(): array
    {{
        return $this->request('GET', '/workflows/_schema');
    }}

    /**
     * Check NexaPy server health.
     *
     * @return array<string, mixed>
     */
    public function health(): array
    {{
        return $this->request('GET', '/health');
    }}

    /**
     * Dispatch HTTP request using either injected PSR-18 Client or native cURL.
     *
     * @param string $method GET, POST, PUT, DELETE, PATCH
     * @param string $endpoint API path
     * @param array<string, mixed>|null $data Payload for POST/PUT/PATCH
     * @return array<string, mixed>
     */
    public function request(string $method, string $endpoint, ?array $data = null): array
    {{
        $url = $this->baseUrl . '/' . ltrim($endpoint, '/');

        // 1. PSR-18 Execution path if client and factory injected
        if ($this->httpClient !== null && $this->requestFactory !== null && method_exists($this->httpClient, 'sendRequest')) {{
            $req = $this->requestFactory->createRequest(strtoupper($method), $url)
                ->withHeader('Content-Type', 'application/json')
                ->withHeader('Accept', 'application/json');
            if ($this->apiKey !== null) {{
                $req = $req->withHeader('X-NexaPy-Key', $this->apiKey);
            }}
            if ($data !== null && in_array(strtoupper($method), ['POST', 'PUT', 'PATCH'], true)) {{
                $req->getBody()->write(json_encode($data, JSON_THROW_ON_ERROR));
            }}
            $res = $this->httpClient->sendRequest($req);
            $body = (string)$res->getBody();
            $decoded = json_decode($body, true);
            return is_array($decoded) ? $decoded : ['raw' => $body, 'http_code' => $res->getStatusCode()];
        }}

        // 2. Native cURL execution path
        $ch = curl_init();

        $headers = ['Content-Type: application/json', 'Accept: application/json'];
        if ($this->apiKey !== null) {{
            $headers[] = 'X-NexaPy-Key: ' . $this->apiKey;
        }}

        $options = [
            CURLOPT_URL => $url,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT => $this->timeout,
            CURLOPT_HTTPHEADER => $headers,
            CURLOPT_CUSTOMREQUEST => strtoupper($method),
        ];

        if ($data !== null && in_array(strtoupper($method), ['POST', 'PUT', 'PATCH'], true)) {{
            $options[CURLOPT_POSTFIELDS] = json_encode($data, JSON_THROW_ON_ERROR);
        }}

        curl_setopt_array($ch, $options);
        $rawResponse = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $curlError = curl_error($ch);
        curl_close($ch);

        if ($curlError) {{
            throw new \\RuntimeException("NexaPy cURL error: " . $curlError);
        }}

        $decoded = json_decode((string)$rawResponse, true);
        if (!is_array($decoded)) {{
            $decoded = ['raw' => $rawResponse, 'http_code' => $httpCode];
        }}

        return $decoded;
    }}
}}
"""
    (src_dir / "NexaPyClient.php").write_text(client_php, encoding="utf-8")

    # 3. README.md
    readme_md = f"""# NexaPy PHP Client SDK (v{__version__})

## Installation
```bash
composer require nexapy/client
```

## Quick Start
```php
require_once __DIR__ . '/vendor/autoload.php';

use NexaPy\\NexaPyClient;

$client = new NexaPyClient(
    baseUrl: 'http://localhost:8000',
    apiKey: 'your_api_key_here'
);

// 1. AI Chat
$res = $client->chat("Explain machine learning in simple terms", [
    'provider' => 'auto'
]);
echo $res['text'];

// 2. Execute Workflow
$wfResult = $client->executeWorkflow('order_processing', ['order_id' => 123]);

// 3. Query Execution Telemetry
$telemetry = $client->getExecution('exec_abc123');
```
"""
    (output_dir / "README.md").write_text(readme_md, encoding="utf-8")
