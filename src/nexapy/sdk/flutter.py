"""NexaPy Flutter / Dart Client SDK Generator."""

from pathlib import Path
from nexapy import __version__


def generate_flutter_sdk(
    output_dir: Path,
    base_url: str = "http://localhost:8000",
    ai_path: str = "/ai/chat",
) -> None:
    """Generate Flutter / Dart NexaPyClient SDK."""
    output_dir.mkdir(parents=True, exist_ok=True)
    clean_base_url = base_url.rstrip("/")
    clean_ai_path = "/" + ai_path.lstrip("/")

    # 1. pubspec.yaml
    pubspec_yaml = f"""name: nexapy_client
description: Official Flutter and Dart Client SDK for NexaPy AI Framework.
version: {__version__}
homepage: https://github.com/setiawancahyonoputro/nexapy

environment:
  sdk: '>=3.0.0 <4.0.0'

dependencies:
  http: ^1.2.0

dev_dependencies:
  lints: ^3.0.0
  test: ^1.24.0
"""
    (output_dir / "pubspec.yaml").write_text(pubspec_yaml, encoding="utf-8")

    lib_dir = output_dir / "lib"
    lib_dir.mkdir(parents=True, exist_ok=True)

    # 2. lib/nexapy_client.dart
    client_dart = f"""import 'dart:convert';
import 'package:http/http.dart' as http;

/// AI Response model from NexaPy.
class AIResponse {{
  final String text;
  final String provider;
  final String model;
  final bool success;
  final String? error;

  AIResponse({{
    required this.text,
    required this.provider,
    required this.model,
    required this.success,
    this.error,
  }});

  factory AIResponse.fromJson(Map<String, dynamic> json) {{
    return AIResponse(
      text: json['text'] as String? ?? '',
      provider: json['provider'] as String? ?? '',
      model: json['model'] as String? ?? '',
      success: json['success'] as bool? ?? false,
      error: json['error'] as String?,
    );
  }}

  Map<String, dynamic> toJson() => {{
    'text': text,
    'provider': provider,
    'model': model,
    'success': success,
    'error': error,
  }};
}}

/// NexaPy Flutter / Dart Client SDK (v{__version__})
class NexaPyClient {{
  final String baseUrl;
  final String? apiKey;
  final String aiPath;
  final Duration timeout;

  NexaPyClient({{
    this.baseUrl = '{clean_base_url}',
    this.apiKey,
    this.aiPath = '{clean_ai_path}',
    this.timeout = const Duration(seconds: 30),
  }});

  Map<String, String> _buildHeaders() {{
    final headers = <String, String>{{
      'Content-Type': 'application/json',
      'Accept': 'application/json',
    }};
    if (apiKey != null && apiKey!.isNotEmpty) {{
      headers['X-NexaPy-Key'] = apiKey!;
    }}
    return headers;
  }}

  /// Execute AI Completion via NexaPy Router.
  Future<AIResponse> chat(
    String prompt, {{
    String? provider,
    String? reasoning,
    double? temperature,
    int? maxTokens,
  }}) async {{
    final payload = <String, dynamic>{{'prompt': prompt}};
    if (provider != null) payload['provider'] = provider;
    if (reasoning != null) payload['reasoning'] = reasoning;
    if (temperature != null) payload['temperature'] = temperature;
    if (maxTokens != null) payload['max_tokens'] = maxTokens;

    final url = Uri.parse('$baseUrl$aiPath');
    final response = await http
        .post(url, headers: _buildHeaders(), body: jsonEncode(payload))
        .timeout(timeout);

    final data = jsonDecode(utf8.decode(response.bodyBytes)) as Map<String, dynamic>;
    return AIResponse.fromJson(data);
  }}

  /// Execute a registered NexaPy automation workflow.
  Future<Map<String, dynamic>> executeWorkflow(
    String workflowName, {{
    Map<String, dynamic>? input,
    String? step,
  }}) async {{
    final query = step != null ? '?step=$step' : '';
    final url = Uri.parse('$baseUrl/workflows/$workflowName$query');
    final response = await http
        .post(url, headers: _buildHeaders(), body: jsonEncode(input ?? {{}}))
        .timeout(timeout);

    return jsonDecode(utf8.decode(response.bodyBytes)) as Map<String, dynamic>;
  }}

  /// Query telemetry execution history.
  Future<Map<String, dynamic>> getExecution(String executionId) async {{
    final url = Uri.parse('$baseUrl/workflows/executions/$executionId');
    final response = await http.get(url, headers: _buildHeaders()).timeout(timeout);
    return jsonDecode(utf8.decode(response.bodyBytes)) as Map<String, dynamic>;
  }}

  /// Discover all registered workflows.
  Future<Map<String, dynamic>> listWorkflows() async {{
    final url = Uri.parse('$baseUrl/workflows/_schema');
    final response = await http.get(url, headers: _buildHeaders()).timeout(timeout);
    return jsonDecode(utf8.decode(response.bodyBytes)) as Map<String, dynamic>;
  }}

  /// Check server health.
  Future<Map<String, dynamic>> health() async {{
    final url = Uri.parse('$baseUrl/health');
    final response = await http.get(url).timeout(timeout);
    return jsonDecode(utf8.decode(response.bodyBytes)) as Map<String, dynamic>;
  }}
}}
"""
    (lib_dir / "nexapy_client.dart").write_text(client_dart, encoding="utf-8")

    # 3. README.md
    readme_md = f"""# NexaPy Flutter / Dart Client SDK (v{__version__})

## Installation
Add to your `pubspec.yaml`:
```yaml
dependencies:
  nexapy_client: ^{__version__}
```

## Quick Start
```dart
import 'package:nexapy_client/nexapy_client.dart';

void main() async {{
  final client = NexaPyClient(
    baseUrl: 'http://localhost:8000',
    apiKey: 'your_api_key_here',
  );

  // 1. AI Completion
  final res = await client.chat('Explain neural networks', provider: 'auto');
  print(res.text);

  // 2. Execute Workflow
  final wfResult = await client.executeWorkflow('sync_orders', input: {{'limit': 50}});
  print(wfResult);
}}
```
"""
    (output_dir / "README.md").write_text(readme_md, encoding="utf-8")
