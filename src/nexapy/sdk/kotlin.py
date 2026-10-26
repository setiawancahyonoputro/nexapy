"""NexaPy Kotlin Client SDK Generator (OkHttp and Coroutines compatible)."""

from pathlib import Path
from nexapy import __version__


def generate_kotlin_sdk(
    output_dir: Path,
    base_url: str = "http://localhost:8000",
    ai_path: str = "/ai/chat",
) -> None:
    """Generate Kotlin NexaPyClient SDK (`NexaPyClient.kt`, `Models.kt`, `build.gradle.kts`)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    clean_base_url = base_url.rstrip("/")
    clean_ai_path = "/" + ai_path.lstrip("/")

    # 1. build.gradle.kts
    gradle_kts = f"""plugins {{
    kotlin("jvm") version "1.9.22"
    kotlin("plugin.serialization") version "1.9.22"
}}

group = "com.nexapy"
version = "{__version__}"

repositories {{
    mavenCentral()
}}

dependencies {{
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.3")
}}
"""
    (output_dir / "build.gradle.kts").write_text(gradle_kts, encoding="utf-8")

    src_dir = output_dir / "src" / "main" / "kotlin" / "com" / "nexapy"
    src_dir.mkdir(parents=True, exist_ok=True)

    # 2. Models.kt
    models_kt = """package com.nexapy

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject

@Serializable
data class AIResponse(
    val text: String = "",
    val provider: String = "",
    val model: String = "",
    val success: Boolean = true,
    val error: String? = null
)

@Serializable
data class AIChatRequest(
    val prompt: String,
    val provider: String? = null,
    val reasoning: String? = null,
    val temperature: Double? = null,
    val max_tokens: Int? = null
)

@Serializable
data class WorkflowResponse(
    val success: Boolean = false,
    val workflow: String? = null,
    val error: String? = null
)
"""
    (src_dir / "Models.kt").write_text(models_kt, encoding="utf-8")

    # 3. NexaPyClient.kt
    client_kt = f"""package com.nexapy

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.IOException
import java.util.concurrent.TimeUnit

/**
 * NexaPy Kotlin Client SDK (v{__version__})
 * Seamless coroutines-based HTTP client for NexaPy AI & Automation Framework.
 */
class NexaPyClient(
    val baseUrl: String = "{clean_base_url}",
    val apiKey: String? = null,
    val timeoutSeconds: Long = 30L,
    val aiPath: String = "{clean_ai_path}"
) {{
    private val json = Json {{ ignoreUnknownKeys = true; encodeDefaults = true }}
    private val client = OkHttpClient.Builder()
        .connectTimeout(timeoutSeconds, TimeUnit.SECONDS)
        .readTimeout(timeoutSeconds, TimeUnit.SECONDS)
        .build()

    private val jsonMediaType = "application/json; charset=utf-8".toMediaType()

    suspend fun chat(
        prompt: String,
        provider: String? = null,
        reasoning: String? = null,
        temperature: Double? = null,
        maxTokens: Int? = null
    ): AIResponse = withContext(Dispatchers.IO) {{
        val reqBody = AIChatRequest(
            prompt = prompt,
            provider = provider,
            reasoning = reasoning,
            temperature = temperature,
            max_tokens = maxTokens
        )
        val bodyStr = json.encodeToString(reqBody)
        val responseStr = post(aiPath, bodyStr)
        json.decodeFromString<AIResponse>(responseStr)
    }}

    suspend fun executeWorkflow(
        workflowName: String,
        inputJson: String = "{{}}",
        step: String? = null
    ): String = withContext(Dispatchers.IO) {{
        val query = if (step != null) "?step=$step" else ""
        post("/workflows/$workflowName$query", inputJson)
    }}

    suspend fun getExecution(executionId: String): String = withContext(Dispatchers.IO) {{
        get("/workflows/executions/$executionId")
    }}

    suspend fun listWorkflows(): String = withContext(Dispatchers.IO) {{
        get("/workflows/_schema")
    }}

    suspend fun health(): String = withContext(Dispatchers.IO) {{
        get("/health")
    }}

    private fun post(path: String, jsonBody: String): String {{
        val url = baseUrl.trimEnd('/') + "/" + path.trimStart('/')
        val reqBuilder = Request.Builder()
            .url(url)
            .post(jsonBody.toRequestBody(jsonMediaType))

        apiKey?.let {{ reqBuilder.addHeader("X-NexaPy-Key", it) }}

        client.newCall(reqBuilder.build()).execute().use {{ response ->
            if (!response.isSuccessful && response.code != 503) {{
                throw IOException("HTTP error code ${{response.code}}: ${{response.message}}")
            }}
            return response.body?.string() ?: ""
        }}
    }}

    private fun get(path: String): String {{
        val url = baseUrl.trimEnd('/') + "/" + path.trimStart('/')
        val reqBuilder = Request.Builder().url(url).get()
        apiKey?.let {{ reqBuilder.addHeader("X-NexaPy-Key", it) }}

        client.newCall(reqBuilder.build()).execute().use {{ response ->
            if (!response.isSuccessful) {{
                throw IOException("HTTP error code ${{response.code}}: ${{response.message}}")
            }}
            return response.body?.string() ?: ""
        }}
    }}
}}
"""
    (src_dir / "NexaPyClient.kt").write_text(client_kt, encoding="utf-8")

    # 4. README.md
    readme_md = f"""# NexaPy Kotlin Client SDK (v{__version__})

## Installation (Gradle)
```kotlin
dependencies {{
    implementation("com.nexapy:client:{__version__}")
}}
```

## Quick Start
```kotlin
val client = NexaPyClient(
    baseUrl = "http://localhost:8000",
    apiKey = "your_api_key_here"
)

// Coroutine-friendly AI completion
val response = client.chat("Explain quantum computing", provider = "auto")
println(response.text)

// Execute Workflow
val wfJson = client.executeWorkflow("payment_pipeline", inputJson = \"\"\"{{\"amount\": 100}}\"\"\")
```
"""
    (output_dir / "README.md").write_text(readme_md, encoding="utf-8")
