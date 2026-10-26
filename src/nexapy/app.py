import inspect
import json
import os
import secrets
from typing import Any, Callable, Dict, List, Literal, Optional, Union
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from .config import Config, config as global_config
from .ai.base import AIResponse
from .ai.router import AI
from .security import (
    AIAuthGuard,
    HMACVerifier,
    RateLimiter,
    generate_correlation_id,
    sanitize_sensitive_text,
)


class AIChatRequest(BaseModel):
    """Interactive request schema for NexaPy AI completion endpoints."""

    prompt: str = Field(..., min_length=1, description="Prompt or message for AI completion")
    provider: Optional[str] = Field(
        default=None,
        description="Optional provider override (e.g. auto, freemodel, gemini, openai, anthropic, deepseek)",
    )
    reasoning: Optional[Literal["low", "medium", "high"]] = Field(
        default=None, description="Optional reasoning effort (low, medium, high)"
    )
    temperature: Optional[float] = Field(default=None, ge=0.0, le=2.0, description="Sampling temperature")
    max_tokens: Optional[int] = Field(default=None, gt=0, description="Maximum output tokens")


from contextlib import asynccontextmanager


class NexaPy:
    """NexaPy Framework Web Application Wrapper around FastAPI."""

    def __init__(
        self,
        title: Optional[str] = None,
        version: Optional[str] = None,
        debug: bool = False,
        enable_cors: Optional[bool] = None,
        cors_origins: Optional[List[str]] = None,
        config: Optional[Config] = None,
        allow_anonymous_ai: Optional[bool] = None,
        ai_api_keys: Optional[List[str]] = None,
        **kwargs: Any,
    ):
        from . import __version__ as pkg_version
        from .automation.queue import job_queue

        self.version = version or pkg_version
        self.config = config or global_config
        app_title = title or self.config.get("app.name", "NexaPy App")

        # Lifespan context manager for worker pool and background jobs
        user_lifespan = kwargs.pop("lifespan", None)

        @asynccontextmanager
        async def app_lifespan(app_inst: FastAPI):
            # Startup: recover jobs and start background workers
            job_queue.start_workers()
            if user_lifespan:
                async with user_lifespan(app_inst) as maybe_state:
                    yield maybe_state
            else:
                yield
            # Shutdown: cleanly terminate workers
            await job_queue.stop_workers()

        self.fastapi = FastAPI(title=app_title, version=self.version, debug=debug, lifespan=app_lifespan, **kwargs)

        self._ai_engine = AI(config=self.config)
        self._ai_limiter = RateLimiter(max_requests=int(self.config.get("ai.rate_limit_requests", 120)))

        ai_key = (
            self.config.get("ai.api_key")
            or self.config.get("security.api_key")
            or os.getenv("NEXAPY_AI_KEY")
            or os.getenv("NEXAPY_API_KEY")
        )
        configured_keys = list(ai_api_keys or [])
        if ai_key and ai_key not in configured_keys:
            configured_keys.append(ai_key)

        token_limit = self.config.get("ai.token_limit")

        # Allow anonymous AI mode: false by default for strict security (P0 Finding 3)
        if allow_anonymous_ai is None:
            anon_cfg = self.config.get("ai.allow_anonymous", self.config.get("ai.allow_anonymous_ai", "false"))
            self.allow_anonymous_ai = str(anon_cfg).lower() in ("true", "1", "yes")
        else:
            self.allow_anonymous_ai = bool(allow_anonymous_ai)

        self.ai_guard = AIAuthGuard(
            valid_keys=configured_keys,
            max_requests_per_minute=int(self.config.get("ai.rate_limit_requests", 120)),
            token_limit=int(token_limit) if token_limit else None,
            allow_anonymous=self.allow_anonymous_ai,
        )

        # Configure persistent SQLite job queue if configured
        queue_db = self.config.get("queue.db_path", self.config.get("workflows.db_path", os.getenv("NEXAPY_JOBS_DB")))
        if queue_db and not job_queue.db_path:
            job_queue.db_path = str(queue_db)
            job_queue._init_db()

        self._custom_ai_routes: Dict[str, Callable] = {}

        self._setup_cors(enable_cors, cors_origins)
        self._setup_default_routes()

    def _setup_cors(self, enable_cors: Optional[bool], cors_origins: Optional[List[str]]) -> None:
        cors_enabled = enable_cors
        if cors_enabled is None:
            cors_enabled = str(self.config.get("cors.enabled", "false")).lower() in ("true", "1", "yes")

        if cors_enabled:
            origins = cors_origins or self.config.get("cors.origins", ["http://localhost:3000", "http://localhost:5173"])
            if isinstance(origins, str):
                origins = [origins]

            allow_credentials = True
            if "*" in origins:
                allow_credentials = False

            self.fastapi.add_middleware(
                CORSMiddleware,
                allow_origins=origins,
                allow_credentials=allow_credentials,
                allow_methods=["*"],
                allow_headers=["*"],
            )

    def _setup_default_routes(self) -> None:
        @self.fastapi.get("/health", tags=["System"])
        async def health_check():
            return {"status": "ok", "framework": "NexaPy", "version": self.version}

        # Default standard /ai/chat route (P0 Finding 9)
        @self.fastapi.post("/ai/chat", tags=["AI Chat"], response_model=AIResponse)
        async def default_chat_ai(body: AIChatRequest, request: Request):
            norm_path = "/ai/chat"
            if norm_path in self._custom_ai_routes:
                return await self._custom_ai_routes[norm_path](body, request)
            auth_ok, key_id, err_msg = self.ai_guard.is_authorized(request)
            if not auth_ok:
                is_auth_error = (
                    "invalid" in (err_msg or "").lower()
                    or "missing" in (err_msg or "").lower()
                    or "required" in (err_msg or "").lower()
                )
                status_code = 401 if is_auth_error else 429
                return JSONResponse(
                    status_code=status_code,
                    content={
                        "text": "",
                        "provider": "nexapy",
                        "model": "none",
                        "success": False,
                        "error": err_msg,
                    },
                )

            ai_res = await self._ai_engine.chat(
                body.prompt,
                provider=body.provider,
                reasoning=body.reasoning,
                temperature=body.temperature,
                max_tokens=body.max_tokens,
            )

            # Record token usage quota (P1 Finding 10)
            if ai_res.success and key_id:
                tokens_count = len(body.prompt.split()) + len((ai_res.text or "").split())
                self.ai_guard.record_usage(key_id, tokens_count)

            status_code = 200 if ai_res.success else 503
            return JSONResponse(status_code=status_code, content=ai_res.dict_response())

        @self.fastapi.post("/ai/stream", tags=["AI Stream"])
        async def stream_ai(request: Request, body: AIChatRequest):
            auth_ok, key_id, err_msg = self.ai_guard.is_authorized(request)
            if not auth_ok:
                is_auth_error = (
                    "invalid" in (err_msg or "").lower()
                    or "missing" in (err_msg or "").lower()
                    or "required" in (err_msg or "").lower()
                )
                status_code = 401 if is_auth_error else 429
                return JSONResponse(
                    status_code=status_code,
                    content={
                        "text": "",
                        "provider": "nexapy",
                        "model": "none",
                        "success": False,
                        "error": err_msg,
                    },
                )

            async def event_generator():
                generated_words = 0
                try:
                    async for chunk in self._ai_engine.stream(
                        body.prompt,
                        provider=body.provider,
                        reasoning=body.reasoning,
                        temperature=body.temperature,
                        max_tokens=body.max_tokens,
                    ):
                        generated_words += len(chunk.split())
                        yield f"data: {json.dumps({'text': chunk, 'success': True})}\n\n"
                    # Record usage on stream completion (P1 Finding 10)
                    if key_id and generated_words > 0:
                        total_tokens = len(body.prompt.split()) + generated_words
                        self.ai_guard.record_usage(key_id, total_tokens)
                    yield "data: [DONE]\n\n"
                except Exception as stream_err:
                    clean_err = sanitize_sensitive_text(str(stream_err))
                    yield f"data: {json.dumps({'text': '', 'success': False, 'error': clean_err})}\n\n"

            return StreamingResponse(event_generator(), media_type="text/event-stream")

    def ai(
        self,
        path: str = "/ai/chat",
        provider: Optional[str] = None,
        methods: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> Callable:
        """
        Decorator to create an AI completion route with OpenAPI request and response schemas.
        Defaults to POST-only route for security.
        """
        route_methods = methods or ["POST"]
        route_kwargs = {"response_model": AIResponse, "tags": ["AI Route"], **kwargs}

        def decorator(func: Callable) -> Callable:
            norm_path = "/" + path.strip("/")

            async def ai_endpoint(body: AIChatRequest, request: Request):
                auth_ok, key_id, err_msg = self.ai_guard.is_authorized(request)
                if not auth_ok:
                    is_auth_error = (
                        "invalid" in (err_msg or "").lower()
                        or "missing" in (err_msg or "").lower()
                        or "required" in (err_msg or "").lower()
                    )
                    status_code = 401 if is_auth_error else 429
                    return JSONResponse(
                        status_code=status_code,
                        content={
                            "text": "",
                            "provider": "nexapy",
                            "model": "none",
                            "success": False,
                            "error": err_msg,
                        },
                    )

                query_prompt = body.prompt
                req_provider = body.provider or provider
                req_reasoning = body.reasoning
                req_temperature = body.temperature
                req_max_tokens = body.max_tokens

                final_prompt = query_prompt
                if callable(func):
                    sig = inspect.signature(func)
                    if len(sig.parameters) > 0:
                        res = await func(query_prompt) if inspect.iscoroutinefunction(func) else func(query_prompt)
                    else:
                        res = await func() if inspect.iscoroutinefunction(func) else func()

                    if isinstance(res, AIResponse):
                        if res.success and key_id:
                            tokens_count = len(final_prompt.split()) + len((res.text or "").split())
                            self.ai_guard.record_usage(key_id, tokens_count)
                        status_code = 200 if res.success else 503
                        return JSONResponse(status_code=status_code, content=res.dict_response())
                    elif isinstance(res, str) and res.strip():
                        final_prompt = res

                ai_res = await self._ai_engine.chat(
                    final_prompt,
                    provider=req_provider,
                    reasoning=req_reasoning,
                    temperature=req_temperature,
                    max_tokens=req_max_tokens,
                )
                if ai_res.success and key_id:
                    tokens_count = len(final_prompt.split()) + len((ai_res.text or "").split())
                    self.ai_guard.record_usage(key_id, tokens_count)

                status_code = 200 if ai_res.success else 503
                return JSONResponse(status_code=status_code, content=ai_res.dict_response())

            self._custom_ai_routes[norm_path] = ai_endpoint
            if norm_path != "/ai/chat":
                self.fastapi.api_route(path, methods=route_methods, **route_kwargs)(ai_endpoint)

            return ai_endpoint
        return decorator

    def mount_workflows(
        self,
        prefix: str = "/workflows",
        api_key: Optional[str] = None,
        hmac_secret: Optional[str] = None,
        rate_limit_requests: int = 100,
        max_payload_bytes: int = 10 * 1024 * 1024,
        allow_anonymous: bool = False,
        protect_schema: bool = False,
    ) -> None:
        """
        Mount registered NexaPy automation workflows under the specified HTTP prefix.
        Secured by default with API key / HMAC verification, payload limits, and rate limiting.
        Fails closed when unconfigured unless allow_anonymous is explicitly True.
        """
        from fastapi import Request
        from .automation.queue import job_queue

        # Ensure background job workers are active (P0 Finding 4)
        if not job_queue._running:
            job_queue.start_workers()

        route_prefix = "/" + prefix.strip("/")
        limiter = RateLimiter(max_requests=rate_limit_requests)

        effective_api_key = (
            api_key
            or self.config.get("security.api_key")
            or self.config.get("workflows.api_key")
            or os.getenv("NEXAPY_API_KEY")
        )
        effective_hmac_secret = (
            hmac_secret
            or self.config.get("security.hmac_secret")
            or os.getenv("NEXAPY_HMAC_SECRET")
        )

        if not allow_anonymous and not effective_api_key and not effective_hmac_secret:
            raise RuntimeError(
                "Workflow authentication must be configured. "
                "Provide api_key, hmac_secret, set NEXAPY_API_KEY environment variable, "
                "or explicitly pass allow_anonymous=True for development."
            )

        def _verify_security(req: Request, body_bytes: bytes) -> Optional[JSONResponse]:
            correlation_id = req.headers.get("X-Correlation-ID") or generate_correlation_id()
            res_headers = {"X-Correlation-ID": correlation_id}

            # Payload size check
            if max_payload_bytes and len(body_bytes) > max_payload_bytes:
                return JSONResponse(
                    status_code=413,
                    headers=res_headers,
                    content={
                        "success": False,
                        "error_code": "PAYLOAD_TOO_LARGE",
                        "error": f"Payload size exceeds limit of {max_payload_bytes} bytes.",
                        "correlation_id": correlation_id,
                    },
                )

            # Rate limiting check
            client_ip = req.client.host if req.client else "unknown"
            allowed, _ = limiter.is_allowed(client_ip)
            if not allowed:
                return JSONResponse(
                    status_code=429,
                    headers=res_headers,
                    content={
                        "success": False,
                        "error_code": "RATE_LIMIT_EXCEEDED",
                        "error": "Rate limit exceeded.",
                        "correlation_id": correlation_id,
                    },
                )

            # Fail-closed authentication check
            if not allow_anonymous:
                if not effective_api_key and not effective_hmac_secret:
                    return JSONResponse(
                        status_code=403,
                        headers=res_headers,
                        content={
                            "success": False,
                            "error_code": "AUTH_CONFIG_REQUIRED",
                            "error": "Workflow authentication is required but not configured.",
                            "correlation_id": correlation_id,
                        },
                    )

                auth_satisfied = False

                # 1. Check API Key
                if effective_api_key:
                    provided_key = (
                        req.headers.get("X-NexaPy-Key")
                        or req.headers.get("x-nexapy-key")
                        or req.headers.get("Authorization", "").replace("Bearer ", "").strip()
                    )
                    if provided_key == effective_api_key:
                        auth_satisfied = True

                # 2. Check HMAC Signature with timestamp and nonce replay protection
                if effective_hmac_secret:
                    sig_header = req.headers.get("X-NexaPy-Signature") or req.headers.get("x-nexapy-signature")
                    ts_header = req.headers.get("X-NexaPy-Timestamp") or req.headers.get("x-nexapy-timestamp")
                    nonce_header = req.headers.get("X-NexaPy-Nonce") or req.headers.get("x-nexapy-nonce")
                    if sig_header and HMACVerifier.verify_signature(
                        effective_hmac_secret,
                        body_bytes,
                        sig_header,
                        timestamp=ts_header,
                        nonce=nonce_header,
                    ):
                        auth_satisfied = True

                if not auth_satisfied:
                    status_code = 401 if effective_api_key and not req.headers.get("X-NexaPy-Signature") else 403
                    return JSONResponse(
                        status_code=status_code,
                        headers=res_headers,
                        content={
                            "success": False,
                            "error_code": "UNAUTHORIZED",
                            "error": "Invalid or missing authentication credentials.",
                            "correlation_id": correlation_id,
                        },
                    )

            return None

        @self.fastapi.get(f"{route_prefix}/_schema", tags=["Workflows"])
        async def discover_workflows_schema(request: Request):
            correlation_id = request.headers.get("X-Correlation-ID") or generate_correlation_id()
            res_headers = {"X-Correlation-ID": correlation_id}
            if protect_schema:
                sec_err = _verify_security(request, b"")
                if sec_err:
                    return sec_err

            from nexapy.automation import workflow_registry
            workflows = workflow_registry.list_all()
            return JSONResponse(
                status_code=200,
                headers=res_headers,
                content={
                    "framework": "NexaPy",
                    "count": len(workflows),
                    "workflows": [
                        {
                            "name": wf.name,
                            "description": getattr(wf, "description", ""),
                            "schedule": getattr(wf, "schedule", None),
                            "type": "DAGWorkflow" if hasattr(wf, "nodes") else "StandardWorkflow",
                        }
                        for wf in workflows
                    ],
                },
            )

        @self.fastapi.get(f"{route_prefix}/executions/{{execution_id}}", tags=["Workflows"])
        async def get_execution_telemetry(execution_id: str, request: Request):
            correlation_id = request.headers.get("X-Correlation-ID") or generate_correlation_id()
            res_headers = {"X-Correlation-ID": correlation_id}
            sec_err = _verify_security(request, b"")
            if sec_err:
                return sec_err

            from nexapy.automation import tracker
            log = tracker.storage.get(execution_id)
            if not log:
                return JSONResponse(
                    status_code=404,
                    headers=res_headers,
                    content={"success": False, "error_code": "NOT_FOUND", "error": f"Execution ID '{execution_id}' not found.", "correlation_id": correlation_id},
                )
            return JSONResponse(
                status_code=200,
                headers=res_headers,
                content={"success": True, "execution": log.to_dict()},
            )

        @self.fastapi.post(f"{route_prefix}/jobs", tags=["Workflows"])
        async def submit_workflow_job(request: Request, body: Dict[str, Any]):
            correlation_id = request.headers.get("X-Correlation-ID") or generate_correlation_id()
            res_headers = {"X-Correlation-ID": correlation_id}
            body_bytes = await request.body()
            sec_err = _verify_security(request, body_bytes)
            if sec_err:
                return sec_err

            from nexapy.automation.queue import job_queue
            workflow_name = body.get("workflow") or body.get("workflow_name")
            if not workflow_name:
                return JSONResponse(
                    status_code=400,
                    headers=res_headers,
                    content={"success": False, "error_code": "INVALID_INPUT", "error": "Field 'workflow' is required.", "correlation_id": correlation_id},
                )

            input_data = body.get("input") or body.get("payload") or {}
            idempotency_key = body.get("idempotency_key")
            job = await job_queue.enqueue(workflow_name, input_data=input_data, idempotency_key=idempotency_key)
            return JSONResponse(
                status_code=202,
                headers=res_headers,
                content={"success": True, "job": job.to_dict()},
            )

        @self.fastapi.get(f"{route_prefix}/jobs/{{job_id}}", tags=["Workflows"])
        async def get_workflow_job_status(job_id: str, request: Request):
            correlation_id = request.headers.get("X-Correlation-ID") or generate_correlation_id()
            res_headers = {"X-Correlation-ID": correlation_id}
            sec_err = _verify_security(request, b"")
            if sec_err:
                return sec_err

            from nexapy.automation.queue import job_queue
            job = job_queue.get_job(job_id)
            if not job:
                return JSONResponse(
                    status_code=404,
                    headers=res_headers,
                    content={"success": False, "error_code": "NOT_FOUND", "error": f"Job '{job_id}' not found.", "correlation_id": correlation_id},
                )
            return JSONResponse(
                status_code=200,
                headers=res_headers,
                content={"success": True, "job": job.to_dict()},
            )

        @self.fastapi.delete(f"{route_prefix}/jobs/{{job_id}}", tags=["Workflows"])
        async def cancel_workflow_job(job_id: str, request: Request):
            correlation_id = request.headers.get("X-Correlation-ID") or generate_correlation_id()
            res_headers = {"X-Correlation-ID": correlation_id}
            sec_err = _verify_security(request, b"")
            if sec_err:
                return sec_err

            from nexapy.automation.queue import job_queue
            cancelled = job_queue.cancel_job(job_id)
            if not cancelled:
                return JSONResponse(
                    status_code=400,
                    headers=res_headers,
                    content={"success": False, "error_code": "CANNOT_CANCEL", "error": f"Job '{job_id}' could not be cancelled.", "correlation_id": correlation_id},
                )
            return JSONResponse(
                status_code=200,
                headers=res_headers,
                content={"success": True, "message": f"Job '{job_id}' cancelled."},
            )

        @self.fastapi.post(f"{route_prefix}/_batch", tags=["Workflows"])
        async def batch_execute_workflows(request: Request, items: List[Dict[str, Any]]):
            from nexapy.automation import workflow_registry, runner

            correlation_id = request.headers.get("X-Correlation-ID") or generate_correlation_id()
            res_headers = {"X-Correlation-ID": correlation_id}
            body_bytes = await request.body()
            sec_err = _verify_security(request, body_bytes)
            if sec_err:
                return sec_err

            results = []
            for item in items:
                wf_name = item.get("workflow")
                inp = item.get("input", {})
                step = item.get("step")
                wf = workflow_registry.get(wf_name) if wf_name else None
                if not wf:
                    results.append({"workflow": wf_name, "success": False, "error": "Workflow not found"})
                    continue

                try:
                    res = await runner.run_async(wf, input_data=inp, step=step)
                    results.append({"workflow": wf_name, "success": True, "result": res})
                except Exception as err:
                    clean_err = sanitize_sensitive_text(str(err))
                    results.append({"workflow": wf_name, "success": False, "error": clean_err})

            return JSONResponse(
                status_code=200,
                headers=res_headers,
                content={"success": True, "batch_results": results},
            )

        @self.fastapi.post(f"{route_prefix}/{{workflow_name}}", tags=["Workflows"])
        async def execute_workflow_endpoint(workflow_name: str, request: Request, body: Optional[Dict[str, Any]] = None):
            from nexapy.automation import workflow_registry, runner

            correlation_id = request.headers.get("X-Correlation-ID") or generate_correlation_id()
            res_headers = {"X-Correlation-ID": correlation_id}
            body_bytes = await request.body()
            sec_err = _verify_security(request, body_bytes)
            if sec_err:
                return sec_err

            wf = workflow_registry.get(workflow_name)
            if not wf:
                return JSONResponse(
                    status_code=404,
                    headers=res_headers,
                    content={
                        "success": False,
                        "workflow": workflow_name,
                        "error_code": "WORKFLOW_NOT_FOUND",
                        "error": f"Workflow '{workflow_name}' not found.",
                        "correlation_id": correlation_id,
                    },
                )

            input_data = body or {}
            if not isinstance(input_data, dict):
                return JSONResponse(
                    status_code=400,
                    headers=res_headers,
                    content={
                        "success": False,
                        "workflow": workflow_name,
                        "error_code": "INVALID_INPUT",
                        "error": "Workflow input must be a JSON object.",
                        "correlation_id": correlation_id,
                    },
                )

            step_name = request.query_params.get("step")
            if not step_name and isinstance(body, dict):
                step_name = body.get("step")

            try:
                raw_result = await runner.run_async(wf, input_data=input_data, step=step_name)

                if hasattr(raw_result, "json") and callable(raw_result.json):
                    try:
                        res_val = raw_result.json()
                    except Exception:
                        res_val = raw_result.text if hasattr(raw_result, "text") else str(raw_result)
                elif hasattr(raw_result, "dict_response") and callable(raw_result.dict_response):
                    res_val = raw_result.dict_response()
                else:
                    res_val = raw_result

                return JSONResponse(
                    status_code=200,
                    headers=res_headers,
                    content={
                        "success": True,
                        "workflow": workflow_name,
                        "result": res_val,
                    },
                )
            except Exception as err:
                clean_err = sanitize_sensitive_text(str(err))
                return JSONResponse(
                    status_code=500,
                    headers=res_headers,
                    content={
                        "success": False,
                        "workflow": workflow_name,
                        "error_code": "WORKFLOW_EXECUTION_ERROR",
                        "error": clean_err,
                        "correlation_id": correlation_id,
                    },
                )


    def get(self, path: str, **kwargs: Any) -> Callable:
        return self.fastapi.get(path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> Callable:
        return self.fastapi.post(path, **kwargs)

    def put(self, path: str, **kwargs: Any) -> Callable:
        return self.fastapi.put(path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> Callable:
        return self.fastapi.delete(path, **kwargs)

    def patch(self, path: str, **kwargs: Any) -> Callable:
        return self.fastapi.patch(path, **kwargs)

    def add_api_route(self, path: str, endpoint: Callable, **kwargs: Any) -> None:
        self.fastapi.add_api_route(path, endpoint, **kwargs)

    async def __call__(self, scope: Dict[str, Any], receive: Callable, send: Callable) -> None:
        """ASGI interface allowing direct execution by uvicorn/hypercorn."""
        await self.fastapi(scope, receive, send)

