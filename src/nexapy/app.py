import inspect
from typing import Any, Callable, Dict, List, Literal, Optional
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from .config import Config, config as global_config
from .ai.base import AIResponse
from .ai.router import AI


class AIChatRequest(BaseModel):
    """Interactive request schema for NexaPy AI completion endpoints."""

    prompt: str = Field(..., min_length=1, description="Prompt or message for AI completion")
    provider: Optional[Literal["auto", "freemodel", "gemini"]] = Field(
        default=None, description="Optional provider override (e.g. freemodel, gemini)"
    )
    reasoning: Optional[Literal["low", "medium", "high"]] = Field(
        default=None, description="Optional reasoning effort (low, medium, high)"
    )
    temperature: Optional[float] = Field(default=None, ge=0.0, le=2.0, description="Sampling temperature")
    max_tokens: Optional[int] = Field(default=None, gt=0, description="Maximum output tokens")


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
        **kwargs: Any,
    ):
        from . import __version__ as pkg_version

        self.version = version or pkg_version
        self.config = config or global_config
        app_title = title or self.config.get("app.name", "NexaPy App")
        self.fastapi = FastAPI(title=app_title, version=self.version, debug=debug, **kwargs)

        self._ai_engine = AI(config=self.config)
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
            @self.fastapi.api_route(path, methods=route_methods, **route_kwargs)
            async def ai_endpoint(body: AIChatRequest, request: Request):
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
                status_code = 200 if ai_res.success else 503
                return JSONResponse(status_code=status_code, content=ai_res.dict_response())

            return ai_endpoint
        return decorator

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
