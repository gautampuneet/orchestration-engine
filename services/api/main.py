"""API service for workflow management."""

from fastapi import FastAPI
from services.api.routes.workflow import router as workflow_router
from services.api.middleware import CorrelationIdMiddleware
from shared.logging_config import setup_logging

setup_logging("api")

app = FastAPI(title="Workflow Orchestration API", version="1.0.0")
app.add_middleware(CorrelationIdMiddleware)

app.include_router(workflow_router, tags=["Workflows"])


@app.get("/")
async def root():
    return {"service": "api", "status": "running"}


@app.get("/health")
async def health():
    return {"status": "healthy"}
