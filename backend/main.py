import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.config import settings
from backend.routers import admin, alerts, benchmarks, ga4, pacing, performance, projects, traditional
from backend.services import bigquery_client as bq

logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting CIP backend (env=%s)", settings.app_env)
    try:
        bq.get_client()
    except Exception:
        logger.warning("BigQuery client failed to initialise — endpoints needing BQ will error at query time")
    yield
    bq.close_client()
    logger.info("CIP backend shut down")


app = FastAPI(
    title="Campaign Intelligence Platform",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from backend.middleware.auth import FirebaseAuthMiddleware
app.add_middleware(FirebaseAuthMiddleware)

app.include_router(projects.router)
app.include_router(performance.router)
app.include_router(pacing.router)
app.include_router(alerts.router)
app.include_router(ga4.router)
app.include_router(benchmarks.router)
app.include_router(traditional.router)
app.include_router(admin.router)


@app.get("/health")
async def health():
    bq_ok = bq.ping()
    status = "ok" if bq_ok else "degraded"
    return {
        "status": status,
        "service": "cip-backend",
        "bigquery": "connected" if bq_ok else "unreachable",
    }
