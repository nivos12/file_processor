"""
RQ queue setup and job enqueue helper.

Why RQ over Celery:
- Redis is our only broker; RQ is purpose-built for Redis and needs zero broker
  configuration.
- Job serialization is transparent pickle, which is fine for internal payloads.
- The RQ worker is a single `rq worker` command — no separate beat process needed
  for the workloads we have.
- Celery's advantage (multiple broker backends, canvas workflows) is irrelevant here.
"""
import redis
from rq import Queue

from app.config import settings
from app.logging_config import get_logger

logger = get_logger(__name__)

_redis_conn: redis.Redis | None = None
_queue: Queue | None = None


def get_redis() -> redis.Redis:
    global _redis_conn
    if _redis_conn is None:
        _redis_conn = redis.from_url(settings.redis_url)
    return _redis_conn


def get_queue() -> Queue:
    global _queue
    if _queue is None:
        _queue = Queue("pipeline", connection=get_redis())
    return _queue


def enqueue_pipeline(job_id: str) -> None:
    """Enqueue the pipeline execution task for job_id."""
    from app.workers.pipeline import execute_pipeline  # avoid circular at module level
    q = get_queue()
    rq_job = q.enqueue(
        execute_pipeline,
        job_id,
        job_timeout=3600,  # 1 hour max per job
        result_ttl=86400,  # keep result in Redis for 24 h for debugging
    )
    logger.info("job_enqueued", job_id=job_id, rq_job_id=rq_job.id)
