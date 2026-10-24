from fastapi import APIRouter

from .chat import router as chat_router
from .configuration import router as configuration_router
from .probe import router as probe_router

router = APIRouter(prefix="/ai")
router.include_router(configuration_router)
router.include_router(probe_router)
router.include_router(chat_router)

__all__ = ["router"]
