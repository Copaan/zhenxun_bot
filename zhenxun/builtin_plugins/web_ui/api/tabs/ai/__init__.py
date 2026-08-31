from fastapi import APIRouter

from .configuration import router as configuration_router

router = APIRouter(prefix="/ai")
router.include_router(configuration_router)

__all__ = ["router"]
