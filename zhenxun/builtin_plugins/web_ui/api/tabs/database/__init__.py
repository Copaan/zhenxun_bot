from fastapi import APIRouter

from .runtime import router as runtime_router

router = APIRouter(prefix="/database")
router.include_router(runtime_router)
