from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api1 import router as api1_router
from .api2 import router as api2_router
from .api3 import router as api3_router
from .api4 import router as api4_router
from .api5 import router as api5_router
from .api6 import router as api6_router


app = FastAPI(
    title="Network Intelligence API",
    version="1.0.0",
    description="Windows-native service layer for curated network analytics.",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:5174",
        "http://127.0.0.1:5174",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(api1_router)
app.include_router(api2_router)
app.include_router(api3_router)
app.include_router(api4_router)
app.include_router(api5_router)
app.include_router(api6_router)