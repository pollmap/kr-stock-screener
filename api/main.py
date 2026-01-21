"""
CUFA Stock Screener v3.0 - FastAPI Backend
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import os

# 라우터
from api.routers import stocks, ratios, dcf, screen

# 앱 초기화
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 시작 시
    print("🚀 CUFA Stock Screener v3.0 API 시작")
    yield
    # 종료 시
    print("👋 API 종료")


app = FastAPI(
    title="CUFA Stock Screener API",
    description="기관투자자급 주식 스크리닝 시스템",
    version="3.0.0",
    lifespan=lifespan
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 등록
app.include_router(stocks.router, prefix="/api/v1/stocks", tags=["Stocks"])
app.include_router(ratios.router, prefix="/api/v1/ratios", tags=["Ratios"])
app.include_router(dcf.router, prefix="/api/v1/dcf", tags=["DCF"])
app.include_router(screen.router, prefix="/api/v1/screen", tags=["Screening"])


@app.get("/")
async def root():
    return {
        "name": "CUFA Stock Screener",
        "version": "3.0.0",
        "status": "running",
        "docs": "/docs"
    }


@app.get("/health")
async def health_check():
    return {"status": "healthy"}
