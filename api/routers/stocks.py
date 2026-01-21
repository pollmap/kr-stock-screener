"""
Stocks Router - 종목 관련 API
"""

from fastapi import APIRouter, Query, HTTPException
from typing import List, Optional
from pydantic import BaseModel

router = APIRouter()


class StockResponse(BaseModel):
    code: str
    name: str
    market: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    is_active: bool = True


class StockListResponse(BaseModel):
    total: int
    items: List[StockResponse]


@router.get("", response_model=StockListResponse)
async def get_stocks(
    market: Optional[str] = Query(None, description="KOSPI/KOSDAQ"),
    sector: Optional[str] = Query(None, description="GICS 섹터"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """종목 리스트 조회"""
    # TODO: DB 연동
    sample = [
        {"code": "005930", "name": "삼성전자", "market": "KOSPI", "sector": "IT", "is_active": True},
        {"code": "000660", "name": "SK하이닉스", "market": "KOSPI", "sector": "IT", "is_active": True},
    ]
    return {"total": len(sample), "items": sample}


@router.get("/{code}")
async def get_stock(code: str):
    """단일 종목 조회"""
    return {
        "code": code,
        "name": "삼성전자",
        "market": "KOSPI",
        "sector": "IT"
    }


@router.get("/{code}/financials")
async def get_stock_financials(code: str, years: int = 3):
    """종목 재무제표 조회"""
    return {"code": code, "financials": []}


@router.get("/{code}/prices")
async def get_stock_prices(code: str, days: int = 30):
    """종목 주가 조회"""
    return {"code": code, "prices": []}
