"""
Ratios Router - 재무비율 API
"""

from fastapi import APIRouter, Query
from typing import List, Optional
from pydantic import BaseModel

router = APIRouter()


class RatioResponse(BaseModel):
    code: str
    name: Optional[str] = None
    per: Optional[float] = None
    pbr: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    debt_ratio: Optional[float] = None


@router.get("")
async def get_all_ratios(
    sector: Optional[str] = None,
    min_roe: Optional[float] = None,
    max_per: Optional[float] = None,
    limit: int = 100
):
    """전체 재무비율 조회"""
    return {"total": 0, "items": []}


@router.get("/{code}")
async def get_stock_ratios(code: str):
    """종목별 재무비율"""
    return {
        "code": code,
        "per": 12.5,
        "pbr": 1.3,
        "roe": 15.2,
        "roa": 8.5,
        "debt_ratio": 35.0
    }


@router.get("/{code}/sector-rank")
async def get_sector_rank(code: str):
    """섹터 내 상대순위"""
    return {
        "code": code,
        "sector": "IT",
        "per_rank_pct": 0.25,
        "roe_rank_pct": 0.85
    }
