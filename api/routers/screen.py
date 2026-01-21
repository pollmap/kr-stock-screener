"""
Screen Router - 스크리닝 API
"""

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, List

router = APIRouter()


class ScreeningRequest(BaseModel):
    # 밸류에이션
    per_min: Optional[float] = None
    per_max: Optional[float] = None
    pbr_min: Optional[float] = None
    pbr_max: Optional[float] = None
    
    # 수익성
    roe_min: Optional[float] = None
    roa_min: Optional[float] = None
    
    # 안정성
    debt_ratio_max: Optional[float] = None
    
    # 성장성
    revenue_growth_min: Optional[float] = None
    
    # 필터
    market: Optional[str] = None       # KOSPI/KOSDAQ
    sector: Optional[str] = None       # GICS 섹터
    
    # 정렬
    sort_by: str = "roe"
    ascending: bool = False
    
    limit: int = 50


class ScreeningItem(BaseModel):
    code: str
    name: str
    market: str
    sector: Optional[str]
    per: Optional[float]
    pbr: Optional[float]
    roe: Optional[float]
    score: Optional[float]


class ScreeningResponse(BaseModel):
    total: int
    strategy: str
    items: List[ScreeningItem]


@router.post("/run", response_model=ScreeningResponse)
async def run_screening(request: ScreeningRequest):
    """커스텀 스크리닝 실행"""
    # TODO: DB 연동 후 실제 스크리닝
    sample = [
        {"code": "000660", "name": "SK하이닉스", "market": "KOSPI", "sector": "IT", "per": 8.2, "pbr": 1.1, "roe": 22.1, "score": 95},
        {"code": "005930", "name": "삼성전자", "market": "KOSPI", "sector": "IT", "per": 12.5, "pbr": 1.3, "roe": 15.2, "score": 88},
    ]
    return {
        "total": len(sample),
        "strategy": "custom",
        "items": sample
    }


@router.get("/presets")
async def get_preset_strategies():
    """프리셋 전략 목록"""
    return {
        "strategies": [
            {"id": "value", "name": "가치투자 (그레이엄)", "description": "PER<15, PBR<1.5, ROE>10%"},
            {"id": "quality", "name": "퀄리티 (버핏)", "description": "ROE>15%, ROIC>12%"},
            {"id": "growth", "name": "성장투자 (린치)", "description": "매출성장>15%, PEG<1.5"},
            {"id": "dividend", "name": "배당투자", "description": "배당수익률>3%, 연속배당>5년"}
        ]
    }


@router.post("/presets/{strategy_id}")
async def run_preset_strategy(strategy_id: str, limit: int = 20):
    """프리셋 전략 실행"""
    return {
        "strategy": strategy_id,
        "total": 0,
        "items": []
    }


@router.post("/backtest")
async def run_backtest(request: ScreeningRequest, start_year: int = 2020, end_year: int = 2024):
    """백테스트 실행"""
    return {
        "strategy": "custom",
        "period": f"{start_year}-{end_year}",
        "total_return": 45.2,
        "annual_return": 9.8,
        "max_drawdown": -15.3,
        "sharpe_ratio": 1.2,
        "win_rate": 65.0
    }
