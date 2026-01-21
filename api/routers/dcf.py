"""
DCF Router - DCF/RIM 밸류에이션 API
"""

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, List

router = APIRouter()


class DCFRequest(BaseModel):
    fcf: float                    # 현재 FCF (억원)
    net_debt: float = 0           # 순부채
    shares: int                   # 발행주식수
    growth_phase1: float = 0.10   # 1~5년 성장률
    growth_phase2: float = 0.05   # 6~10년 성장률
    wacc: float = 0.10            # 할인율
    terminal_growth: float = 0.02 # 영구성장률


class DCFResponse(BaseModel):
    fair_value: float             # 적정주가
    enterprise_value: float       # 기업가치
    equity_value: float           # 주주가치
    upside: Optional[float] = None # 상승여력


class RIMRequest(BaseModel):
    book_value: float             # 자기자본
    roe: float                    # ROE
    cost_of_equity: float = 0.10  # 자기자본비용
    shares: int                   # 발행주식수


@router.post("/calculate", response_model=DCFResponse)
async def calculate_dcf(request: DCFRequest):
    """DCF 내재가치 계산"""
    from analyzers import DCFCalculator
    
    calc = DCFCalculator(wacc=request.wacc, terminal_growth=request.terminal_growth)
    result = calc.calculate_fair_value(
        request.fcf,
        request.net_debt,
        request.shares,
        growth_phase1=request.growth_phase1,
        growth_phase2=request.growth_phase2
    )
    
    return DCFResponse(
        fair_value=result.get('fair_value', 0),
        enterprise_value=result.get('enterprise_value', 0),
        equity_value=result.get('equity_value', 0)
    )


@router.post("/rim/calculate")
async def calculate_rim(request: RIMRequest):
    """RIM 내재가치 계산"""
    # RIM = BV + Σ(Residual Income / (1+r)^n)
    excess_return = request.roe - request.cost_of_equity
    rim_value = request.book_value
    
    for year in range(1, 11):
        residual_income = request.book_value * excess_return
        pv = residual_income / ((1 + request.cost_of_equity) ** year)
        rim_value += pv
    
    fair_value = (rim_value * 100_000_000) / request.shares
    
    return {
        "rim_value": rim_value,
        "fair_value": round(fair_value, 0)
    }


@router.post("/sensitivity")
async def sensitivity_analysis(request: DCFRequest):
    """민감도 분석"""
    from analyzers import DCFCalculator
    
    calc = DCFCalculator()
    result = calc.sensitivity_analysis(
        request.fcf,
        request.net_debt,
        request.shares
    )
    
    return {"matrix": result.to_dict()}


@router.get("/{code}/auto")
async def auto_dcf_for_stock(code: str):
    """종목 자동 DCF (재무데이터 기반)"""
    # TODO: DB에서 FCF, 순부채, 주식수 조회
    return {
        "code": code,
        "auto_dcf": {
            "fair_value": 75000,
            "current_price": 55000,
            "upside": 36.4
        }
    }
