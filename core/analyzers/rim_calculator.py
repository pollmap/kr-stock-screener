"""
RIM 계산기 (잔여이익모형, v3.0)
- 한국 시장 적합 밸류에이션 모델
- ROE 기반 초과이익 산출
"""

import numpy as np
from typing import Dict, Optional
import logging

logger = logging.getLogger("kr_stock_collector.rim")


class RIMCalculator:
    """
    RIM (Residual Income Model) 계산기
    
    RIM 공식:
    V = BV + Σ[(ROE - r) × BV / (1 + r)^t]
    
    Where:
    - V: 내재가치
    - BV: 자기자본 (Book Value)
    - ROE: 자기자본이익률
    - r: 요구수익률 (자기자본비용)
    """
    
    # 한국 시장 기본 파라미터
    DEFAULT_COST_OF_EQUITY = 0.10    # 10% (무위험 + 리스크 프리미엄)
    DEFAULT_PROJECTION_YEARS = 10
    DEFAULT_FADE_RATE = 0.10         # ROE 퇴색률 (매년 10% 감소)
    
    def __init__(
        self,
        cost_of_equity: float = None,
        projection_years: int = 10,
        fade_rate: float = None
    ):
        self.cost_of_equity = cost_of_equity or self.DEFAULT_COST_OF_EQUITY
        self.projection_years = projection_years
        self.fade_rate = fade_rate or self.DEFAULT_FADE_RATE
    
    def calculate_rim_value(
        self,
        book_value: float,           # 자기자본 (억원)
        roe: float,                   # ROE (%, 예: 15.0)
        cost_of_equity: float = None, # 요구수익률 (비율)
        growth_rate: float = 0,       # 자기자본 성장률
        fade_to_market: bool = True   # ROE가 시장 평균으로 수렴
    ) -> Dict:
        """
        RIM 내재가치 계산
        
        Args:
            book_value: 자기자본 (억원)
            roe: 자기자본이익률 (%, 예: 15.0 = 15%)
            cost_of_equity: 요구수익률 (비율, 예: 0.10 = 10%)
            growth_rate: 자기자본 연간 성장률
            fade_to_market: ROE가 점진적으로 시장 평균으로 수렴
        
        Returns:
            RIM 밸류에이션 결과
        """
        r = cost_of_equity or self.cost_of_equity
        roe_decimal = roe / 100 if roe > 1 else roe  # % → 비율
        
        # 초과이익 = (ROE - 요구수익률) × 자기자본
        total_residual_income_pv = 0
        current_bv = book_value
        current_roe = roe_decimal
        
        ri_projections = []
        
        for year in range(1, self.projection_years + 1):
            # ROE Fade (시장 평균으로 수렴)
            if fade_to_market and current_roe > r:
                current_roe = r + (current_roe - r) * (1 - self.fade_rate)
            
            # 잔여이익
            residual_income = (current_roe - r) * current_bv
            
            # 현재가치
            pv = residual_income / ((1 + r) ** year)
            total_residual_income_pv += pv
            
            ri_projections.append({
                'year': year,
                'roe': current_roe * 100,
                'bv': current_bv,
                'ri': residual_income,
                'pv': pv
            })
            
            # 자기자본 성장
            current_bv *= (1 + growth_rate)
        
        # RIM 가치 = 현재 BV + 잔여이익 PV 합계
        rim_value = book_value + total_residual_income_pv
        
        return {
            'book_value': book_value,
            'roe': roe,
            'cost_of_equity': r * 100,
            'rim_value': rim_value,
            'residual_income_pv': total_residual_income_pv,
            'premium_to_bv': (rim_value / book_value - 1) * 100 if book_value > 0 else 0,
            'projections': ri_projections
        }
    
    def calculate_fair_value(
        self,
        book_value: float,
        roe: float,
        shares_outstanding: int,
        cost_of_equity: float = None
    ) -> Dict:
        """
        주당 적정가치 계산
        
        Args:
            book_value: 자기자본 (억원)
            roe: ROE (%)
            shares_outstanding: 발행주식수 (주)
            cost_of_equity: 요구수익률
        
        Returns:
            적정주가 포함 결과
        """
        result = self.calculate_rim_value(
            book_value, roe, cost_of_equity
        )
        
        # 주당 가치 (억원 → 원)
        if shares_outstanding > 0 and result['rim_value'] > 0:
            fair_value = (result['rim_value'] * 100_000_000) / shares_outstanding
            result['fair_value'] = round(fair_value, 0)
            result['shares_outstanding'] = shares_outstanding
        else:
            result['fair_value'] = None
        
        return result
    
    def compare_with_price(
        self,
        book_value: float,
        roe: float,
        shares_outstanding: int,
        current_price: float,
        cost_of_equity: float = None
    ) -> Dict:
        """
        현재 주가와 비교
        
        Returns:
            upside, margin_of_safety 포함
        """
        result = self.calculate_fair_value(
            book_value, roe, shares_outstanding, cost_of_equity
        )
        
        if result.get('fair_value') and current_price > 0:
            fair = result['fair_value']
            result['current_price'] = current_price
            result['upside'] = (fair - current_price) / current_price * 100
            result['margin_of_safety'] = (fair - current_price) / fair * 100
            
            if result['upside'] > 30:
                result['signal'] = '강력매수'
            elif result['upside'] > 10:
                result['signal'] = '매수'
            elif result['upside'] > -10:
                result['signal'] = '중립'
            else:
                result['signal'] = '매도'
        
        return result


def auto_rim_valuation(stock_data: Dict, market_data: Dict = None) -> Dict:
    """
    자동 RIM 밸류에이션
    
    Args:
        stock_data: {book_value, roe, shares}
        market_data: {current_price}
    """
    calculator = RIMCalculator()
    
    result = calculator.compare_with_price(
        book_value=stock_data.get('book_value', 0),
        roe=stock_data.get('roe', 0),
        shares_outstanding=stock_data.get('shares', 1),
        current_price=market_data.get('current_price', 0) if market_data else 0
    )
    
    return result


if __name__ == '__main__':
    # 테스트: 삼성전자
    calc = RIMCalculator()
    
    result = calc.compare_with_price(
        book_value=3000000,           # 300조원 자기자본
        roe=15.0,                      # ROE 15%
        shares_outstanding=5_900_000_000,  # 59억주
        current_price=55000            # 현재가 55,000원
    )
    
    print(f"RIM 적정주가: {result.get('fair_value'):,.0f}원")
    print(f"상승여력: {result.get('upside'):.1f}%")
    print(f"투자의견: {result.get('signal')}")
