"""
DCF 자동 계산 모듈 (v2.0)
- 과거 FCF 기반 1차 추정
- 민감도 분석 (WACC, 성장률)
- 적정 주가 산출
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple
import logging

logger = logging.getLogger("kr_stock_collector.dcf")


class DCFCalculator:
    """DCF 자동 계산기"""
    
    # 기본 가정
    DEFAULT_WACC = 0.10          # 10%
    DEFAULT_TERMINAL_GROWTH = 0.02  # 2% (GDP 성장률 수준)
    DEFAULT_PROJECTION_YEARS = 10
    
    def __init__(
        self,
        wacc: float = None,
        terminal_growth: float = None,
        projection_years: int = 10
    ):
        self.wacc = wacc or self.DEFAULT_WACC
        self.terminal_growth = terminal_growth or self.DEFAULT_TERMINAL_GROWTH
        self.projection_years = projection_years
    
    def estimate_growth_rate(
        self,
        historical_fcf: list,
        method: str = 'cagr'
    ) -> float:
        """
        과거 FCF 기반 성장률 추정
        
        Args:
            historical_fcf: 과거 FCF 리스트 (오래된 순)
            method: 'cagr' 또는 'average'
        """
        if not historical_fcf or len(historical_fcf) < 2:
            return 0.05  # 기본값 5%
        
        # 양수 값만 사용
        positive_fcf = [f for f in historical_fcf if f and f > 0]
        if len(positive_fcf) < 2:
            return 0.05
        
        if method == 'cagr':
            # CAGR 계산
            start = positive_fcf[0]
            end = positive_fcf[-1]
            years = len(positive_fcf) - 1
            
            if start > 0 and end > 0:
                cagr = (end / start) ** (1 / years) - 1
                return max(min(cagr, 0.30), -0.10)  # -10% ~ 30% 제한
        
        # 평균 성장률
        growth_rates = []
        for i in range(1, len(positive_fcf)):
            if positive_fcf[i-1] > 0:
                rate = (positive_fcf[i] - positive_fcf[i-1]) / positive_fcf[i-1]
                growth_rates.append(rate)
        
        if growth_rates:
            return max(min(np.median(growth_rates), 0.30), -0.10)
        
        return 0.05
    
    def project_fcf(
        self,
        base_fcf: float,
        growth_phase1: float = 0.10,   # 1~5년 성장률
        growth_phase2: float = 0.05,   # 6~10년 성장률
        years: int = 10
    ) -> list:
        """FCF 성장 추정"""
        fcf_projections = []
        current_fcf = base_fcf
        
        for year in range(1, years + 1):
            if year <= 5:
                growth = growth_phase1
            else:
                growth = growth_phase2
            
            current_fcf = current_fcf * (1 + growth)
            fcf_projections.append(current_fcf)
        
        return fcf_projections
    
    def calculate_pv(self, cash_flows: list, discount_rate: float) -> float:
        """현재가치 계산"""
        pv = 0
        for i, cf in enumerate(cash_flows, 1):
            pv += cf / ((1 + discount_rate) ** i)
        return pv
    
    def calculate_terminal_value(
        self,
        final_fcf: float,
        growth_rate: float = None,
        wacc: float = None
    ) -> float:
        """영구가치 계산 (Gordon Growth Model)"""
        g = growth_rate or self.terminal_growth
        r = wacc or self.wacc
        
        if r <= g:
            return 0  # 무한대 방지
        
        return final_fcf * (1 + g) / (r - g)
    
    def calculate_ev(
        self,
        base_fcf: float,
        growth_phase1: float = 0.10,
        growth_phase2: float = 0.05,
        wacc: float = None,
        terminal_growth: float = None
    ) -> Dict:
        """
        기업가치(Enterprise Value) 계산
        
        Returns:
            Dict with EV breakdown
        """
        wacc = wacc or self.wacc
        terminal_growth = terminal_growth or self.terminal_growth
        
        # 1. FCF 추정
        fcf_projections = self.project_fcf(
            base_fcf, growth_phase1, growth_phase2, self.projection_years
        )
        
        # 2. DCF 가치 (1~10년)
        dcf_value = self.calculate_pv(fcf_projections, wacc)
        
        # 3. 영구가치
        terminal_value = self.calculate_terminal_value(
            fcf_projections[-1], terminal_growth, wacc
        )
        
        # 영구가치의 현재가치
        terminal_pv = terminal_value / ((1 + wacc) ** self.projection_years)
        
        # 4. 기업가치
        enterprise_value = dcf_value + terminal_pv
        
        return {
            'base_fcf': base_fcf,
            'growth_phase1': growth_phase1,
            'growth_phase2': growth_phase2,
            'wacc': wacc,
            'terminal_growth': terminal_growth,
            'dcf_value': dcf_value,
            'terminal_value': terminal_value,
            'terminal_pv': terminal_pv,
            'enterprise_value': enterprise_value,
            'fcf_projections': fcf_projections
        }
    
    def calculate_fair_value(
        self,
        base_fcf: float,
        net_debt: float,           # 순부채 (차입금 - 현금)
        shares_outstanding: int,    # 발행주식수
        growth_phase1: float = None,
        growth_phase2: float = None,
        wacc: float = None
    ) -> Dict:
        """
        주당 적정가치 계산
        
        Args:
            base_fcf: 기준 FCF (억원)
            net_debt: 순부채 (억원)
            shares_outstanding: 발행주식수 (주)
        """
        if base_fcf <= 0 or shares_outstanding <= 0:
            return {'fair_value': None, 'error': 'Invalid inputs'}
        
        # 성장률 미지정 시 기본값
        g1 = growth_phase1 if growth_phase1 is not None else 0.10
        g2 = growth_phase2 if growth_phase2 is not None else 0.05
        
        # EV 계산
        ev_result = self.calculate_ev(base_fcf, g1, g2, wacc)
        
        # 주주가치 = EV - 순부채
        equity_value = ev_result['enterprise_value'] - net_debt
        
        # 주당 가치 (억원 → 원 변환, 주식수 조정)
        fair_value_per_share = (equity_value * 100_000_000) / shares_outstanding
        
        return {
            **ev_result,
            'net_debt': net_debt,
            'equity_value': equity_value,
            'shares_outstanding': shares_outstanding,
            'fair_value': round(fair_value_per_share, 0)
        }
    
    def sensitivity_analysis(
        self,
        base_fcf: float,
        net_debt: float,
        shares_outstanding: int,
        wacc_range: Tuple[float, float] = (0.08, 0.12),
        growth_range: Tuple[float, float] = (0.02, 0.06)
    ) -> pd.DataFrame:
        """
        민감도 분석 (WACC × 영구성장률)
        """
        wacc_values = np.linspace(wacc_range[0], wacc_range[1], 5)
        growth_values = np.linspace(growth_range[0], growth_range[1], 5)
        
        results = []
        for wacc in wacc_values:
            for g in growth_values:
                fv = self.calculate_fair_value(
                    base_fcf, net_debt, shares_outstanding,
                    wacc=wacc, growth_phase2=g
                )
                results.append({
                    'WACC': f"{wacc:.1%}",
                    'Terminal_Growth': f"{g:.1%}",
                    'Fair_Value': fv.get('fair_value', 0)
                })
        
        df = pd.DataFrame(results)
        # 피벗 테이블로 변환
        pivot = df.pivot(
            index='WACC',
            columns='Terminal_Growth',
            values='Fair_Value'
        )
        
        return pivot


def auto_dcf_valuation(financial_data: Dict, market_data: Dict = None) -> Dict:
    """
    자동 DCF 밸류에이션
    
    Args:
        financial_data: {fcf, net_debt, shares, historical_fcf}
        market_data: {current_price}
    """
    calculator = DCFCalculator()
    
    base_fcf = financial_data.get('fcf', 0)
    net_debt = financial_data.get('net_debt', 0)
    shares = financial_data.get('shares', 1)
    historical_fcf = financial_data.get('historical_fcf', [])
    
    # 성장률 자동 추정
    growth_rate = calculator.estimate_growth_rate(historical_fcf)
    
    # 적정가치 계산
    result = calculator.calculate_fair_value(
        base_fcf, net_debt, shares,
        growth_phase1=min(growth_rate * 1.5, 0.25),  # 1~5년: 1.5배
        growth_phase2=min(growth_rate, 0.10)         # 6~10년: 원래값
    )
    
    # 현재 주가 대비 할인율
    if market_data and 'current_price' in market_data and result.get('fair_value'):
        current = market_data['current_price']
        fair = result['fair_value']
        result['current_price'] = current
        result['upside'] = (fair - current) / current * 100
        result['margin_of_safety'] = (fair - current) / fair * 100
    
    return result


if __name__ == '__main__':
    # 테스트: 삼성전자
    calc = DCFCalculator()
    
    result = calc.calculate_fair_value(
        base_fcf=5000,           # 5000억원 FCF
        net_debt=-30000,         # -3조원 (순현금)
        shares_outstanding=5_900_000_000,  # 59억주
        growth_phase1=0.10,
        growth_phase2=0.05
    )
    
    print(f"적정주가: {result.get('fair_value'):,.0f}원")
    
    # 민감도 분석
    sensitivity = calc.sensitivity_analysis(
        base_fcf=5000,
        net_debt=-30000,
        shares_outstanding=5_900_000_000
    )
    print("\n민감도 분석:")
    print(sensitivity)
