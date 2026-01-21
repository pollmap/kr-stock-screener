"""
백테스팅 MVP 모듈 (v2.0)
- 단순 스크리닝 전략 백테스트
- 연도별 수익률 시뮬레이션
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Callable, Optional
from datetime import datetime, date
import logging

logger = logging.getLogger("kr_stock_collector.backtester")


class SimpleBacktester:
    """단순 백테스터 (연간 리밸런싱)"""
    
    def __init__(
        self,
        initial_capital: float = 100_000_000,  # 1억원
        max_positions: int = 20,                # 최대 종목수
        rebalance_freq: str = 'yearly'          # yearly/quarterly
    ):
        self.initial_capital = initial_capital
        self.max_positions = max_positions
        self.rebalance_freq = rebalance_freq
    
    def run(
        self,
        screening_func: Callable[[pd.DataFrame], pd.DataFrame],
        historical_data: Dict[str, pd.DataFrame],
        start_year: int = 2020,
        end_year: int = 2024
    ) -> Dict:
        """
        백테스트 실행
        
        Args:
            screening_func: 스크리닝 함수 (DataFrame → 선택된 종목 DataFrame)
            historical_data: {year: DataFrame} 각 연도별 재무/주가 데이터
            start_year: 시작 연도
            end_year: 종료 연도
        
        Returns:
            백테스트 결과 딕셔너리
        """
        results = {
            'years': [],
            'returns': [],
            'holdings': [],
            'cumulative': [1.0]
        }
        
        for year in range(start_year, end_year):
            if str(year) not in historical_data:
                logger.warning(f"데이터 없음: {year}년")
                continue
            
            year_data = historical_data[str(year)]
            
            # 스크리닝
            try:
                selected = screening_func(year_data)
                if selected is None or selected.empty:
                    logger.warning(f"{year}년 선택 종목 없음")
                    continue
                
                # 상위 N개 선택
                selected = selected.head(self.max_positions)
                holdings = selected['종목코드'].tolist() if '종목코드' in selected.columns else []
                
            except Exception as e:
                logger.error(f"{year}년 스크리닝 오류: {e}")
                continue
            
            # 다음 연도 수익률 계산
            next_year = str(year + 1)
            if next_year not in historical_data:
                continue
            
            next_data = historical_data[next_year]
            
            # 수익률 계산 (단순 평균)
            returns = []
            for code in holdings:
                code_col = '종목코드' if '종목코드' in next_data.columns else 'stock_code'
                stock = next_data[next_data[code_col] == code]
                
                if not stock.empty and 'return' in stock.columns:
                    returns.append(stock['return'].iloc[0])
            
            if returns:
                avg_return = np.mean(returns)
            else:
                avg_return = 0
            
            results['years'].append(year)
            results['returns'].append(avg_return)
            results['holdings'].append(holdings)
            results['cumulative'].append(
                results['cumulative'][-1] * (1 + avg_return)
            )
        
        # 성과 지표 계산
        if results['returns']:
            results['total_return'] = (results['cumulative'][-1] - 1) * 100
            results['annual_return'] = np.mean(results['returns']) * 100
            results['volatility'] = np.std(results['returns']) * 100
            results['sharpe_ratio'] = (
                np.mean(results['returns']) / np.std(results['returns'])
                if np.std(results['returns']) > 0 else 0
            )
            results['max_drawdown'] = self._calculate_mdd(results['cumulative'])
            results['win_rate'] = sum(1 for r in results['returns'] if r > 0) / len(results['returns']) * 100
        
        return results
    
    def _calculate_mdd(self, cumulative: list) -> float:
        """최대 낙폭 계산"""
        peak = cumulative[0]
        max_dd = 0
        
        for value in cumulative:
            if value > peak:
                peak = value
            dd = (peak - value) / peak
            if dd > max_dd:
                max_dd = dd
        
        return max_dd * 100


def value_strategy(df: pd.DataFrame) -> pd.DataFrame:
    """그레이엄 스타일 가치투자 전략"""
    if df is None or df.empty:
        return pd.DataFrame()
    
    result = df.copy()
    
    # 필터 조건
    conditions = pd.Series([True] * len(result))
    
    if 'PER' in result.columns:
        conditions &= (result['PER'] > 0) & (result['PER'] < 15)
    
    if 'PBR' in result.columns:
        conditions &= (result['PBR'] > 0) & (result['PBR'] < 1.5)
    
    if 'ROE(%)' in result.columns:
        conditions &= (result['ROE(%)'] > 10)
    
    if '부채비율(%)' in result.columns:
        conditions &= (result['부채비율(%)'] < 100)
    
    filtered = result[conditions]
    
    # PER + PBR 순위로 정렬
    if 'PER' in filtered.columns and 'PBR' in filtered.columns:
        filtered = filtered.copy()
        filtered['composite_rank'] = filtered['PER'].rank() + filtered['PBR'].rank()
        filtered = filtered.sort_values('composite_rank')
    
    return filtered


def quality_strategy(df: pd.DataFrame) -> pd.DataFrame:
    """버핏 스타일 퀄리티 전략"""
    if df is None or df.empty:
        return pd.DataFrame()
    
    result = df.copy()
    
    conditions = pd.Series([True] * len(result))
    
    if 'ROE(%)' in result.columns:
        conditions &= (result['ROE(%)'] > 15)
    
    if 'ROIC(%)' in result.columns:
        conditions &= (result['ROIC(%)'] > 12)
    
    if '영업이익률(%)' in result.columns:
        conditions &= (result['영업이익률(%)'] > 10)
    
    if 'OCF/순이익' in result.columns:
        conditions &= (result['OCF/순이익'] > 0.8)
    
    filtered = result[conditions]
    
    if 'ROE(%)' in filtered.columns:
        filtered = filtered.sort_values('ROE(%)', ascending=False)
    
    return filtered


def growth_strategy(df: pd.DataFrame) -> pd.DataFrame:
    """피터 린치 스타일 성장 전략"""
    if df is None or df.empty:
        return pd.DataFrame()
    
    result = df.copy()
    
    conditions = pd.Series([True] * len(result))
    
    if '매출성장률(%)' in result.columns:
        conditions &= (result['매출성장률(%)'] > 15)
    
    if '영업이익성장률(%)' in result.columns:
        conditions &= (result['영업이익성장률(%)'] > 15)
    
    if 'PER' in result.columns and '순이익성장률(%)' in result.columns:
        # PEG < 1
        result['PEG'] = result['PER'] / result['순이익성장률(%)'].clip(lower=1)
        conditions &= (result['PEG'] < 1.5)
    
    filtered = result[conditions]
    
    if '매출성장률(%)' in filtered.columns:
        filtered = filtered.sort_values('매출성장률(%)', ascending=False)
    
    return filtered


# 전략 레지스트리
STRATEGIES = {
    'value': {
        'name': '가치투자 (그레이엄)',
        'func': value_strategy,
        'description': 'PER<15, PBR<1.5, ROE>10%, 부채<100%'
    },
    'quality': {
        'name': '퀄리티 (버핏)',
        'func': quality_strategy,
        'description': 'ROE>15%, ROIC>12%, 영업이익률>10%'
    },
    'growth': {
        'name': '성장투자 (린치)',
        'func': growth_strategy,
        'description': '매출성장>15%, PEG<1.5'
    }
}


if __name__ == '__main__':
    # 테스트 데이터
    test_data = pd.DataFrame({
        '종목코드': ['005930', '000660', '035420'],
        'PER': [10, 8, 35],
        'PBR': [1.2, 0.9, 4.0],
        'ROE(%)': [15, 22, 12],
        '부채비율(%)': [30, 50, 40]
    })
    
    # 가치 전략 테스트
    selected = value_strategy(test_data)
    print("가치전략 선택 종목:")
    print(selected)
