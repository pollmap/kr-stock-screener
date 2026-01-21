"""
수정주가 계산 엔진 (v3.0)
- 액면분할, 무상증자, 배당락 반영
- corporate_actions 테이블 기반
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import date
from dataclasses import dataclass
import logging

logger = logging.getLogger("kr_stock_collector.adjusted_price")


@dataclass
class CorporateAction:
    """기업 이벤트"""
    action_date: date
    action_type: str      # split, dividend, rights, merger
    ratio: float          # 분할비율 (1:10 = 0.1)
    dividend_amount: float = 0


class AdjustedPriceCalculator:
    """
    수정주가 계산 엔진
    
    수정계수(Adjustment Factor) 방식:
    - 액면분할: adj_factor *= split_ratio
    - 배당락: adj_factor *= (price - dividend) / price
    - 무상증자: adj_factor *= rights_ratio
    
    수정주가 = 원주가 * adj_factor
    """
    
    def __init__(self):
        self.actions: List[CorporateAction] = []
    
    def add_action(self, action: CorporateAction):
        """이벤트 추가"""
        self.actions.append(action)
        # 날짜순 정렬
        self.actions.sort(key=lambda x: x.action_date)
    
    def add_actions_from_df(self, df: pd.DataFrame):
        """DataFrame에서 이벤트 로드"""
        for _, row in df.iterrows():
            self.add_action(CorporateAction(
                action_date=row['action_date'],
                action_type=row['action_type'],
                ratio=row.get('ratio', 1.0),
                dividend_amount=row.get('dividend_amount', 0)
            ))
    
    def calculate_adjustment_factors(
        self, 
        prices: pd.DataFrame,
        date_col: str = 'date',
        price_col: str = 'close'
    ) -> pd.DataFrame:
        """
        수정계수 계산
        
        역순 누적 방식:
        - 가장 최근 → 과거로 가면서 누적
        - 최신 데이터의 adj_factor = 1.0
        """
        result = prices.copy()
        result['adj_factor'] = 1.0
        
        for action in reversed(self.actions):
            mask = result[date_col] < action.action_date
            
            if action.action_type == 'split':
                # 액면분할: 과거 주가에 분할비율 곱함
                result.loc[mask, 'adj_factor'] *= action.ratio
                logger.info(f"액면분할 반영: {action.action_date}, 비율: {action.ratio}")
                
            elif action.action_type == 'dividend':
                # 배당락: (주가-배당금)/주가
                if action.dividend_amount > 0:
                    # 배당락일 직전 종가 기준
                    ex_date_mask = result[date_col] == action.action_date
                    if ex_date_mask.any():
                        ref_price = result.loc[ex_date_mask, price_col].iloc[0]
                        if ref_price > 0:
                            div_factor = (ref_price - action.dividend_amount) / ref_price
                            result.loc[mask, 'adj_factor'] *= div_factor
                            logger.info(f"배당락 반영: {action.action_date}, 배당: {action.dividend_amount}")
                
            elif action.action_type == 'rights':
                # 무상증자
                result.loc[mask, 'adj_factor'] *= action.ratio
                logger.info(f"무상증자 반영: {action.action_date}, 비율: {action.ratio}")
        
        # 수정주가 계산
        result['adj_close'] = result[price_col] * result['adj_factor']
        
        return result
    
    def adjust_prices(
        self,
        prices: pd.DataFrame,
        date_col: str = 'date'
    ) -> pd.DataFrame:
        """
        전체 수정주가 계산 (OHLCV)
        """
        result = self.calculate_adjustment_factors(prices, date_col, 'close')
        
        # OHLC 모두 수정
        for col in ['open', 'high', 'low']:
            if col in result.columns:
                result[f'adj_{col}'] = result[col] * result['adj_factor']
        
        # 거래량은 역수 적용 (주식 수 증가)
        if 'volume' in result.columns:
            result['adj_volume'] = result['volume'] / result['adj_factor']
        
        return result


def collect_corporate_actions(stock_code: str, start_date: str = '2015-01-01') -> pd.DataFrame:
    """
    기업 이벤트 수집 (DART/KRX)
    
    TODO: 실제 API 연동
    """
    # 샘플 데이터
    sample_actions = {
        '005930': [  # 삼성전자
            {'action_date': date(2018, 5, 4), 'action_type': 'split', 'ratio': 0.02, 'dividend_amount': 0},  # 1:50 액면분할
        ],
        '035720': [  # 카카오
            {'action_date': date(2021, 4, 15), 'action_type': 'split', 'ratio': 0.2, 'dividend_amount': 0},  # 1:5 액면분할
        ]
    }
    
    if stock_code in sample_actions:
        return pd.DataFrame(sample_actions[stock_code])
    
    return pd.DataFrame()


def get_adjusted_prices(
    stock_code: str,
    raw_prices: pd.DataFrame,
    corporate_actions: pd.DataFrame = None
) -> pd.DataFrame:
    """
    수정주가 조회 (메인 함수)
    
    Args:
        stock_code: 종목코드
        raw_prices: 원 주가 DataFrame (date, open, high, low, close, volume)
        corporate_actions: 기업 이벤트 DataFrame (optional)
    
    Returns:
        수정주가가 포함된 DataFrame
    """
    if raw_prices is None or raw_prices.empty:
        return raw_prices
    
    calculator = AdjustedPriceCalculator()
    
    # 기업 이벤트 로드
    if corporate_actions is None or corporate_actions.empty:
        corporate_actions = collect_corporate_actions(stock_code)
    
    if not corporate_actions.empty:
        calculator.add_actions_from_df(corporate_actions)
    
    # 수정주가 계산
    result = calculator.adjust_prices(raw_prices)
    
    logger.info(f"[{stock_code}] 수정주가 계산 완료: {len(result)}개 데이터, {len(calculator.actions)}개 이벤트")
    
    return result


if __name__ == '__main__':
    # 테스트
    test_prices = pd.DataFrame({
        'date': pd.date_range('2018-01-01', periods=10),
        'close': [2500000, 2510000, 2490000, 2520000, 50000, 51000, 52000, 51500, 52500, 53000],  # 5/4 액면분할
        'volume': [1000000] * 10
    })
    
    result = get_adjusted_prices('005930', test_prices)
    print(result[['date', 'close', 'adj_factor', 'adj_close']])
