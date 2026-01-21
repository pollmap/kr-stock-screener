"""
상장폐지 종목 수집기 (v3.0)
- Survivorship Bias 방지
- 과거 상장폐지 종목 데이터 보존
"""

import pandas as pd
import requests
from datetime import date, datetime
from typing import List, Dict, Optional
import logging

logger = logging.getLogger("kr_stock_collector.delisted")


class DelistedStockCollector:
    """
    상장폐지 종목 수집기
    
    Why?
    - 백테스팅 시 상장폐지 종목을 제외하면 생존 편향 발생
    - 과거 저PER 종목 중 부도/상폐된 종목이 빠지면 수익률 과대포장
    """
    
    KRX_DELISTED_URL = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    
    def __init__(self):
        self.cached_delisted = None
    
    def collect_from_krx(self, start_year: int = 2010) -> pd.DataFrame:
        """
        KRX 상장폐지 종목 수집
        
        TODO: 실제 KRX API 연동 필요
        """
        # 샘플 데이터 (실제 상폐 종목)
        delisted_samples = [
            {'code': '001500', 'name': '현대저축은행', 'market': 'KOSDAQ', 
             'listed_at': '1999-09-01', 'delisted_at': '2012-04-30', 'reason': '부도'},
            {'code': '036570', 'name': '엔씨소프트', 'market': 'KOSDAQ', 
             'listed_at': '2000-11-01', 'delisted_at': None, 'reason': None},  # 정상 유지
            {'code': '019680', 'name': '대교', 'market': 'KOSPI',
             'listed_at': '1987-06-20', 'delisted_at': '2023-12-28', 'reason': '자진상폐'},
        ]
        
        df = pd.DataFrame(delisted_samples)
        df['listed_at'] = pd.to_datetime(df['listed_at']).dt.date
        df['delisted_at'] = pd.to_datetime(df['delisted_at']).dt.date
        
        # 상폐된 것만 필터
        delisted = df[df['delisted_at'].notna()]
        
        logger.info(f"상장폐지 종목 수집: {len(delisted)}개")
        return delisted
    
    def get_delisted_at_date(self, as_of_date: date) -> pd.DataFrame:
        """
        특정 시점까지의 상장폐지 종목
        """
        if self.cached_delisted is None:
            self.cached_delisted = self.collect_from_krx()
        
        return self.cached_delisted[self.cached_delisted['delisted_at'] <= as_of_date]
    
    def was_listed_at(self, stock_code: str, check_date: date) -> bool:
        """
        특정 시점에 상장 상태였는지 확인
        """
        if self.cached_delisted is None:
            self.cached_delisted = self.collect_from_krx()
        
        stock = self.cached_delisted[self.cached_delisted['code'] == stock_code]
        
        if stock.empty:
            return True  # 상폐 기록 없으면 상장 중으로 간주
        
        row = stock.iloc[0]
        listed = row['listed_at']
        delisted = row['delisted_at']
        
        if delisted is None:
            return check_date >= listed
        
        return listed <= check_date < delisted
    
    def get_universe_at(self, as_of_date: date, all_stocks: pd.DataFrame) -> pd.DataFrame:
        """
        특정 시점의 투자 유니버스 반환
        
        Args:
            as_of_date: 기준일
            all_stocks: 전체 종목 (상폐 포함)
        
        Returns:
            해당 시점에 상장되어 있던 종목만
        """
        if all_stocks is None or all_stocks.empty:
            return all_stocks
        
        result = all_stocks.copy()
        
        # delisted_at 컬럼이 있으면 필터
        if 'delisted_at' in result.columns:
            # 상폐 전 또는 상폐일 없음
            mask = (result['delisted_at'].isna()) | (result['delisted_at'] > as_of_date)
            result = result[mask]
        
        # listed_at 컬럼이 있으면 필터
        if 'listed_at' in result.columns:
            result = result[result['listed_at'] <= as_of_date]
        
        logger.debug(f"유니버스 {as_of_date}: {len(all_stocks)} → {len(result)} 종목")
        
        return result


def apply_survivorship_bias_free(
    backtest_data: Dict[str, pd.DataFrame],
    all_stocks_with_delisted: pd.DataFrame
) -> Dict[str, pd.DataFrame]:
    """
    백테스팅 데이터에 상폐 종목 포함
    
    Args:
        backtest_data: {year: DataFrame} 연도별 데이터
        all_stocks_with_delisted: 상폐 종목 포함 전체 종목
    
    Returns:
        Survivorship Bias 제거된 데이터
    """
    collector = DelistedStockCollector()
    result = {}
    
    for year_str, df in backtest_data.items():
        year = int(year_str)
        as_of_date = date(year, 12, 31)
        
        # 해당 시점 유니버스
        universe = collector.get_universe_at(as_of_date, all_stocks_with_delisted)
        
        # 유니버스에 있는 종목만 필터
        code_col = '종목코드' if '종목코드' in df.columns else 'stock_code'
        if code_col in df.columns:
            df_filtered = df[df[code_col].isin(universe['code' if 'code' in universe.columns else 'Code'])]
            result[year_str] = df_filtered
        else:
            result[year_str] = df
    
    return result


if __name__ == '__main__':
    # 테스트
    collector = DelistedStockCollector()
    delisted = collector.collect_from_krx()
    print("상장폐지 종목:")
    print(delisted)
    
    # 2015년 시점 확인
    was_listed = collector.was_listed_at('001500', date(2011, 1, 1))
    print(f"\n현대저축은행 2011년 상장 상태? {was_listed}")  # True
    
    was_listed = collector.was_listed_at('001500', date(2013, 1, 1))
    print(f"현대저축은행 2013년 상장 상태? {was_listed}")  # False
