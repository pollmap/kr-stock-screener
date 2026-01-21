"""
FinanceDataReader 기반 수집기
- KRX 전 종목 리스트
- 주가 히스토리
- 시가총액 데이터
"""

import FinanceDataReader as fdr
import pandas as pd
from typing import List, Optional
from datetime import datetime
import logging

from .base_collector import BaseCollector

logger = logging.getLogger("kr_stock_collector.fdr")


class FDRCollector(BaseCollector):
    """
    FinanceDataReader 기반 주가 데이터 수집기
    
    무료 API로 KRX 전 종목 데이터 수집
    """
    
    def __init__(self, cache_dir: str = "cache"):
        super().__init__(
            name="fdr",
            cache_dir=cache_dir,
            cache_expiry_days=1,  # 주가 데이터는 1일 캐시
            rate_limit_per_minute=300  # 무료 API이므로 제한 느슨
        )
    
    def get_all_stock_list(self, market: str = 'KRX') -> pd.DataFrame:
        """
        전 종목 리스트 조회
        
        Args:
            market: 'KRX'(전체), 'KOSPI', 'KOSDAQ', 'KONEX'
        
        Returns:
            종목 리스트 DataFrame
            - Code: 종목코드
            - Name: 종목명
            - Market: 시장구분
            - Sector: 업종
        """
        cache_key = f"stock_list_{market}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return pd.DataFrame(cached)
        
        try:
            df = fdr.StockListing(market)
            self.logger.info(f"{market} 상장종목 {len(df)}개 조회")
            
            # 캐시 저장
            self._save_to_cache(cache_key, df.to_dict('records'))
            
            return df
            
        except Exception as e:
            self.logger.error(f"종목 리스트 조회 실패: {e}")
            raise
    
    def get_price_history(
        self,
        stock_code: str,
        start: str,
        end: str = None
    ) -> pd.DataFrame:
        """
        개별 종목 주가 히스토리 조회
        
        Args:
            stock_code: 종목코드 (예: '005930')
            start: 시작일 (예: '2020-01-01')
            end: 종료일 (None이면 오늘)
        
        Returns:
            주가 DataFrame (Open, High, Low, Close, Volume, Change)
        """
        if end is None:
            end = datetime.now().strftime('%Y-%m-%d')
        
        cache_key = f"price_{stock_code}_{start}_{end}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            df = pd.DataFrame(cached)
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.set_index('Date')
            return df
        
        try:
            df = fdr.DataReader(stock_code, start, end)
            
            if not df.empty:
                # 캐시 저장 (인덱스 리셋)
                cache_df = df.reset_index()
                cache_df['Date'] = cache_df['Date'].astype(str)
                self._save_to_cache(cache_key, cache_df.to_dict('records'))
            
            return df
            
        except Exception as e:
            self.logger.warning(f"주가 조회 실패 [{stock_code}]: {e}")
            return pd.DataFrame()
    
    def get_all_prices_batch(
        self,
        stock_codes: List[str],
        start: str,
        end: str = None
    ) -> pd.DataFrame:
        """
        전 종목 주가 일괄 조회
        
        Args:
            stock_codes: 종목코드 리스트
            start: 시작일
            end: 종료일
        
        Returns:
            통합 주가 DataFrame
        """
        if end is None:
            end = datetime.now().strftime('%Y-%m-%d')
        
        all_data = []
        total = len(stock_codes)
        
        for i, code in enumerate(stock_codes):
            if (i + 1) % 100 == 0:
                self.logger.info(f"주가 조회 진행: {i+1}/{total}")
            
            try:
                df = self.get_price_history(code, start, end)
                if not df.empty:
                    df = df.reset_index()
                    df['stock_code'] = code
                    all_data.append(df)
            except Exception as e:
                self.logger.warning(f"주가 조회 실패 [{code}]: {e}")
        
        if not all_data:
            return pd.DataFrame()
        
        result = pd.concat(all_data, ignore_index=True)
        self.logger.info(f"총 {len(result)} 행 주가 데이터 수집 완료")
        
        return result
    
    def get_market_cap(self, date: str = None) -> pd.DataFrame:
        """
        시가총액 데이터 조회 (KRX-MARCAP)
        
        Args:
            date: 기준일 (None이면 최신)
        
        Returns:
            시가총액 DataFrame
        """
        cache_key = f"marcap_{date or 'latest'}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return pd.DataFrame(cached)
        
        try:
            df = fdr.StockListing('KRX-MARCAP')
            
            self._save_to_cache(cache_key, df.to_dict('records'))
            self.logger.info(f"시가총액 데이터 {len(df)}개 조회")
            
            return df
            
        except Exception as e:
            self.logger.error(f"시가총액 조회 실패: {e}")
            return pd.DataFrame()
    
    def get_index_data(
        self,
        symbol: str,
        start: str,
        end: str = None
    ) -> pd.DataFrame:
        """
        지수 데이터 조회
        
        Args:
            symbol: 지수 심볼 (예: 'KS11' KOSPI, 'KQ11' KOSDAQ)
            start: 시작일
            end: 종료일
        
        Returns:
            지수 DataFrame
        """
        if end is None:
            end = datetime.now().strftime('%Y-%m-%d')
        
        try:
            df = fdr.DataReader(symbol, start, end)
            return df
            
        except Exception as e:
            self.logger.error(f"지수 조회 실패 [{symbol}]: {e}")
            return pd.DataFrame()
    
    def collect(
        self,
        stock_codes: List[str] = None,
        start: str = '2021-01-01',
        end: str = None
    ) -> pd.DataFrame:
        """BaseCollector 인터페이스 구현"""
        if stock_codes is None:
            stock_list = self.get_all_stock_list('KRX')
            stock_codes = stock_list['Code'].tolist()
        
        return self.get_all_prices_batch(stock_codes, start, end)
