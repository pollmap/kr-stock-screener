"""
pykrx 기반 KRX 데이터 수집기
- 일별 전 종목 시세
- 투자지표 (PER, PBR, 배당수익률)
- 지수 데이터
- 시가총액
"""

from pykrx import stock
import pandas as pd
from typing import Optional, List
from datetime import datetime, timedelta
import logging

from .base_collector import BaseCollector

logger = logging.getLogger("kr_stock_collector.pykrx")


class PyKrxCollector(BaseCollector):
    """
    pykrx 기반 KRX 데이터 수집기
    
    KRX에서 직접 시세, 투자지표, 지수 데이터 수집
    무료 API, 호출 제한 없음
    """
    
    # 주요 지수 코드
    INDEX_CODES = {
        'KOSPI': '1001',
        'KOSPI200': '1028',
        'KOSDAQ': '2001',
        'KOSDAQ150': '2203'
    }
    
    def __init__(self, cache_dir: str = "cache"):
        super().__init__(
            name="pykrx",
            cache_dir=cache_dir,
            cache_expiry_days=1,
            rate_limit_per_minute=300
        )
    
    def _get_valid_date(self, date: str = None) -> str:
        """유효한 거래일 반환 (주말/공휴일 회피)"""
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        
        # 날짜 형식 통일 (YYYYMMDD)
        date = date.replace('-', '')
        
        # 주말이면 금요일로
        dt = datetime.strptime(date, '%Y%m%d')
        if dt.weekday() == 5:  # 토요일
            dt = dt - timedelta(days=1)
        elif dt.weekday() == 6:  # 일요일
            dt = dt - timedelta(days=2)
        
        return dt.strftime('%Y%m%d')
    
    def get_market_ohlcv(
        self,
        date: str = None,
        market: str = "ALL"
    ) -> pd.DataFrame:
        """
        특정일 전 종목 시세 조회
        
        Args:
            date: 날짜 (YYYYMMDD 또는 YYYY-MM-DD)
            market: 'KOSPI', 'KOSDAQ', 'KONEX', 'ALL'
        
        Returns:
            DataFrame with columns:
            - stock_code, open, high, low, close, volume, value, change, date
        """
        date = self._get_valid_date(date)
        
        cache_key = f"ohlcv_{date}_{market}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return pd.DataFrame(cached)
        
        try:
            df = stock.get_market_ohlcv(date, market=market)
            
            if df.empty:
                self.logger.warning(f"시세 데이터 없음: {date}")
                return pd.DataFrame()
            
            df = df.reset_index()
            df.columns = ['stock_code', 'open', 'high', 'low', 'close', 
                          'volume', 'value', 'change']
            df['date'] = date
            
            self._save_to_cache(cache_key, df.to_dict('records'))
            self.logger.info(f"{date} 시세 {len(df)}개 종목 조회")
            
            return df
            
        except Exception as e:
            self.logger.error(f"시세 조회 실패 [{date}]: {e}")
            return pd.DataFrame()
    
    def get_market_fundamental(
        self,
        date: str = None,
        market: str = "ALL"
    ) -> pd.DataFrame:
        """
        전 종목 투자지표 조회 (PER, PBR, 배당수익률)
        
        ⚠️ 이 함수가 핵심! OpenDART에서 얻기 어려운 실시간 지표
        
        Args:
            date: 날짜 (YYYYMMDD)
            market: 시장 구분
        
        Returns:
            DataFrame with columns:
            - stock_code, bps, per, pbr, eps, div_yield, dps, date
        """
        date = self._get_valid_date(date)
        
        cache_key = f"fundamental_{date}_{market}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return pd.DataFrame(cached)
        
        try:
            df = stock.get_market_fundamental(date, market=market)
            
            if df.empty:
                self.logger.warning(f"투자지표 데이터 없음: {date}")
                return pd.DataFrame()
            
            df = df.reset_index()
            df.columns = ['stock_code', 'bps', 'per', 'pbr', 
                          'eps', 'div_yield', 'dps']
            df['date'] = date
            
            self._save_to_cache(cache_key, df.to_dict('records'))
            self.logger.info(f"{date} 투자지표 {len(df)}개 종목 조회")
            
            return df
            
        except Exception as e:
            self.logger.error(f"투자지표 조회 실패 [{date}]: {e}")
            return pd.DataFrame()
    
    def get_market_cap(
        self,
        date: str = None,
        market: str = "ALL"
    ) -> pd.DataFrame:
        """
        전 종목 시가총액 조회
        
        Args:
            date: 날짜 (YYYYMMDD)
            market: 시장 구분
        
        Returns:
            DataFrame with columns:
            - stock_code, market_cap, volume, value, shares, date
        """
        date = self._get_valid_date(date)
        
        cache_key = f"cap_{date}_{market}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return pd.DataFrame(cached)
        
        try:
            df = stock.get_market_cap(date, market=market)
            
            if df.empty:
                return pd.DataFrame()
            
            df = df.reset_index()
            df.columns = ['stock_code', 'market_cap', 'volume', 
                          'value', 'shares']
            df['date'] = date
            
            self._save_to_cache(cache_key, df.to_dict('records'))
            
            return df
            
        except Exception as e:
            self.logger.error(f"시가총액 조회 실패 [{date}]: {e}")
            return pd.DataFrame()
    
    def get_index_ohlcv(
        self,
        ticker: str,
        start: str,
        end: str = None
    ) -> pd.DataFrame:
        """
        지수 시세 조회
        
        Args:
            ticker: 지수 코드 (예: '1001' KOSPI, '2001' KOSDAQ)
            start: 시작일 (YYYYMMDD)
            end: 종료일
        
        Returns:
            지수 시세 DataFrame
        """
        start = start.replace('-', '')
        if end is None:
            end = datetime.now().strftime('%Y%m%d')
        else:
            end = end.replace('-', '')
        
        try:
            df = stock.get_index_ohlcv(start, end, ticker)
            df = df.reset_index()
            df['ticker'] = ticker
            return df
            
        except Exception as e:
            self.logger.error(f"지수 시세 조회 실패 [{ticker}]: {e}")
            return pd.DataFrame()
    
    def get_index_fundamental(
        self,
        ticker: str,
        start: str,
        end: str = None
    ) -> pd.DataFrame:
        """
        지수 투자지표 조회 (PER, PBR, 배당수익률)
        """
        start = start.replace('-', '')
        if end is None:
            end = datetime.now().strftime('%Y%m%d')
        else:
            end = end.replace('-', '')
        
        try:
            df = stock.get_index_fundamental(start, end, ticker)
            df = df.reset_index()
            df['ticker'] = ticker
            return df
            
        except Exception as e:
            self.logger.error(f"지수 지표 조회 실패 [{ticker}]: {e}")
            return pd.DataFrame()
    
    def get_stock_ticker_list(self, date: str = None, market: str = "ALL") -> List[str]:
        """특정일 기준 종목코드 리스트"""
        date = self._get_valid_date(date)
        
        try:
            tickers = stock.get_market_ticker_list(date, market=market)
            return list(tickers)
        except Exception as e:
            self.logger.error(f"종목코드 리스트 조회 실패: {e}")
            return []
    
    def get_stock_name(self, ticker: str) -> str:
        """종목코드로 종목명 조회"""
        try:
            return stock.get_market_ticker_name(ticker)
        except:
            return ""
    
    def collect(self, date: str = None) -> pd.DataFrame:
        """BaseCollector 인터페이스 구현 - 투자지표 수집"""
        return self.get_market_fundamental(date)
