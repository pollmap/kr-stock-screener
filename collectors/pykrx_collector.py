"""
pykrx 기반 KRX 데이터 수집기 (수정판)
- 최신 pykrx 컬럼명 대응
- 어제 날짜 자동 사용 (오늘 데이터 없을 때)
"""

from pykrx import stock
import pandas as pd
from typing import Optional, List
from datetime import datetime, timedelta
import logging

from .base_collector import BaseCollector

logger = logging.getLogger("kr_stock_collector.pykrx")


class PyKrxCollector(BaseCollector):
    """pykrx 기반 KRX 데이터 수집기"""
    
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
        """유효한 거래일 반환 (주말/공휴일/오늘 회피)"""
        if date is None:
            # 오늘이 아닌 어제 사용 (장 마감 후 데이터 확보)
            dt = datetime.now() - timedelta(days=1)
        else:
            date = date.replace('-', '')
            dt = datetime.strptime(date, '%Y%m%d')
        
        # 주말이면 금요일로
        if dt.weekday() == 5:  # 토요일
            dt = dt - timedelta(days=1)
        elif dt.weekday() == 6:  # 일요일
            dt = dt - timedelta(days=2)
        
        return dt.strftime('%Y%m%d')
    
    def _normalize_columns(self, df: pd.DataFrame, expected: dict) -> pd.DataFrame:
        """컬럼명 정규화 (pykrx 버전 변화 대응)"""
        # 실제 컬럼 확인
        actual = df.columns.tolist()
        
        # 매핑 시도
        rename_map = {}
        for target, candidates in expected.items():
            for cand in candidates:
                if cand in actual:
                    rename_map[cand] = target
                    break
        
        if rename_map:
            df = df.rename(columns=rename_map)
        
        return df
    
    def get_market_ohlcv(
        self,
        date: str = None,
        market: str = "ALL"
    ) -> pd.DataFrame:
        """전 종목 시세 조회"""
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
            
            # 컬럼명 정규화
            col_map = {
                'stock_code': ['티커', 'index', '종목코드'],
                'open': ['시가'],
                'high': ['고가'],
                'low': ['저가'],
                'close': ['종가'],
                'volume': ['거래량'],
                'value': ['거래대금'],
                'change': ['등락률']
            }
            df = self._normalize_columns(df, col_map)
            
            # 첫 컬럼을 stock_code로
            if 'stock_code' not in df.columns:
                df = df.rename(columns={df.columns[0]: 'stock_code'})
            
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
        """전 종목 투자지표 조회"""
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
            
            # 컬럼명 정규화
            col_map = {
                'stock_code': ['티커', 'index', '종목코드'],
                'bps': ['BPS'],
                'per': ['PER'],
                'pbr': ['PBR'],
                'eps': ['EPS'],
                'div_yield': ['DIV', 'DY'],
                'dps': ['DPS']
            }
            df = self._normalize_columns(df, col_map)
            
            if 'stock_code' not in df.columns:
                df = df.rename(columns={df.columns[0]: 'stock_code'})
            
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
        """전 종목 시가총액 조회"""
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
            
            # 컬럼명 정규화  
            col_map = {
                'stock_code': ['티커', 'index', '종목코드'],
                'market_cap': ['시가총액'],
                'volume': ['거래량'],
                'value': ['거래대금'],
                'shares': ['상장주식수']
            }
            df = self._normalize_columns(df, col_map)
            
            if 'stock_code' not in df.columns:
                df = df.rename(columns={df.columns[0]: 'stock_code'})
            
            df['date'] = date
            
            self._save_to_cache(cache_key, df.to_dict('records'))
            
            return df
            
        except Exception as e:
            self.logger.error(f"시가총액 조회 실패 [{date}]: {e}")
            return pd.DataFrame()
    
    def get_stock_ticker_list(self, date: str = None, market: str = "ALL") -> List[str]:
        """종목코드 리스트"""
        date = self._get_valid_date(date)
        try:
            tickers = stock.get_market_ticker_list(date, market=market)
            return list(tickers)
        except Exception as e:
            self.logger.error(f"종목코드 리스트 조회 실패: {e}")
            return []
    
    def get_stock_name(self, ticker: str) -> str:
        """종목명 조회"""
        try:
            return stock.get_market_ticker_name(ticker)
        except:
            return ""
    
    def collect(self, date: str = None) -> pd.DataFrame:
        """BaseCollector 인터페이스"""
        return self.get_market_fundamental(date)
