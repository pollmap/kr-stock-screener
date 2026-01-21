"""
pykrx 기반 KRX 데이터 수집기 (강화판)
- 빈 데이터 처리 개선
- 다중 날짜 시도 (최대 7일 전까지)
- 휴일 회피
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
    
    def _find_valid_trading_date(self, max_tries: int = 7) -> str:
        """유효한 거래일 찾기 (최대 7일 전까지 시도)"""
        dt = datetime.now()
        
        for i in range(max_tries):
            check_dt = dt - timedelta(days=i+1)
            
            # 주말 건너뛰기
            if check_dt.weekday() >= 5:
                continue
            
            date_str = check_dt.strftime('%Y%m%d')
            
            # 실제 데이터 존재 확인
            try:
                df = stock.get_market_ohlcv(date_str, market="KOSPI")
                if df is not None and not df.empty:
                    self.logger.info(f"유효 거래일 발견: {date_str}")
                    return date_str
            except:
                continue
        
        # 찾지 못하면 어제 날짜 반환
        return (dt - timedelta(days=1)).strftime('%Y%m%d')
    
    def _get_valid_date(self, date: str = None) -> str:
        """유효한 거래일 반환"""
        if date is None:
            return self._find_valid_trading_date()
        
        date = date.replace('-', '')
        dt = datetime.strptime(date, '%Y%m%d')
        
        # 주말이면 금요일로
        if dt.weekday() == 5:
            dt = dt - timedelta(days=1)
        elif dt.weekday() == 6:
            dt = dt - timedelta(days=2)
        
        return dt.strftime('%Y%m%d')
    
    def get_market_ohlcv(self, date: str = None, market: str = "ALL") -> pd.DataFrame:
        """전 종목 시세 조회"""
        date = self._get_valid_date(date)
        
        cache_key = f"ohlcv_{date}_{market}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return pd.DataFrame(cached)
        
        try:
            df = stock.get_market_ohlcv(date, market=market)
            
            if df is None or df.empty:
                self.logger.warning(f"시세 데이터 없음: {date}")
                return pd.DataFrame()
            
            df = df.reset_index()
            
            # 첫 컬럼을 stock_code로
            df = df.rename(columns={df.columns[0]: 'stock_code'})
            
            # 한글 컬럼 -> 영문
            rename_map = {
                '시가': 'open', '고가': 'high', '저가': 'low', '종가': 'close',
                '거래량': 'volume', '거래대금': 'value', '등락률': 'change'
            }
            df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
            
            df['date'] = date
            
            self._save_to_cache(cache_key, df.to_dict('records'))
            self.logger.info(f"{date} 시세 {len(df)}개 종목 조회")
            
            return df
            
        except Exception as e:
            self.logger.error(f"시세 조회 실패 [{date}]: {e}")
            return pd.DataFrame()
    
    def get_market_fundamental(self, date: str = None, market: str = "ALL") -> pd.DataFrame:
        """전 종목 투자지표 조회"""
        date = self._get_valid_date(date)
        
        cache_key = f"fundamental_{date}_{market}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return pd.DataFrame(cached)
        
        try:
            df = stock.get_market_fundamental(date, market=market)
            
            if df is None or df.empty:
                self.logger.warning(f"투자지표 데이터 없음: {date}")
                return pd.DataFrame()
            
            df = df.reset_index()
            df = df.rename(columns={df.columns[0]: 'stock_code'})
            
            # 컬럼명 변환
            rename_map = {
                'BPS': 'bps', 'PER': 'per', 'PBR': 'pbr', 
                'EPS': 'eps', 'DIV': 'div_yield', 'DPS': 'dps'
            }
            df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
            
            df['date'] = date
            
            self._save_to_cache(cache_key, df.to_dict('records'))
            self.logger.info(f"{date} 투자지표 {len(df)}개 종목 조회")
            
            return df
            
        except Exception as e:
            self.logger.error(f"투자지표 조회 실패 [{date}]: {e}")
            return pd.DataFrame()
    
    def get_market_cap(self, date: str = None, market: str = "ALL") -> pd.DataFrame:
        """전 종목 시가총액 조회"""
        date = self._get_valid_date(date)
        
        cache_key = f"cap_{date}_{market}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return pd.DataFrame(cached)
        
        try:
            df = stock.get_market_cap(date, market=market)
            
            if df is None or df.empty:
                return pd.DataFrame()
            
            df = df.reset_index()
            df = df.rename(columns={df.columns[0]: 'stock_code'})
            
            rename_map = {
                '시가총액': 'market_cap', '거래량': 'volume',
                '거래대금': 'value', '상장주식수': 'shares', '종가': 'close'
            }
            df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
            
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
