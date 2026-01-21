"""
FRED API 수집기 (글로벌 확장 v3)
- 50개+ 글로벌 경제지표
- DAX, 니케이, 항셍, 상해, KOSPI 포함
- 최신값만 반환
"""

import requests
import pandas as pd
from typing import Optional, Dict, List
from datetime import datetime, timedelta
import logging

from .base_collector import BaseCollector, retry

logger = logging.getLogger("kr_stock_collector.fred")


class FREDCollector(BaseCollector):
    """FRED API 수집기 (50개+ 글로벌 지표)"""
    
    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
    
    # ===== 50개+ 글로벌 경제지표 =====
    SERIES = {
        # 글로벌 주요 지수 (10개)
        'S&P500': 'SP500',
        '나스닥': 'NASDAQCOM',
        '다우존스': 'DJIA',
        'VIX(공포지수)': 'VIXCLS',
        'MOVE(채권변동성)': 'MOVE',
        '니케이225(일본)': 'NIKKEI225',
        'DAX(독일)': 'GDAXI',
        '항셍(홍콩)': 'HSI',
        '상해종합(중국)': 'SHCOMP',
        'KOSPI(한국)': 'KOSPI',
        
        # 미국 금리 (12개)
        'Fed기준금리': 'FEDFUNDS',
        'SOFR': 'SOFR',
        '미국채1M': 'DGS1MO',
        '미국채3M': 'DGS3MO',
        '미국채6M': 'DGS6MO',
        '미국채1Y': 'DGS1',
        '미국채2Y': 'DGS2',
        '미국채5Y': 'DGS5',
        '미국채10Y': 'DGS10',
        '미국채30Y': 'DGS30',
        '10Y-2Y스프레드': 'T10Y2Y',
        '10Y-3M스프레드': 'T10Y3M',
        
        # 원자재 (10개)
        'WTI원유': 'DCOILWTICO',
        'Brent원유': 'DCOILBRENTEU',
        '천연가스': 'DHHNGSP',
        '금': 'GOLDAMGBD228NLBM',
        '은': 'SLVPRUSD',
        '구리': 'PCOPPUSDM',
        '알루미늄': 'PALUMUSDM',
        '옥수수': 'PMAIZMTUSDM',
        '대두': 'PSOYBUSDM',
        '소맥(밀)': 'PWHEAMTUSDM',
        
        # 환율/통화 (8개)
        '달러인덱스(DXY)': 'DTWEXBGS',
        'EUR/USD': 'DEXUSEU',
        'USD/JPY': 'DEXJPUS',
        'GBP/USD': 'DEXUSUK',
        'USD/CNY': 'DEXCHUS',
        'USD/KRW': 'DEXKOUS',
        '비트코인': 'CBBTCUSD',
        '이더리움': 'CBETHUSD',
        
        # 글로벌 경제 (8개)
        '미국GDP': 'GDP',
        '미국CPI': 'CPIAUCSL',
        '미국CoreCPI': 'CPILFESL',
        '미국실업률': 'UNRATE',
        '미국산업생산': 'INDPRO',
        '미국소비자신뢰': 'UMCSENT',
        '중국PMI': 'MPMIBZ01CNM486S',
        '유로존CPI': 'CP0000EZ19M086NEST',
        
        # 신용/리스크 (5개)
        'HY스프레드': 'BAMLH0A0HYM2',
        'IG스프레드': 'BAMLC0A0CM',
        'TED스프레드': 'TEDRATE',
        'LIBOR-OIS': 'USDONTD156N',
        'BBB스프레드': 'BAMLC0A4CBBB',
    }
    
    CATEGORIES = {
        '글로벌지수': ['S&P500', '나스닥', '다우존스', 'VIX(공포지수)', 
                     '니케이225(일본)', 'DAX(독일)', '항셍(홍콩)', '상해종합(중국)', 'KOSPI(한국)'],
        '미국금리': ['Fed기준금리', 'SOFR', '미국채2Y', '미국채10Y', '미국채30Y', 
                   '10Y-2Y스프레드', '10Y-3M스프레드'],
        '원자재': ['WTI원유', 'Brent원유', '천연가스', '금', '은', '구리'],
        '환율': ['달러인덱스(DXY)', 'EUR/USD', 'USD/JPY', 'USD/CNY', 'USD/KRW', '비트코인'],
        '글로벌경제': ['미국GDP', '미국CPI', '미국실업률', '미국소비자신뢰', '중국PMI'],
        '신용리스크': ['HY스프레드', 'IG스프레드', 'TED스프레드', 'BBB스프레드'],
    }
    
    def __init__(self, api_key: str, cache_dir: str = "cache"):
        super().__init__(
            name="fred",
            cache_dir=cache_dir,
            cache_expiry_days=1,
            rate_limit_per_minute=100
        )
        self.api_key = api_key
    
    @retry(max_attempts=2, delay=0.3)
    def _fetch_latest(self, series_id: str) -> Optional[Dict]:
        """시리즈 최신값 조회"""
        params = {
            'series_id': series_id,
            'api_key': self.api_key,
            'file_type': 'json',
            'sort_order': 'desc',
            'limit': 1,
        }
        
        try:
            response = self._make_request('GET', self.BASE_URL, params=params, timeout=10)
            data = response.json()
            
            observations = data.get('observations', [])
            if not observations:
                return None
            
            latest = observations[0]
            value = latest.get('value', '.')
            if value == '.':
                return None
            
            return {
                'date': latest['date'],
                'value': float(value),
            }
            
        except Exception as e:
            self.logger.warning(f"FRED [{series_id}]: {e}")
            return None
    
    def _get_yoy(self, series_id: str) -> Optional[float]:
        """전년대비 변화율 (물가용)"""
        params = {
            'series_id': series_id,
            'api_key': self.api_key,
            'file_type': 'json',
            'sort_order': 'desc',
            'limit': 13,
        }
        
        try:
            response = self._make_request('GET', self.BASE_URL, params=params, timeout=10)
            data = response.json()
            
            observations = data.get('observations', [])
            if len(observations) < 2:
                return None
            
            latest = float(observations[0]['value'])
            oldest = float(observations[-1]['value'])
            
            if oldest != 0:
                return round(((latest - oldest) / oldest) * 100, 2)
            
        except:
            pass
        return None
    
    def collect_all_indicators(self) -> pd.DataFrame:
        """모든 지표 최신값 수집"""
        results = []
        
        for cat, indicators in self.CATEGORIES.items():
            self.logger.info(f"🌍 {cat} 지표 수집 중...")
            
            for name in indicators:
                if name not in self.SERIES:
                    continue
                
                self.logger.info(f"  수집: {name}")
                series_id = self.SERIES[name]
                data = self._fetch_latest(series_id)
                
                if data:
                    result = {
                        'indicator': name,
                        'date': data['date'],
                        'value': data['value'],
                        'category': cat,
                    }
                    
                    # 물가는 YoY 추가
                    if 'CPI' in name or 'PPI' in name:
                        yoy = self._get_yoy(series_id)
                        if yoy:
                            result['yoy_pct'] = yoy
                    
                    results.append(result)
        
        if results:
            df = pd.DataFrame(results)
            df['source'] = 'FRED'
            self.logger.info(f"✓ 총 {len(df)}개 글로벌 지표 수집")
            return df
        
        return pd.DataFrame()
    
    def collect(self, start: str = None, end: str = None, categories: List[str] = None) -> pd.DataFrame:
        """BaseCollector 인터페이스"""
        return self.collect_all_indicators()
