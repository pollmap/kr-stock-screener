"""
FRED API 수집기 (수정판)
- 유효한 시리즈 ID만 사용
- 존재하지 않는 시리즈 제거
"""

import requests
import pandas as pd
from typing import Optional, List, Dict
import logging
import time

from .base_collector import BaseCollector, retry

logger = logging.getLogger("kr_stock_collector.fred")


class FREDCollector(BaseCollector):
    """FRED API 수집기 (검증된 시리즈만)"""
    
    BASE_URL = "https://api.stlouisfed.org/fred"
    
    # 검증된 시리즈 ID만 포함
    SERIES = {
        # 미국 금리 (검증됨)
        'Fed_Funds': 'FEDFUNDS',
        '미국채_3M': 'DGS3MO',
        '미국채_2Y': 'DGS2',
        '미국채_5Y': 'DGS5',
        '미국채_10Y': 'DGS10',
        '미국채_30Y': 'DGS30',
        '10Y-2Y_스프레드': 'T10Y2Y',
        '10Y-3M_스프레드': 'T10Y3M',
        '5Y_Breakeven': 'T5YIE',
        
        # 변동성 (검증됨)
        'VIX': 'VIXCLS',
        
        # 원자재 (검증됨)
        'WTI_원유': 'DCOILWTICO',
        'Brent_원유': 'DCOILBRENTEU',
        '천연가스': 'DHHNGSP',
        '구리': 'PCOPPUSDM',
        
        # 환율 (검증됨)
        '달러인덱스': 'DTWEXBGS',
        'EUR_USD': 'DEXUSEU',
        'USD_JPY': 'DEXJPUS',
        'USD_KRW': 'DEXKOUS',
        'USD_CNY': 'DEXCHUS',
        
        # 미국 경제 (검증됨)
        '미국_GDP': 'GDP',
        '미국_CPI': 'CPIAUCSL',
        '미국_Core_CPI': 'CPILFESL',
        '미국_실업률': 'UNRATE',
        '미국_산업생산': 'INDPRO',
        
        # 신용 (검증됨)
        'High_Yield_스프레드': 'BAMLH0A0HYM2',
        'IG_스프레드': 'BAMLC0A0CM',
    }
    
    # 카테고리
    CATEGORIES = {
        '미국금리': ['Fed_Funds', '미국채_2Y', '미국채_5Y', '미국채_10Y', '미국채_30Y',
                   '10Y-2Y_스프레드', '10Y-3M_스프레드'],
        '변동성': ['VIX'],
        '원자재': ['WTI_원유', 'Brent_원유', '천연가스', '구리'],
        '환율': ['달러인덱스', 'EUR_USD', 'USD_JPY', 'USD_KRW'],
        '미국경제': ['미국_GDP', '미국_CPI', '미국_실업률', '미국_산업생산'],
        '신용스프레드': ['High_Yield_스프레드', 'IG_스프레드'],
    }
    
    def __init__(self, api_key: str, cache_dir: str = "cache"):
        super().__init__(
            name="fred",
            cache_dir=cache_dir,
            cache_expiry_days=1,
            rate_limit_per_minute=120
        )
        self.api_key = api_key
    
    @retry(max_attempts=2, delay=0.5)
    def get_series(
        self,
        series_id: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """FRED 시리즈 조회"""
        cache_key = f"series_{series_id}_{start_date}_{end_date}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return pd.DataFrame(cached)
        
        url = f"{self.BASE_URL}/series/observations"
        params = {
            'series_id': series_id,
            'api_key': self.api_key,
            'observation_start': start_date,
            'observation_end': end_date,
            'file_type': 'json'
        }
        
        try:
            response = self._make_request('GET', url, params=params, timeout=30)
            data = response.json()
            
            if 'observations' in data:
                observations = data['observations']
                if not observations:
                    return None
                
                df = pd.DataFrame(observations)
                df['value'] = pd.to_numeric(df['value'], errors='coerce')
                df['series_id'] = series_id
                df = df[['date', 'value', 'series_id']]
                
                self._save_to_cache(cache_key, df.to_dict('records'))
                return df
            else:
                return None
                
        except Exception as e:
            self.logger.warning(f"FRED [{series_id}]: {e}")
            return None
    
    def get_indicator(self, name: str, start: str, end: str) -> Optional[pd.DataFrame]:
        """단일 지표 조회"""
        if name not in self.SERIES:
            return None
        
        series_id = self.SERIES[name]
        df = self.get_series(series_id, start, end)
        if df is not None:
            df['indicator'] = name
        return df
    
    def collect_category(self, category: str, start: str, end: str) -> pd.DataFrame:
        """카테고리별 수집"""
        if category not in self.CATEGORIES:
            return pd.DataFrame()
        
        indicators = self.CATEGORIES[category]
        all_data = []
        
        for ind in indicators:
            self.logger.info(f"  수집: {ind}")
            df = self.get_indicator(ind, start, end)
            if df is not None and not df.empty:
                df['category'] = category
                all_data.append(df)
            time.sleep(0.05)
        
        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    
    def collect_all_indicators(
        self,
        start_date: str,
        end_date: str,
        categories: List[str] = None
    ) -> pd.DataFrame:
        """전체 지표 수집"""
        if categories is None:
            categories = list(self.CATEGORIES.keys())
        
        all_data = []
        for cat in categories:
            self.logger.info(f"🌍 {cat} 지표 수집 중...")
            df = self.collect_category(cat, start_date, end_date)
            if not df.empty:
                all_data.append(df)
        
        if not all_data:
            return pd.DataFrame()
        
        result = pd.concat(all_data, ignore_index=True)
        self.logger.info(f"✓ 총 {len(result)} 행 글로벌 지표 수집")
        return result
    
    def get_available_indicators(self) -> Dict[str, List[str]]:
        return self.CATEGORIES
    
    def collect(self, start: str, end: str, categories: List[str] = None) -> pd.DataFrame:
        return self.collect_all_indicators(start, end, categories)
