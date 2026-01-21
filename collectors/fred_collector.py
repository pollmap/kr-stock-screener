"""
FRED API 수집기 (대폭 확장판)
- 40개+ 글로벌 경제지표
- 카테고리별 체계적 구성
- 비트코인, 원자재, 각국 금리 포함
"""

import requests
import pandas as pd
from typing import Optional, Dict, List
from datetime import datetime, timedelta
import logging

from .base_collector import BaseCollector, retry

logger = logging.getLogger("kr_stock_collector.fred")


class FREDCollector(BaseCollector):
    """FRED API 수집기 (40개+ 지표)"""
    
    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
    
    # ===== 40개+ 글로벌 경제지표 =====
    SERIES = {
        # ===== 주요 지수 (5개) =====
        'S&P500': 'SP500',
        '다우존스': 'DJIA',
        '나스닥': 'NASDAQCOM',
        '러셀2000': 'RUT',
        'VIX': 'VIXCLS',
        
        # ===== 암호화폐 (1개) =====
        '비트코인': 'CBBTCUSD',
        
        # ===== 원자재 (8개) =====
        'WTI유': 'DCOILWTICO',
        'Brent유': 'DCOILBRENTEU',
        '천연가스': 'DHHNGSP',
        '구리': 'PCOPPUSDM',
        '알루미늄': 'PALUMUSDM',
        '금': 'GOLDAMGBD228NLBM',
        '은': 'SLVPRUSD',
        '옥수수': 'PMAIZMTUSDM',
        
        # ===== 미국 금리 (10개) =====
        'Fed기준금리': 'FEDFUNDS',
        '미국채3M': 'DGS3MO',
        '미국채6M': 'DGS6MO',
        '미국채1Y': 'DGS1',
        '미국채2Y': 'DGS2',
        '미국채5Y': 'DGS5',
        '미국채10Y': 'DGS10',
        '미국채30Y': 'DGS30',
        '10Y-2Y스프레드': 'T10Y2Y',
        '10Y-3M스프레드': 'T10Y3M',
        
        # ===== 환율 (8개) =====
        '달러인덱스': 'DTWEXBGS',
        'EUR/USD': 'DEXUSEU',
        'USD/JPY': 'DEXJPUS',
        'USD/KRW': 'DEXKOUS',
        'USD/CNY': 'DEXCHUS',
        'USD/GBP': 'DEXUSUK',
        'USD/CHF': 'DEXSZUS',
        'USD/CAD': 'DEXCAUS',
        
        # ===== 미국 경제 (8개) =====
        '미국GDP': 'GDP',
        '미국CPI': 'CPIAUCSL',
        '미국Core_CPI': 'CPILFESL',
        '미국PCE': 'PCEPI',
        '미국실업률': 'UNRATE',
        '미국실업보험청구': 'ICSA',
        '미국산업생산': 'INDPRO',
        '미국소비자신뢰': 'UMCSENT',
        
        # ===== 신용 스프레드 (4개) =====
        'HY스프레드': 'BAMLH0A0HYM2',
        'IG스프레드': 'BAMLC0A0CM',
        'BBB스프레드': 'BAMLC0A4CBBB',
        'CCC스프레드': 'BAMLH0A3HYC',
        
        # ===== 기타 주요 지표 (6개) =====
        'M2통화량': 'M2SL',
        'Fed총자산': 'WALCL',
        '브레이크이븐5Y': 'T5YIE',
        '브레이크이븐10Y': 'T10YIE',
        '주택착공': 'HOUST',
        '소매판매': 'RSAFS',
    }
    
    # 카테고리별 그룹핑
    CATEGORIES = {
        '주요지수': ['S&P500', '다우존스', '나스닥', 'VIX', '러셀2000'],
        '암호화폐': ['비트코인'],
        '원자재_에너지': ['WTI유', 'Brent유', '천연가스'],
        '원자재_금속': ['금', '은', '구리', '알루미늄'],
        '원자재_농산물': ['옥수수'],
        '미국금리': ['Fed기준금리', '미국채2Y', '미국채10Y', '미국채30Y', '10Y-2Y스프레드'],
        '환율': ['달러인덱스', 'EUR/USD', 'USD/JPY', 'USD/KRW', 'USD/CNY'],
        '미국경제': ['미국GDP', '미국CPI', '미국실업률', '미국소비자신뢰'],
        '신용스프레드': ['HY스프레드', 'IG스프레드', 'BBB스프레드'],
        '통화/유동성': ['M2통화량', 'Fed총자산'],
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
    def get_series(
        self,
        series_id: str,
        start_date: str = None,
        end_date: str = None,
        frequency: str = None
    ) -> Optional[pd.DataFrame]:
        """단일 시리즈 조회"""
        
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365*3)).strftime('%Y-%m-%d')
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        cache_key = f"series_{series_id}_{start_date}_{end_date}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return pd.DataFrame(cached)
        
        params = {
            'series_id': series_id,
            'api_key': self.api_key,
            'file_type': 'json',
            'observation_start': start_date,
            'observation_end': end_date,
        }
        
        if frequency:
            params['frequency'] = frequency
        
        try:
            response = self._make_request('GET', self.BASE_URL, params=params, timeout=15)
            data = response.json()
            
            observations = data.get('observations', [])
            if not observations:
                return None
            
            df = pd.DataFrame(observations)
            df = df[['date', 'value']].copy()
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df.dropna(subset=['value'])
            
            self._save_to_cache(cache_key, df.to_dict('records'))
            return df
            
        except Exception as e:
            self.logger.warning(f"FRED [{series_id}]: {e}")
            return None
    
    def get_indicator(self, name: str, start: str = None, end: str = None) -> Optional[pd.DataFrame]:
        """지표명으로 조회"""
        if name not in self.SERIES:
            return None
        
        series_id = self.SERIES[name]
        df = self.get_series(series_id, start, end)
        
        if df is not None:
            df['indicator'] = name
            df['series_id'] = series_id
        
        return df
    
    def collect_category(self, category: str, start: str = None, end: str = None) -> pd.DataFrame:
        """카테고리별 수집"""
        if category not in self.CATEGORIES:
            return pd.DataFrame()
        
        indicators = self.CATEGORIES[category]
        all_data = []
        
        for name in indicators:
            self.logger.info(f"  수집: {name}")
            df = self.get_indicator(name, start, end)
            if df is not None and not df.empty:
                df['category'] = category
                all_data.append(df)
        
        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    
    def collect_all_indicators(
        self,
        start_date: str = None,
        end_date: str = None,
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
        result['source'] = 'FRED'
        
        self.logger.info(f"✓ 총 {len(result)} 행 글로벌 지표 수집")
        return result
    
    def get_latest_values(self) -> Dict[str, float]:
        """모든 지표의 최신값 조회"""
        latest = {}
        
        for name, series_id in self.SERIES.items():
            df = self.get_indicator(name)
            if df is not None and not df.empty:
                latest[name] = df.iloc[-1]['value']
        
        return latest
    
    def collect(self, start: str = None, end: str = None, categories: List[str] = None) -> pd.DataFrame:
        """BaseCollector 인터페이스"""
        return self.collect_all_indicators(start, end, categories)
