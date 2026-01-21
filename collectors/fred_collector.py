"""
FRED API 수집기 (최신값만 수집)
- 시계열 데이터 없음
- 모든 지표 최신값/YoY만 표시
- 60개+ 글로벌 경제지표
"""

import requests
import pandas as pd
from typing import Optional, Dict, List
from datetime import datetime, timedelta
import logging

from .base_collector import BaseCollector, retry

logger = logging.getLogger("kr_stock_collector.fred")


class FREDCollector(BaseCollector):
    """FRED API 수집기 (최신값 전용)"""
    
    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
    
    # ===== 핵심 지표만 (중복 제거) =====
    SERIES = {
        # 글로벌 주요 지수
        'S&P500': 'SP500',
        '다우존스': 'DJIA',
        '나스닥': 'NASDAQCOM',
        'VIX(공포지수)': 'VIXCLS',
        
        # 암호화폐
        '비트코인': 'CBBTCUSD',
        
        # 원자재
        'WTI원유': 'DCOILWTICO',
        'Brent원유': 'DCOILBRENTEU',
        '천연가스': 'DHHNGSP',
        '금': 'GOLDAMGBD228NLBM',
        '은': 'SLVPRUSD',
        '구리': 'PCOPPUSDM',
        
        # 각국 기준금리
        'Fed기준금리(미국)': 'FEDFUNDS',
        'ECB기준금리(유럽)': 'ECBMRRFR',
        'BOJ기준금리(일본)': 'IRSTCI01JPM156N',
        '중국기준금리': 'IRSTCB01CNM156N',
        
        # 각국 국채 10년물
        '미국채10Y': 'DGS10',
        '독일국채10Y': 'IRLTLT01DEM156N',
        '영국국채10Y': 'IRLTLT01GBM156N',
        '일본국채10Y': 'IRLTLT01JPM156N',
        
        # 미국 금리
        '미국채2Y': 'DGS2',
        '미국채30Y': 'DGS30',
        '10Y-2Y스프레드': 'T10Y2Y',
        'HY스프레드': 'BAMLH0A0HYM2',
        
        # 환율
        '달러인덱스': 'DTWEXBGS',
        'EUR/USD': 'DEXUSEU',
        'USD/JPY': 'DEXJPUS',
        'USD/KRW': 'DEXKOUS',
        'USD/CNY': 'DEXCHUS',
        
        # 물가 (YoY 계산용)
        '미국CPI': 'CPIAUCSL',
        '미국CoreCPI': 'CPILFESL',
        '미국PCE': 'PCEPI',
        '미국PPI': 'PPIACO',
        
        # 고용
        '미국실업률': 'UNRATE',
        '신규실업수당청구': 'ICSA',
        
        # 경기
        'ISM제조업지수': 'MANEMP',
        '미국소비자신뢰': 'UMCSENT',
        '소매판매': 'RSAFS',
        
        # 유동성
        'M2통화량': 'M2SL',
        'Fed총자산': 'WALCL',
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
    def _fetch_series(self, series_id: str, limit: int = 13) -> Optional[pd.DataFrame]:
        """시리즈 최근 N개 조회"""
        cache_key = f"latest_{series_id}_{limit}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return pd.DataFrame(cached)
        
        params = {
            'series_id': series_id,
            'api_key': self.api_key,
            'file_type': 'json',
            'sort_order': 'desc',
            'limit': limit,
        }
        
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
    
    def _get_latest(self, name: str) -> Optional[Dict]:
        """최신값 조회"""
        if name not in self.SERIES:
            return None
        
        df = self._fetch_series(self.SERIES[name], limit=1)
        if df is not None and not df.empty:
            return {
                'indicator': name,
                'date': df.iloc[0]['date'],
                'value': df.iloc[0]['value'],
            }
        return None
    
    def _get_yoy(self, name: str) -> Optional[Dict]:
        """전년대비 변화율"""
        if name not in self.SERIES:
            return None
        
        df = self._fetch_series(self.SERIES[name], limit=13)
        if df is not None and len(df) >= 2:
            df = df.sort_values('date')
            latest = df.iloc[-1]['value']
            oldest = df.iloc[0]['value']
            
            if oldest and oldest != 0:
                yoy = ((latest - oldest) / oldest) * 100
                return {
                    'indicator': name,
                    'date': df.iloc[-1]['date'],
                    'value': latest,
                    'yoy_pct': round(yoy, 2),
                }
        return None
    
    def collect_all_indicators(self, start_date=None, end_date=None, categories=None) -> pd.DataFrame:
        """모든 지표 최신값 수집"""
        results = []
        
        # 1. 금리류 - 최신값
        rates = ['Fed기준금리(미국)', 'ECB기준금리(유럽)', 'BOJ기준금리(일본)', '중국기준금리',
                 '미국채2Y', '미국채10Y', '미국채30Y', 
                 '독일국채10Y', '영국국채10Y', '일본국채10Y',
                 '10Y-2Y스프레드', 'HY스프레드']
        for name in rates:
            self.logger.info(f"  수집: {name}")
            data = self._get_latest(name)
            if data:
                data['category'] = '금리'
                results.append(data)
        
        # 2. 물가 - YoY%
        prices = ['미국CPI', '미국CoreCPI', '미국PCE', '미국PPI']
        for name in prices:
            self.logger.info(f"  수집: {name} (YoY)")
            data = self._get_yoy(name)
            if data:
                data['category'] = '물가(YoY%)'
                results.append(data)
        
        # 3. 고용
        self.logger.info("  수집: 미국실업률")
        data = self._get_latest('미국실업률')
        if data:
            data['category'] = '고용'
            results.append(data)
        
        self.logger.info("  수집: 신규실업수당청구(4주)")
        df = self._fetch_series(self.SERIES['신규실업수당청구'], limit=4)
        if df is not None:
            for _, row in df.iterrows():
                results.append({
                    'indicator': '신규실업수당청구',
                    'date': row['date'],
                    'value': row['value'],
                    'category': '고용(주간)'
                })
        
        # 4. 지수/시장
        markets = ['S&P500', '다우존스', '나스닥', 'VIX(공포지수)', '비트코인']
        for name in markets:
            self.logger.info(f"  수집: {name}")
            data = self._get_latest(name)
            if data:
                data['category'] = '주요지수'
                results.append(data)
        
        # 5. 원자재
        commodities = ['WTI원유', 'Brent원유', '금', '은', '구리', '천연가스']
        for name in commodities:
            self.logger.info(f"  수집: {name}")
            data = self._get_latest(name)
            if data:
                data['category'] = '원자재'
                results.append(data)
        
        # 6. 환율
        currencies = ['달러인덱스', 'USD/KRW', 'USD/JPY', 'USD/CNY', 'EUR/USD']
        for name in currencies:
            self.logger.info(f"  수집: {name}")
            data = self._get_latest(name)
            if data:
                data['category'] = '환율'
                results.append(data)
        
        # 7. 경기
        econ = ['ISM제조업지수', '미국소비자신뢰', '소매판매']
        for name in econ:
            self.logger.info(f"  수집: {name}")
            data = self._get_latest(name)
            if data:
                data['category'] = '경기'
                results.append(data)
        
        # 8. 유동성
        liquidity = ['M2통화량', 'Fed총자산']
        for name in liquidity:
            self.logger.info(f"  수집: {name}")
            data = self._get_latest(name)
            if data:
                data['category'] = '유동성'
                results.append(data)
        
        if results:
            df = pd.DataFrame(results)
            df['source'] = 'FRED'
            self.logger.info(f"✓ 총 {len(df)} 개 지표 수집 완료")
            return df
        
        return pd.DataFrame()
    
    def collect(self, start: str = None, end: str = None, categories: List[str] = None) -> pd.DataFrame:
        return self.collect_all_indicators()
