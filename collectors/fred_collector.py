"""
FRED (Federal Reserve Economic Data) API (Pro-Level)
50개+ 글로벌 경제지표:
- 미국 금리 (12개)
- 주식/변동성 (6개)
- 원자재 (12개)
- 환율 (8개)
- 미국 경제 (10개)
- 신용/스프레드 (6개)
"""

import requests
import pandas as pd
from typing import Optional, List, Dict
import logging
import time

from .base_collector import BaseCollector, retry

logger = logging.getLogger("kr_stock_collector.fred")


class FREDCollector(BaseCollector):
    """
    FRED API 수집기 (Pro-Level)
    
    50개+ 글로벌 경제지표 수집
    """
    
    BASE_URL = "https://api.stlouisfed.org/fred"
    
    # =====================================
    # 주요 시리즈 코드 (확장판)
    # =====================================
    SERIES = {
        # ===== 미국 금리 (12개) =====
        'Fed_Funds': 'FEDFUNDS',
        'SOFR': 'SOFR',
        '미국채_1M': 'DGS1MO',
        '미국채_3M': 'DGS3MO',
        '미국채_6M': 'DGS6MO',
        '미국채_1Y': 'DGS1',
        '미국채_2Y': 'DGS2',
        '미국채_5Y': 'DGS5',
        '미국채_10Y': 'DGS10',
        '미국채_30Y': 'DGS30',
        '10Y-2Y_스프레드': 'T10Y2Y',
        '10Y-3M_스프레드': 'T10Y3M',
        '5Y_Breakeven_Inflation': 'T5YIE',
        '10Y_Breakeven_Inflation': 'T10YIE',
        
        # ===== 주식/변동성 (6개) =====
        'S&P500': 'SP500',
        'NASDAQ': 'NASDAQCOM',
        'VIX': 'VIXCLS',
        'MOVE_Index': 'MOVE',
        'SKEW': 'SKEW',
        'Put_Call_Ratio': 'PCERI',
        
        # ===== 원자재 (12개) =====
        'WTI_원유': 'DCOILWTICO',
        'Brent_원유': 'DCOILBRENTEU',
        '천연가스': 'DHHNGSP',
        '금': 'GOLDAMGBD228NLBM',
        '은': 'SLVPRUSD',
        '구리': 'PCOPPUSDM',
        '알루미늄': 'PALUMUSDM',
        '철광석': 'PIORECRUSDM',
        '옥수수': 'PMAIZMTUSDM',
        '대두': 'PSOYBUSDM',
        '소맥': 'PWHEAMTUSDM',
        'CRB_지수': 'CRBPI',
        
        # ===== 통화/환율 (8개) =====
        '달러인덱스': 'DTWEXBGS',
        'EUR_USD': 'DEXUSEU',
        'USD_JPY': 'DEXJPUS',
        'GBP_USD': 'DEXUSUK',
        'USD_CNY': 'DEXCHUS',
        'USD_KRW': 'DEXKOUS',
        'USD_CHF': 'DEXSZUS',
        'Bitcoin': 'CBBTCUSD',
        
        # ===== 미국 경제 (10개) =====
        '미국_GDP': 'GDP',
        '미국_GDP_성장률': 'A191RL1Q225SBEA',
        '미국_CPI': 'CPIAUCSL',
        '미국_Core_CPI': 'CPILFESL',
        '미국_PCE': 'PCEPI',
        '미국_Core_PCE': 'PCEPILFE',
        '미국_실업률': 'UNRATE',
        '미국_고용': 'PAYEMS',
        '미국_산업생산': 'INDPRO',
        '미국_소매판매': 'RSAFS',
        '미국_주택착공': 'HOUST',
        '미국_소비자신뢰': 'UMCSENT',
        
        # ===== 신용/스프레드 (6개) =====
        'High_Yield_스프레드': 'BAMLH0A0HYM2',
        'IG_스프레드': 'BAMLC0A0CM',
        'TED_스프레드': 'TEDRATE',
        'AAA_회사채': 'AAA',
        'BAA_회사채': 'BAA',
        'BAA-AAA_스프레드': 'BAA10Y',
        
        # ===== 글로벌 (4개) =====
        '글로벌_무역량': 'INDPROMANM',
        '중국_PMI': 'MPMIBMCN',
        '유로존_CPI': 'FPCPITOTLZGEMU',
        '일본_CPI': 'FPCPITOTLZGJPN',
    }
    
    # 카테고리별 그룹핑
    CATEGORIES = {
        '미국금리': ['Fed_Funds', '미국채_2Y', '미국채_5Y', '미국채_10Y', '미국채_30Y',
                   '10Y-2Y_스프레드', '10Y-3M_스프레드', '5Y_Breakeven_Inflation'],
        '변동성': ['VIX', 'MOVE_Index', 'SKEW'],
        '원자재': ['WTI_원유', 'Brent_원유', '천연가스', '금', '은', '구리', '옥수수'],
        '환율': ['달러인덱스', 'EUR_USD', 'USD_JPY', 'USD_CNY', 'USD_KRW'],
        '미국경제': ['미국_GDP_성장률', '미국_CPI', '미국_Core_PCE', '미국_실업률',
                   '미국_산업생산', '미국_소비자신뢰'],
        '신용스프레드': ['High_Yield_스프레드', 'IG_스프레드', 'TED_스프레드'],
        '주식': ['S&P500', 'NASDAQ'],
        '글로벌': ['중국_PMI', '유로존_CPI'],
    }
    
    def __init__(self, api_key: str, cache_dir: str = "cache"):
        super().__init__(
            name="fred",
            cache_dir=cache_dir,
            cache_expiry_days=1,
            rate_limit_per_minute=120
        )
        self.api_key = api_key
    
    @retry(max_attempts=3, delay=1.0)
    def get_series(
        self,
        series_id: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        FRED 시리즈 데이터 조회
        """
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
                error = data.get('error_message', 'Unknown')
                self.logger.warning(f"FRED API: {error}")
                return None
                
        except Exception as e:
            self.logger.error(f"FRED API 실패 [{series_id}]: {e}")
            raise
    
    def get_indicator(
        self,
        indicator_name: str,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        단일 지표 조회 (간편 함수)
        """
        if indicator_name not in self.SERIES:
            self.logger.warning(f"알 수 없는 지표: {indicator_name}")
            return None
        
        series_id = self.SERIES[indicator_name]
        df = self.get_series(series_id, start_date, end_date)
        
        if df is not None:
            df['indicator'] = indicator_name
        
        return df
    
    def collect_category(
        self,
        category: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        카테고리별 지표 수집
        """
        if category not in self.CATEGORIES:
            self.logger.warning(f"알 수 없는 카테고리: {category}")
            return pd.DataFrame()
        
        indicators = self.CATEGORIES[category]
        all_data = []
        
        for ind in indicators:
            self.logger.info(f"  수집: {ind}")
            df = self.get_indicator(ind, start_date, end_date)
            if df is not None and not df.empty:
                df['category'] = category
                all_data.append(df)
            time.sleep(0.05)
        
        if not all_data:
            return pd.DataFrame()
        
        return pd.concat(all_data, ignore_index=True)
    
    def collect_all_indicators(
        self,
        start_date: str,
        end_date: str,
        categories: List[str] = None
    ) -> pd.DataFrame:
        """
        전체/선택 글로벌 지표 수집
        
        Args:
            start_date: 시작일 (YYYY-MM-DD)
            end_date: 종료일
            categories: 수집할 카테고리 (None이면 주요 항목)
        """
        if categories is None:
            # 기본: 주요 카테고리만
            categories = ['미국금리', '변동성', '원자재', '환율', '미국경제']
        
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
    
    def get_yield_curve(self, date: str) -> Dict[str, Optional[float]]:
        """
        미국 수익률 곡선 조회
        
        Args:
            date: 조회일 (YYYY-MM-DD)
        
        Returns:
            {tenor: yield} 딕셔너리
        """
        tenors = ['1M', '3M', '6M', '1Y', '2Y', '5Y', '10Y', '30Y']
        series_map = {
            '1M': 'DGS1MO', '3M': 'DGS3MO', '6M': 'DGS6MO',
            '1Y': 'DGS1', '2Y': 'DGS2', '5Y': 'DGS5',
            '10Y': 'DGS10', '30Y': 'DGS30'
        }
        
        result = {}
        
        for tenor in tenors:
            df = self.get_series(series_map[tenor], date, date)
            if df is not None and not df.empty:
                result[tenor] = df['value'].iloc[-1]
            else:
                result[tenor] = None
        
        return result
    
    def get_available_indicators(self) -> Dict[str, List[str]]:
        """사용 가능한 지표 목록 반환"""
        return self.CATEGORIES
    
    def collect(self, start: str, end: str, categories: List[str] = None) -> pd.DataFrame:
        """BaseCollector 인터페이스"""
        return self.collect_all_indicators(start, end, categories)
