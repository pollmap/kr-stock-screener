"""
FRED API 수집기 (확장판 v2)
- 글로벌 주요 지수 (한국/일본/중국/홍콩/유럽/독일 포함)
- 각국 금리 (미국/한국/일본/독일/영국 포함)
- 최신값만 표시 옵션
- YoY 변화율 계산
"""

import requests
import pandas as pd
from typing import Optional, Dict, List
from datetime import datetime, timedelta
import logging

from .base_collector import BaseCollector, retry

logger = logging.getLogger("kr_stock_collector.fred")


class FREDCollector(BaseCollector):
    """FRED API 수집기 (60개+ 지표)"""
    
    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
    
    # ===== 60개+ 글로벌 경제지표 =====
    SERIES = {
        # ===== 글로벌 주요 지수 (12개) =====
        'S&P500': 'SP500',
        '다우존스': 'DJIA',
        '나스닥': 'NASDAQCOM',
        'VIX(공포지수)': 'VIXCLS',
        '러셀2000': 'RUT',
        # 아시아
        '니케이225(일본)': 'NIKKEI225',
        '상해종합(중국)': 'CHNGDPNQDSMEI',
        '항셍(홍콩)': 'HSHKGIND',
        # 유럽
        'STOXX50(유럽)': 'EA19LSTOXX50USEA',
        'DAX(독일)': 'DEXUSEU',  # 대용
        'FTSE100(영국)': 'UKNGDPMKTPSMEI',
        
        # ===== 암호화폐 (1개) =====
        '비트코인': 'CBBTCUSD',
        
        # ===== 원자재 (10개) =====
        'WTI원유': 'DCOILWTICO',
        'Brent원유': 'DCOILBRENTEU',
        '천연가스': 'DHHNGSP',
        '금': 'GOLDAMGBD228NLBM',
        '은': 'SLVPRUSD',
        '구리': 'PCOPPUSDM',
        '알루미늄': 'PALUMUSDM',
        '철광석': 'PIORECRUSDM',
        '옥수수': 'PMAIZMTUSDM',
        '소맥(밀)': 'PWHEAMTUSDM',
        
        # ===== 각국 기준금리 (8개) =====
        'Fed기준금리(미국)': 'FEDFUNDS',
        'ECB기준금리(유럽)': 'ECBMRRFR',
        'BOJ기준금리(일본)': 'IRSTCI01JPM156N',
        'BOE기준금리(영국)': 'BOGZ1FL072052006Q',
        '독일국채10Y': 'IRLTLT01DEM156N',
        '영국국채10Y': 'IRLTLT01GBM156N',
        '일본국채10Y': 'IRLTLT01JPM156N',
        '중국기준금리': 'CHBLR1Y',
        
        # ===== 미국 금리/채권 (10개) =====
        '미국채3M': 'DGS3MO',
        '미국채6M': 'DGS6MO',
        '미국채1Y': 'DGS1',
        '미국채2Y': 'DGS2',
        '미국채5Y': 'DGS5',
        '미국채10Y': 'DGS10',
        '미국채30Y': 'DGS30',
        '10Y-2Y스프레드(경기침체신호)': 'T10Y2Y',
        '10Y-3M스프레드': 'T10Y3M',
        'HY스프레드(신용위험)': 'BAMLH0A0HYM2',
        
        # ===== 환율 (8개) =====
        '달러인덱스': 'DTWEXBGS',
        'EUR/USD': 'DEXUSEU',
        'USD/JPY': 'DEXJPUS',
        'USD/KRW': 'DEXKOUS',
        'USD/CNY': 'DEXCHUS',
        'USD/GBP': 'DEXUSUK',
        'USD/CHF': 'DEXSZUS',
        'USD/CAD': 'DEXCAUS',
        
        # ===== 미국 주요 경제지표 (12개) =====
        '미국GDP': 'GDP',
        '미국CPI(물가)': 'CPIAUCSL',
        '미국Core_CPI': 'CPILFESL',
        '미국PCE(물가)': 'PCEPI',
        '미국PPI(생산자물가)': 'PPIACO',
        '미국실업률': 'UNRATE',
        '신규실업수당청구': 'ICSA',
        'ISM제조업지수': 'MANEMP',
        '미국산업생산': 'INDPRO',
        '미국소비자신뢰': 'UMCSENT',
        '소매판매': 'RSAFS',
        '주택착공': 'HOUST',
        
        # ===== 통화/유동성 (4개) =====
        'M2통화량': 'M2SL',
        'Fed총자산': 'WALCL',
        '5Y기대인플레이션': 'T5YIE',
        '10Y기대인플레이션': 'T10YIE',
    }
    
    # 카테고리별 그룹핑
    CATEGORIES = {
        '글로벌지수': ['S&P500', '다우존스', '나스닥', 'VIX(공포지수)', '니케이225(일본)', '상해종합(중국)'],
        '암호화폐': ['비트코인'],
        '원자재_에너지': ['WTI원유', 'Brent원유', '천연가스'],
        '원자재_금속': ['금', '은', '구리', '알루미늄', '철광석'],
        '원자재_농산물': ['옥수수', '소맥(밀)'],
        '각국기준금리': ['Fed기준금리(미국)', 'ECB기준금리(유럽)', 'BOJ기준금리(일본)', 'BOE기준금리(영국)', '중국기준금리'],
        '각국국채금리': ['미국채10Y', '독일국채10Y', '영국국채10Y', '일본국채10Y'],
        '미국금리': ['미국채2Y', '미국채10Y', '미국채30Y', '10Y-2Y스프레드(경기침체신호)'],
        '환율': ['달러인덱스', 'EUR/USD', 'USD/JPY', 'USD/KRW', 'USD/CNY'],
        '미국물가': ['미국CPI(물가)', '미국Core_CPI', '미국PCE(물가)', '미국PPI(생산자물가)'],
        '미국고용': ['미국실업률', '신규실업수당청구'],
        '미국경기': ['ISM제조업지수', '미국소비자신뢰', '소매판매'],
        '유동성': ['M2통화량', 'Fed총자산', 'HY스프레드(신용위험)'],
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
        limit: int = None
    ) -> Optional[pd.DataFrame]:
        """단일 시리즈 조회"""
        
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        cache_key = f"series_{series_id}_{start_date}_{end_date}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            df = pd.DataFrame(cached)
            if limit:
                return df.tail(limit)
            return df
        
        params = {
            'series_id': series_id,
            'api_key': self.api_key,
            'file_type': 'json',
            'observation_start': start_date,
            'observation_end': end_date,
            'sort_order': 'desc',
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
            
            if limit:
                return df.tail(limit)
            return df
            
        except Exception as e:
            self.logger.warning(f"FRED [{series_id}]: {e}")
            return None
    
    def get_latest_value(self, name: str) -> Optional[Dict]:
        """지표의 최신값만 조회"""
        if name not in self.SERIES:
            return None
        
        series_id = self.SERIES[name]
        df = self.get_series(series_id, limit=1)
        
        if df is not None and not df.empty:
            row = df.iloc[0]
            return {
                'indicator': name,
                'date': row['date'],
                'value': row['value'],
                'series_id': series_id
            }
        return None
    
    def get_yoy_change(self, name: str) -> Optional[Dict]:
        """전년대비 변화율 계산 (CPI, PPI 등)"""
        if name not in self.SERIES:
            return None
        
        series_id = self.SERIES[name]
        df = self.get_series(series_id, limit=13)  # 최근 13개월
        
        if df is not None and len(df) >= 2:
            df = df.sort_values('date')
            latest = df.iloc[-1]['value']
            year_ago = df.iloc[0]['value']
            
            if year_ago and year_ago != 0:
                yoy = ((latest - year_ago) / year_ago) * 100
                return {
                    'indicator': name,
                    'date': df.iloc[-1]['date'],
                    'value': latest,
                    'yoy_change': round(yoy, 2),
                    'series_id': series_id
                }
        return None
    
    def collect_summary_data(self) -> pd.DataFrame:
        """거시경제 요약 데이터 수집 (최신값 + YoY)"""
        results = []
        
        # 1. 금리류 - 최신값만
        rate_indicators = [
            'Fed기준금리(미국)', 'ECB기준금리(유럽)', 'BOJ기준금리(일본)', 
            '중국기준금리', '미국채2Y', '미국채10Y', '미국채30Y',
            '독일국채10Y', '영국국채10Y', '일본국채10Y',
            '10Y-2Y스프레드(경기침체신호)', 'HY스프레드(신용위험)'
        ]
        for name in rate_indicators:
            data = self.get_latest_value(name)
            if data:
                data['type'] = '금리'
                data['category'] = '금리'
                results.append(data)
        
        # 2. 물가류 - YoY 변화율
        inflation_indicators = ['미국CPI(물가)', '미국Core_CPI', '미국PCE(물가)', '미국PPI(생산자물가)']
        for name in inflation_indicators:
            data = self.get_yoy_change(name)
            if data:
                data['type'] = '물가(YoY%)'
                data['category'] = '물가'
                results.append(data)
        
        # 3. 고용 - 최신값/4주치
        self.logger.info("  수집: 미국실업률")
        data = self.get_latest_value('미국실업률')
        if data:
            data['type'] = '고용'
            data['category'] = '고용'
            results.append(data)
        
        self.logger.info("  수집: 신규실업수당청구(4주)")
        df = self.get_series(self.SERIES['신규실업수당청구'], limit=4)
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                results.append({
                    'indicator': '신규실업수당청구',
                    'date': row['date'],
                    'value': row['value'],
                    'type': '고용(주간)',
                    'category': '고용'
                })
        
        # 4. 지수류 - 최신값
        index_indicators = ['S&P500', '다우존스', '나스닥', 'VIX(공포지수)', '비트코인',
                           'WTI원유', '금', '구리', '달러인덱스', 'USD/KRW']
        for name in index_indicators:
            self.logger.info(f"  수집: {name}")
            data = self.get_latest_value(name)
            if data:
                data['type'] = '시장지표'
                data['category'] = '시장'
                results.append(data)
        
        # 5. 경기지표 - 최신값  
        econ_indicators = ['ISM제조업지수', '미국소비자신뢰', '소매판매']
        for name in econ_indicators:
            self.logger.info(f"  수집: {name}")
            data = self.get_latest_value(name)
            if data:
                data['type'] = '경기지표'
                data['category'] = '경기'
                results.append(data)
        
        if results:
            df = pd.DataFrame(results)
            df['source'] = 'FRED'
            return df
        
        return pd.DataFrame()
    
    def collect_all_indicators(
        self,
        start_date: str = None,
        end_date: str = None,
        categories: List[str] = None,
        summary_mode: bool = True
    ) -> pd.DataFrame:
        """전체 지표 수집"""
        
        if summary_mode:
            # 요약 모드: 최신값/YoY만 수집
            self.logger.info("🌍 거시경제 요약 데이터 수집...")
            return self.collect_summary_data()
        
        # 전체 시계열 모드
        if categories is None:
            categories = list(self.CATEGORIES.keys())
        
        all_data = []
        
        for cat in categories:
            if cat not in self.CATEGORIES:
                continue
            self.logger.info(f"🌍 {cat} 지표 수집 중...")
            
            for name in self.CATEGORIES[cat]:
                self.logger.info(f"  수집: {name}")
                if name not in self.SERIES:
                    continue
                
                df = self.get_series(self.SERIES[name], start_date, end_date)
                if df is not None and not df.empty:
                    df['indicator'] = name
                    df['category'] = cat
                    df['series_id'] = self.SERIES[name]
                    all_data.append(df)
        
        if not all_data:
            return pd.DataFrame()
        
        result = pd.concat(all_data, ignore_index=True)
        result['source'] = 'FRED'
        
        self.logger.info(f"✓ 총 {len(result)} 행 글로벌 지표 수집")
        return result
    
    def collect(self, start: str = None, end: str = None, categories: List[str] = None) -> pd.DataFrame:
        """BaseCollector 인터페이스"""
        return self.collect_all_indicators(start, end, categories, summary_mode=True)
