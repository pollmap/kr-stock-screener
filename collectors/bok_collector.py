"""
한국은행 경제통계시스템 API (수정판)
- 월간 데이터 위주로 안정적인 지표만 수집
- 일간 데이터는 날짜 형식 문제로 제외
"""

import requests
import pandas as pd
from typing import Optional, List, Dict
import logging
import time

from .base_collector import BaseCollector, retry

logger = logging.getLogger("kr_stock_collector.bok")


class BOKCollector(BaseCollector):
    """한국은행 경제통계시스템 API 수집기 (안정판)"""
    
    BASE_URL = "https://ecos.bok.or.kr/api"
    
    # 월간(M) 데이터만 사용 (안정적)
    STAT_CODES = {
        # 금리 (월간)
        '기준금리': ('722Y001', '0101000', 'M'),
        
        # 물가 (월간)
        'CPI': ('901Y009', '0', 'M'),
        'PPI': ('404Y014', '*AA', 'M'),
        '수출물가지수': ('401Y015', '*AA', 'M'),
        '수입물가지수': ('401Y016', '*AA', 'M'),
        
        # 통화량 (월간)
        'M1': ('101Y004', 'BBGS00', 'M'),
        'M2': ('101Y004', 'BBHS00', 'M'),
        'Lf': ('101Y004', 'BBJS00', 'M'),
        '본원통화': ('101Y003', 'BBKS00', 'M'),
        
        # 경기 (월간)
        '경기선행지수': ('901Y067', 'I16E', 'M'),
        '경기동행지수': ('901Y067', 'I16C', 'M'),
        '소비자심리지수': ('511Y002', 'FME', 'M'),
        '기업경기실사지수': ('512Y007', 'BA', 'M'),
        
        # 고용 (월간)
        '실업률': ('901Y027', '36301', 'M'),
        '고용률': ('901Y027', '36201', 'M'),
        '경제활동참가율': ('901Y027', '36101', 'M'),
        
        # 부동산 (월간)
        '주택매매가격지수': ('901Y062', 'P63AA', 'M'),
        '전세가격지수': ('901Y062', 'P64AA', 'M'),
        
        # 산업생산 (월간)
        '광공업생산지수': ('901Y033', 'I31A', 'M'),
        '서비스업생산지수': ('901Y033', 'I33A', 'M'),
    }
    
    # 카테고리
    CATEGORIES = {
        '금리': ['기준금리'],
        '물가': ['CPI', 'PPI', '수출물가지수', '수입물가지수'],
        '통화': ['M1', 'M2', 'Lf', '본원통화'],
        '경기': ['경기선행지수', '경기동행지수', '소비자심리지수', '기업경기실사지수'],
        '고용': ['실업률', '고용률', '경제활동참가율'],
        '부동산': ['주택매매가격지수', '전세가격지수'],
    }
    
    def __init__(self, api_key: str, cache_dir: str = "cache"):
        super().__init__(
            name="bok",
            cache_dir=cache_dir,
            cache_expiry_days=1,
            rate_limit_per_minute=50
        )
        self.api_key = api_key
    
    @retry(max_attempts=2, delay=0.5)
    def get_stat_data(
        self,
        stat_code: str,
        item_code: str,
        start_date: str,
        end_date: str,
        cycle: str = 'M'
    ) -> Optional[pd.DataFrame]:
        """통계 데이터 조회"""
        cache_key = f"stat_{stat_code}_{item_code}_{start_date}_{end_date}_{cycle}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return pd.DataFrame(cached)
        
        url = f"{self.BASE_URL}/StatisticSearch/{self.api_key}/json/kr/1/1000/{stat_code}/{cycle}/{start_date}/{end_date}/{item_code}"
        
        try:
            response = self._make_request('GET', url, timeout=30)
            data = response.json()
            
            if 'StatisticSearch' in data:
                rows = data['StatisticSearch'].get('row', [])
                if not rows:
                    return None
                
                df = pd.DataFrame(rows)
                keep_cols = ['TIME', 'DATA_VALUE', 'STAT_NAME', 'ITEM_NAME1', 'UNIT_NAME']
                df = df[[c for c in keep_cols if c in df.columns]]
                df['DATA_VALUE'] = pd.to_numeric(df['DATA_VALUE'], errors='coerce')
                
                self._save_to_cache(cache_key, df.to_dict('records'))
                return df
            else:
                return None
                
        except Exception as e:
            self.logger.warning(f"BOK [{stat_code}]: {e}")
            return None
    
    def get_indicator(self, name: str, start: str, end: str) -> Optional[pd.DataFrame]:
        """단일 지표 조회"""
        if name not in self.STAT_CODES:
            return None
        
        stat_code, item_code, cycle = self.STAT_CODES[name]
        df = self.get_stat_data(stat_code, item_code, start, end, cycle)
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
            time.sleep(0.1)
        
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
            self.logger.info(f"📊 {cat} 지표 수집 중...")
            df = self.collect_category(cat, start_date, end_date)
            if not df.empty:
                all_data.append(df)
        
        if not all_data:
            return pd.DataFrame()
        
        result = pd.concat(all_data, ignore_index=True)
        self.logger.info(f"✓ 총 {len(result)} 행 한국경제 지표 수집")
        return result
    
    def get_available_indicators(self) -> Dict[str, List[str]]:
        return self.CATEGORIES
    
    def collect(self, start: str, end: str, categories: List[str] = None) -> pd.DataFrame:
        return self.collect_all_indicators(start, end, categories)
