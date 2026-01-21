"""
한국은행 경제통계시스템 API (Pro-Level)
30개+ 국내 거시경제 지표:
- 금리 (8개)
- 물가 (5개)
- 통화 (5개)
- 경기 (6개)
- 무역 (4개)
- 고용 (3개)
"""

import requests
import pandas as pd
from typing import Optional, List, Dict
import logging
import time

from .base_collector import BaseCollector, retry

logger = logging.getLogger("kr_stock_collector.bok")


class BOKCollector(BaseCollector):
    """
    한국은행 경제통계시스템 API 수집기 (Pro-Level)
    
    30개+ 국내 거시경제 지표 수집
    """
    
    BASE_URL = "https://ecos.bok.or.kr/api"
    
    # =====================================
    # 주요 통계표 코드 (확장판)
    # =====================================
    STAT_CODES = {
        # 금리 (8개)
        '기준금리': ('722Y001', '0101000', 'M'),
        '콜금리_1일': ('817Y002', 'D21A00', 'D'),
        'CD금리_91일': ('817Y002', 'D12B10', 'D'),
        'CP금리_91일': ('817Y002', 'D14A10', 'D'),
        '국고채_1년': ('817Y002', 'D46A11', 'D'),
        '국고채_3년': ('817Y002', 'D46A13', 'D'),
        '국고채_5년': ('817Y002', 'D46A15', 'D'),
        '국고채_10년': ('817Y002', 'D46A10', 'D'),
        '회사채_AA-': ('817Y002', 'D53A00', 'D'),
        '회사채_BBB-': ('817Y002', 'D53B00', 'D'),
        '가계대출금리': ('121Y006', 'BEEALB', 'M'),
        '기업대출금리': ('121Y006', 'BEEALA', 'M'),
        
        # 물가 (5개)
        'CPI': ('901Y009', '0', 'M'),
        'CPI_근원': ('901Y010', 'DA', 'M'),
        'PPI': ('404Y014', '*AA', 'M'),
        '수출물가지수': ('401Y015', '*AA', 'M'),
        '수입물가지수': ('401Y016', '*AA', 'M'),
        
        # 통화량 (5개)
        'M1': ('101Y004', 'BBGS00', 'M'),
        'M2': ('101Y004', 'BBHS00', 'M'),
        'Lf': ('101Y004', 'BBJS00', 'M'),
        '본원통화': ('101Y003', 'BBKS00', 'M'),
        '가계신용': ('151Y001', 'I05A0A', 'Q'),
        '기업대출': ('104Y034', 'BBBA10', 'M'),
        
        # 경기/심리 (6개)
        '경기선행지수': ('901Y067', 'I16E', 'M'),
        '경기동행지수': ('901Y067', 'I16C', 'M'),
        '경기후행지수': ('901Y067', 'I16L', 'M'),
        '소비자심리지수': ('511Y002', 'FME', 'M'),
        '기업경기실사지수_제조업': ('512Y007', 'BA', 'M'),
        '기업경기실사지수_비제조업': ('512Y007', 'NA', 'M'),
        
        # 무역/국제수지 (4개)
        '수출금액': ('403Y003', '110', 'M'),
        '수입금액': ('403Y003', '120', 'M'),
        '무역수지': ('403Y003', '100', 'M'),
        '경상수지': ('301Y013', 'CA', 'M'),
        
        # 고용 (3개)
        '실업률': ('901Y027', '36301', 'M'),
        '고용률': ('901Y027', '36201', 'M'),
        '경제활동참가율': ('901Y027', '36101', 'M'),
        
        # 환율 (4개)
        '원달러환율': ('731Y003', '0000001', 'D'),
        '원엔환율': ('731Y003', '0000002', 'D'),
        '원유로환율': ('731Y003', '0000003', 'D'),
        '원위안환율': ('731Y003', '0000053', 'D'),
        
        # 부동산 (2개)
        '주택매매가격지수': ('901Y062', 'P63AA', 'M'),
        '전세가격지수': ('901Y062', 'P64AA', 'M'),
        
        # 산업생산 (2개)
        '광공업생산지수': ('901Y033', 'I31A', 'M'),
        '서비스업생산지수': ('901Y033', 'I33A', 'M'),
    }
    
    # 카테고리별 그룹핑
    CATEGORIES = {
        '금리': ['기준금리', '콜금리_1일', 'CD금리_91일', '국고채_3년', '국고채_10년', 
                '회사채_AA-', '가계대출금리', '기업대출금리'],
        '물가': ['CPI', 'CPI_근원', 'PPI', '수출물가지수', '수입물가지수'],
        '통화': ['M1', 'M2', 'Lf', '본원통화', '가계신용'],
        '경기': ['경기선행지수', '경기동행지수', '소비자심리지수', '기업경기실사지수_제조업'],
        '무역': ['수출금액', '수입금액', '무역수지', '경상수지'],
        '고용': ['실업률', '고용률', '경제활동참가율'],
        '환율': ['원달러환율', '원엔환율', '원유로환율'],
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
    
    @retry(max_attempts=3, delay=1.0)
    def get_stat_data(
        self,
        stat_code: str,
        item_code: str,
        start_date: str,
        end_date: str,
        cycle: str = 'M'
    ) -> Optional[pd.DataFrame]:
        """
        통계 데이터 조회
        
        Args:
            stat_code: 통계표 코드
            item_code: 항목코드
            start_date: 시작일 (YYYYMM 또는 YYYYMMDD)
            end_date: 종료일
            cycle: D(일), M(월), Q(분기), A(연)
        """
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
                
                # 필요 컬럼만
                keep_cols = ['TIME', 'DATA_VALUE', 'STAT_NAME', 'ITEM_NAME1', 'UNIT_NAME']
                df = df[[c for c in keep_cols if c in df.columns]]
                
                df['DATA_VALUE'] = pd.to_numeric(df['DATA_VALUE'], errors='coerce')
                
                self._save_to_cache(cache_key, df.to_dict('records'))
                return df
            else:
                result = data.get('RESULT', {})
                self.logger.warning(f"BOK API: {result.get('MESSAGE', 'Unknown')}")
                return None
                
        except Exception as e:
            self.logger.error(f"BOK API 실패: {e}")
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
        if indicator_name not in self.STAT_CODES:
            self.logger.warning(f"알 수 없는 지표: {indicator_name}")
            return None
        
        stat_code, item_code, cycle = self.STAT_CODES[indicator_name]
        
        df = self.get_stat_data(stat_code, item_code, start_date, end_date, cycle)
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
        
        Args:
            category: '금리', '물가', '통화', '경기', '무역', '고용', '환율'
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
            time.sleep(0.1)
        
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
        전체/선택 거시경제 지표 수집
        
        Args:
            start_date: 시작월 (YYYYMM)
            end_date: 종료월
            categories: 수집할 카테고리 (None이면 전체)
        """
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
        """사용 가능한 지표 목록 반환"""
        return self.CATEGORIES
    
    def collect(self, start: str, end: str, categories: List[str] = None) -> pd.DataFrame:
        """BaseCollector 인터페이스"""
        return self.collect_all_indicators(start, end, categories)
