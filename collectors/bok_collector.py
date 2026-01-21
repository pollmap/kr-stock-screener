"""
한국은행 ECOS API 수집기 (최신값 전용 v2)
- 30개+ 한국 거시경제 지표
- 시계열 없음, 최신값만 반환
"""

import requests
import pandas as pd
from typing import Optional, Dict, List
from datetime import datetime
import logging

from .base_collector import BaseCollector, retry

logger = logging.getLogger("kr_stock_collector.bok")


class BOKCollector(BaseCollector):
    """한국은행 ECOS API 수집기 (최신값 전용)"""
    
    BASE_URL = "https://ecos.bok.or.kr/api/StatisticSearch"
    
    # ===== 30개+ 한국 경제지표 =====
    # 형식: (통계표코드, 항목코드1, 항목코드2, 주기)
    INDICATORS = {
        # 금리 (8개)
        '기준금리': ('722Y001', 'I010K', '*', 'M'),
        '콜금리(1일)': ('817Y002', 'I010D', '*', 'M'),
        'CD금리(91일)': ('817Y002', 'I020D', '*', 'M'),
        'CP금리(91일)': ('817Y002', 'I030D', '*', 'M'),
        '국고채3년': ('817Y002', 'I020G', '*', 'M'),
        '국고채5년': ('817Y002', 'I020H', '*', 'M'),
        '국고채10년': ('817Y002', 'I020I', '*', 'M'),
        '회사채AA-': ('817Y002', 'I030A', '*', 'M'),
        
        # 물가 (5개)
        '소비자물가지수': ('901Y009', '*', '*', 'M'),
        '생산자물가지수': ('901Y010', '*', '*', 'M'),
        '수출물가지수': ('901Y011', 'AA', '*', 'M'),
        '수입물가지수': ('901Y012', 'AA', '*', 'M'),
        '근원물가지수': ('901Y009', 'CB', '*', 'M'),
        
        # 통화 (5개)
        'M1(협의통화)': ('101Y002', 'BBGA00', '*', 'M'),
        'M2(광의통화)': ('101Y003', 'BBGA00', '*', 'M'),
        'Lf(금융기관유동성)': ('101Y004', 'BBGA00', '*', 'M'),
        '본원통화': ('101Y001', 'BBGA00', '*', 'M'),
        '가계신용': ('151Y002', 'BLCA', '*', 'Q'),
        
        # 경기 (6개)
        '경기선행지수': ('901Y067', 'I11D', '*', 'M'),
        '경기동행지수': ('901Y067', 'I21D', '*', 'M'),
        '경기후행지수': ('901Y067', 'I31D', '*', 'M'),
        '제조업BSI': ('512Y014', 'I001', '*', 'M'),
        '소비자심리지수': ('511Y002', 'FME', '*', 'M'),
        '기업경기실사지수': ('512Y014', 'I001', '*', 'M'),
        
        # 무역 (4개)
        '수출금액': ('403Y003', '*', '*', 'M'),
        '수입금액': ('403Y004', '*', '*', 'M'),
        '무역수지': ('301Y017', 'AA', '*', 'M'),
        '경상수지': ('301Y013', 'AA', '*', 'M'),
        
        # 고용 (3개)
        '실업률': ('901Y027', '*', '*', 'M'),
        '고용률': ('901Y028', '*', '*', 'M'),
        '경제활동참가율': ('901Y029', '*', '*', 'M'),
    }
    
    CATEGORIES = {
        '금리': ['기준금리', '콜금리(1일)', 'CD금리(91일)', 'CP금리(91일)', 
                '국고채3년', '국고채5년', '국고채10년', '회사채AA-'],
        '물가': ['소비자물가지수', '생산자물가지수', '수출물가지수', '수입물가지수', '근원물가지수'],
        '통화': ['M1(협의통화)', 'M2(광의통화)', 'Lf(금융기관유동성)', '본원통화', '가계신용'],
        '경기': ['경기선행지수', '경기동행지수', '경기후행지수', '제조업BSI', '소비자심리지수'],
        '무역': ['수출금액', '수입금액', '무역수지', '경상수지'],
        '고용': ['실업률', '고용률', '경제활동참가율'],
    }
    
    def __init__(self, api_key: str, cache_dir: str = "cache"):
        super().__init__(
            name="bok",
            cache_dir=cache_dir,
            cache_expiry_days=1,
            rate_limit_per_minute=50
        )
        self.api_key = api_key
    
    def _get_date_range(self, freq: str) -> tuple:
        """주기에 따른 날짜 범위"""
        now = datetime.now()
        
        if freq == 'M':
            # 최근 3개월
            end = now.strftime('%Y%m')
            start = (now.replace(day=1) - pd.DateOffset(months=3)).strftime('%Y%m')
        elif freq == 'Q':
            # 최근 2분기
            q = (now.month - 1) // 3 + 1
            end = f"{now.year}Q{q}"
            start = f"{now.year - 1}Q{q}"
        else:
            # 최근 1년
            end = now.strftime('%Y')
            start = str(now.year - 1)
        
        return start, end
    
    @retry(max_attempts=2, delay=0.5)
    def _fetch_indicator(self, name: str) -> Optional[Dict]:
        """단일 지표 최신값 조회"""
        if name not in self.INDICATORS:
            return None
        
        stat_code, item1, item2, freq = self.INDICATORS[name]
        start, end = self._get_date_range(freq)
        
        url = f"{self.BASE_URL}/{self.api_key}/json/kr/1/10/{stat_code}/{freq}/{start}/{end}/{item1}/{item2}"
        
        try:
            response = self._make_request('GET', url, timeout=10)
            data = response.json()
            
            if 'StatisticSearch' not in data:
                return None
            
            rows = data['StatisticSearch'].get('row', [])
            if not rows:
                return None
            
            # 가장 최신 데이터
            latest = rows[-1]
            
            return {
                'indicator': name,
                'date': latest.get('TIME', ''),
                'value': float(latest.get('DATA_VALUE', 0)),
            }
            
        except Exception as e:
            self.logger.warning(f"BOK [{name}]: {e}")
            return None
    
    def collect_all_indicators(self) -> pd.DataFrame:
        """모든 지표 최신값 수집"""
        results = []
        
        for cat, indicators in self.CATEGORIES.items():
            self.logger.info(f"🇰🇷 {cat} 지표 수집 중...")
            
            for name in indicators:
                self.logger.info(f"  수집: {name}")
                data = self._fetch_indicator(name)
                if data:
                    data['category'] = cat
                    results.append(data)
        
        if results:
            df = pd.DataFrame(results)
            df['source'] = 'BOK'
            self.logger.info(f"✓ 총 {len(df)}개 한국 지표 수집")
            return df
        
        return pd.DataFrame()
    
    def collect(self) -> pd.DataFrame:
        """BaseCollector 인터페이스"""
        return self.collect_all_indicators()
