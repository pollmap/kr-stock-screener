"""
한국은행 ECOS API 수집기 (v4 - 수정된 항목코드)
- 정확한 ECOS API 항목코드 적용
- 부분 실패 허용
"""

import requests
import pandas as pd
from typing import Optional, Dict, List
from datetime import datetime
import logging

from .base_collector import BaseCollector, retry

logger = logging.getLogger("kr_stock_collector.bok")


class BOKCollector(BaseCollector):
    """한국은행 ECOS API 수집기 (수정된 항목코드)"""
    
    BASE_URL = "https://ecos.bok.or.kr/api/StatisticSearch"
    
    # ===== 수정된 한국 경제지표 (올바른 ECOS 코드) =====
    # 형식: (통계표코드, 항목코드1, 항목코드2, 주기)
    # 항목코드2가 필요없으면 빈 문자열
    INDICATORS = {
        # 금리 (6개)
        '기준금리': ('722Y001', '0101000', '', 'M'),
        'CD금리(91일)': ('817Y002', '010502000', '', 'D'),
        '국고채3년': ('817Y002', '010200000', '', 'D'),
        '국고채5년': ('817Y002', '010200001', '', 'D'),
        '국고채10년': ('817Y002', '010210000', '', 'D'),
        '회사채AA-': ('817Y002', '010300000', '', 'D'),
        
        # 물가 (3개)
        '소비자물가지수': ('901Y009', '0', '', 'M'),
        '근원물가지수': ('901Y009', 'CB', '', 'M'),
        '생산자물가지수': ('901Y010', 'AA', '', 'M'),
        
        # 통화 (3개)
        'M2(광의통화)': ('101Y003', 'BBGA00', '', 'M'),
        '본원통화': ('101Y001', 'BBGA00', '', 'M'),
        '가계신용': ('151Y002', 'BLCA', '', 'Q'),
        
        # 경기 (4개)
        '경기선행지수': ('901Y067', 'I16B', '', 'M'),
        '경기동행지수': ('901Y067', 'I16C', '', 'M'),
        '소비자심리지수': ('511Y002', 'FME', '', 'M'),
        'BSI(제조업)': ('512Y014', 'A001', '', 'M'),
        
        # 무역 (3개)
        '수출금액': ('403Y001', 'A', '', 'M'),
        '수입금액': ('403Y001', 'B', '', 'M'),
        '경상수지': ('301Y013', 'CA', '', 'M'),
        
        # 고용 (2개)
        '실업률': ('901Y027', '1', '', 'M'),
        '고용률': ('901Y028', '1', '', 'M'),
    }
    
    CATEGORIES = {
        '금리': ['기준금리', 'CD금리(91일)', '국고채3년', '국고채5년', '국고채10년', '회사채AA-'],
        '물가': ['소비자물가지수', '근원물가지수', '생산자물가지수'],
        '통화': ['M2(광의통화)', '본원통화', '가계신용'],
        '경기': ['경기선행지수', '경기동행지수', '소비자심리지수', 'BSI(제조업)'],
        '무역': ['수출금액', '수입금액', '경상수지'],
        '고용': ['실업률', '고용률'],
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
        
        if freq == 'D':
            # 최근 1개월
            end = now.strftime('%Y%m%d')
            start = (now - pd.DateOffset(months=1)).strftime('%Y%m%d')
        elif freq == 'M':
            # 최근 6개월
            end = now.strftime('%Y%m')
            start = (now - pd.DateOffset(months=6)).strftime('%Y%m')
        elif freq == 'Q':
            # 최근 4분기
            q = (now.month - 1) // 3 + 1
            end = f"{now.year}Q{q}"
            start = f"{now.year - 1}Q{q}"
        else:
            end = now.strftime('%Y')
            start = str(now.year - 1)
        
        return start, end
    
    @retry(max_attempts=3, delay=1.0)
    def _fetch_indicator(self, name: str) -> Optional[Dict]:
        """단일 지표 최신값 조회"""
        if name not in self.INDICATORS:
            return None
        
        stat_code, item1, item2, freq = self.INDICATORS[name]
        start, end = self._get_date_range(freq)
        
        # URL 구성 - item2가 빈 문자열이면 생략
        if item2:
            url = f"{self.BASE_URL}/{self.api_key}/json/kr/1/100/{stat_code}/{freq}/{start}/{end}/{item1}/{item2}"
        else:
            url = f"{self.BASE_URL}/{self.api_key}/json/kr/1/100/{stat_code}/{freq}/{start}/{end}/{item1}"
        
        try:
            response = self._make_request('GET', url, timeout=15)
            data = response.json()
            
            # 응답 확인
            if 'StatisticSearch' not in data:
                error_msg = data.get('RESULT', {}).get('MESSAGE', 'Unknown')
                self.logger.debug(f"BOK [{name}]: {error_msg}")
                return None
            
            rows = data['StatisticSearch'].get('row', [])
            if not rows:
                return None
            
            # 가장 최신 데이터
            latest = rows[-1]
            
            try:
                value = float(latest.get('DATA_VALUE', 0))
            except:
                value = 0
            
            return {
                'indicator': name,
                'date': latest.get('TIME', ''),
                'value': value,
            }
            
        except Exception as e:
            self.logger.debug(f"BOK [{name}] 오류: {e}")
            return None
    
    def collect_all_indicators(self) -> pd.DataFrame:
        """모든 지표 최신값 수집 (부분 실패 허용)"""
        results = []
        success = 0
        fail = 0
        
        for cat, indicators in self.CATEGORIES.items():
            self.logger.info(f"🇰🇷 {cat} 지표 수집 중...")
            
            for name in indicators:
                self.logger.info(f"  수집: {name}")
                data = self._fetch_indicator(name)
                if data:
                    data['category'] = cat
                    data['source'] = 'BOK'
                    results.append(data)
                    success += 1
                else:
                    fail += 1
        
        self.logger.info(f"BOK 수집 완료: 성공 {success}개 / 실패 {fail}개")
        
        if results:
            df = pd.DataFrame(results)
            return df
        
        self.logger.warning("BOK 지표 수집 결과 없음")
        return pd.DataFrame()
    
    def collect(self) -> pd.DataFrame:
        """BaseCollector 인터페이스"""
        return self.collect_all_indicators()
