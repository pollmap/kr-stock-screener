"""
OpenDART API 수집기
- 기업 고유번호 조회 및 캐싱
- 단일/다중 회사 재무제표 조회
- 연결/개별 재무제표 자동 전환

주의사항:
1. corp_code는 반드시 8자리 문자열로 패딩
2. API 호출 간 0.1초 이상 대기
3. status '000'이 정상 응답
"""

import os
import io
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import time
from typing import Optional, List, Dict

from .base_collector import BaseCollector, retry


# 보고서 코드 정의
REPORT_CODES = {
    '11011': '사업보고서',      # 연간 (결산 후 90일)
    '11012': '반기보고서',      # 6월 (반기 후 45일)
    '11013': '1분기보고서',     # 3월 (분기 후 45일)
    '11014': '3분기보고서'      # 9월 (분기 후 45일)
}

# API 상태 코드
STATUS_CODES = {
    '000': '정상',
    '010': '등록되지 않은 키',
    '011': '사용할 수 없는 키',
    '013': '요청 제한 초과',
    '020': '요청 파라미터 오류',
    '800': '데이터 없음',
    '900': '정의되지 않은 오류'
}


class OpenDartCollector(BaseCollector):
    """
    OpenDART API 수집기
    
    국내 상장사 재무제표, 공시정보 수집
    """
    
    BASE_URL = "https://opendart.fss.or.kr/api"
    
    def __init__(self, api_key: str, cache_dir: str = "cache"):
        """
        Args:
            api_key: OpenDART API 인증키
            cache_dir: 캐시 디렉토리
        """
        super().__init__(
            name="opendart",
            cache_dir=cache_dir,
            cache_expiry_days=30,  # 기업코드는 한 달 캐시
            rate_limit_per_minute=100  # 분당 100회 (여유있게)
        )
        self.api_key = api_key
        self.corp_code_map: Dict[str, str] = {}  # stock_code -> corp_code
        self.corp_name_map: Dict[str, str] = {}  # corp_code -> corp_name
        
        self._load_corp_codes()
    
    def _load_corp_codes(self) -> None:
        """기업 고유번호 목록 로드 (캐시 우선)"""
        cache_path = os.path.join(self.cache_dir, "corp_codes.csv")
        
        try:
            # 캐시에서 로드 시도
            df = pd.read_csv(cache_path, dtype=str)
            self.corp_code_map = dict(zip(df['stock_code'], df['corp_code']))
            self.corp_name_map = dict(zip(df['corp_code'], df['corp_name']))
            self.logger.info(f"캐시에서 {len(self.corp_code_map)}개 기업 코드 로드")
            return
        except FileNotFoundError:
            pass
        
        self.logger.info("OpenDART에서 기업 고유번호 다운로드 중...")
        
        # API에서 다운로드
        url = f"{self.BASE_URL}/corpCode.xml"
        
        try:
            response = self._make_request('GET', url, params={'crtfc_key': self.api_key})
            
            if response.status_code != 200:
                raise Exception(f"기업코드 다운로드 실패: {response.status_code}")
            
            # ZIP 파일 해제
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                xml_content = zf.read('CORPCODE.xml').decode('utf-8')
            
            # XML 파싱
            root = ET.fromstring(xml_content)
            records = []
            
            for corp in root.findall('list'):
                corp_code = corp.findtext('corp_code', '')
                corp_name = corp.findtext('corp_name', '')
                stock_code = corp.findtext('stock_code', '')
                modify_date = corp.findtext('modify_date', '')
                
                if stock_code and stock_code.strip():  # 상장사만
                    records.append({
                        'corp_code': corp_code.zfill(8),  # 8자리 패딩
                        'corp_name': corp_name,
                        'stock_code': stock_code.strip(),
                        'modify_date': modify_date
                    })
            
            df = pd.DataFrame(records)
            df.to_csv(cache_path, index=False)
            
            self.corp_code_map = dict(zip(df['stock_code'], df['corp_code']))
            self.corp_name_map = dict(zip(df['corp_code'], df['corp_name']))
            
            self.logger.info(f"API에서 {len(self.corp_code_map)}개 상장사 코드 다운로드 완료")
            
        except Exception as e:
            self.logger.error(f"기업 코드 로드 실패: {e}")
            raise
    
    def get_corp_code(self, stock_code: str) -> Optional[str]:
        """종목코드 -> 기업고유번호 변환"""
        return self.corp_code_map.get(stock_code)
    
    def get_corp_name(self, corp_code: str) -> Optional[str]:
        """기업고유번호 -> 기업명 조회"""
        return self.corp_name_map.get(corp_code)
    
    def get_all_stock_codes(self) -> List[str]:
        """모든 상장사 종목코드 리스트 반환"""
        return list(self.corp_code_map.keys())
    
    @retry(max_attempts=3, delay=1.0)
    def get_financial_statement(
        self,
        corp_code: str,
        bsns_year: str,
        reprt_code: str = '11011',
        fs_div: str = 'CFS'
    ) -> Optional[pd.DataFrame]:
        """
        단일회사 전체 재무제표 조회
        
        Args:
            corp_code: 기업고유번호 (8자리)
            bsns_year: 사업연도 (예: '2023')
            reprt_code: 보고서코드 (11011=사업보고서)
            fs_div: CFS(연결) / OFS(개별)
        
        Returns:
            재무제표 DataFrame 또는 None
        """
        # 캐시 키 생성
        cache_key = f"fs_{corp_code}_{bsns_year}_{reprt_code}_{fs_div}"
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return pd.DataFrame(cached)
        
        url = f"{self.BASE_URL}/fnlttSinglAcntAll.json"
        
        params = {
            'crtfc_key': self.api_key,
            'corp_code': corp_code.zfill(8),  # ⚠️ 8자리 패딩 필수
            'bsns_year': str(bsns_year),
            'reprt_code': reprt_code,
            'fs_div': fs_div
        }
        
        try:
            response = self._make_request('GET', url, params=params)
            data = response.json()
            
            # ⚠️ status 체크 필수
            status = data.get('status', '')
            
            if status != '000':
                status_msg = STATUS_CODES.get(status, '알 수 없는 오류')
                
                # 데이터 없음 (800)인 경우
                if status == '800':
                    # 연결재무제표 없으면 개별 시도
                    if fs_div == 'CFS':
                        self.logger.debug(
                            f"연결재무제표 없음 [{corp_code}], 개별 시도"
                        )
                        return self.get_financial_statement(
                            corp_code, bsns_year, reprt_code, 'OFS'
                        )
                    return None
                
                self.logger.warning(
                    f"API 오류 [{corp_code}]: {status} - {status_msg}"
                )
                return None
            
            df = pd.DataFrame(data.get('list', []))
            
            if not df.empty:
                df['corp_code'] = corp_code
                df['bsns_year'] = bsns_year
                df['reprt_code'] = reprt_code
                df['fs_div'] = fs_div
                
                # 캐시 저장
                self._save_to_cache(cache_key, df.to_dict('records'))
            
            return df
            
        except Exception as e:
            self.logger.error(f"재무제표 조회 실패 [{corp_code}]: {e}")
            raise
    
    @retry(max_attempts=3, delay=1.0)
    def get_single_account(
        self,
        corp_code: str,
        bsns_year: str,
        reprt_code: str = '11011'
    ) -> Optional[pd.DataFrame]:
        """
        단일회사 주요계정 조회 (간단 버전)
        
        fnlttSinglAcnt API 사용
        """
        url = f"{self.BASE_URL}/fnlttSinglAcnt.json"
        
        params = {
            'crtfc_key': self.api_key,
            'corp_code': corp_code.zfill(8),
            'bsns_year': str(bsns_year),
            'reprt_code': reprt_code
        }
        
        try:
            response = self._make_request('GET', url, params=params)
            data = response.json()
            
            if data.get('status') == '000':
                df = pd.DataFrame(data.get('list', []))
                df['corp_code'] = corp_code
                return df
            
            return None
            
        except Exception as e:
            self.logger.error(f"주요계정 조회 실패 [{corp_code}]: {e}")
            return None
    
    @retry(max_attempts=3, delay=2.0)
    def get_multi_company_accounts(
        self,
        corp_codes: List[str],
        bsns_year: str,
        reprt_code: str = '11011'
    ) -> Optional[pd.DataFrame]:
        """
        다중회사 주요계정 조회 (최대 100개 기업)
        
        ⚠️ 주의: API 1회 호출로 100개 기업까지만 가능
        """
        url = f"{self.BASE_URL}/fnlttMultiAcnt.json"
        
        all_data = []
        
        # 100개씩 분할
        for i in range(0, len(corp_codes), 100):
            batch = corp_codes[i:i+100]
            
            params = {
                'crtfc_key': self.api_key,
                'corp_code': ','.join([c.zfill(8) for c in batch]),
                'bsns_year': str(bsns_year),
                'reprt_code': reprt_code
            }
            
            try:
                response = self._make_request('GET', url, params=params, timeout=60)
                data = response.json()
                
                if data.get('status') == '000':
                    all_data.extend(data.get('list', []))
                else:
                    self.logger.warning(
                        f"배치 조회 실패 ({i}~{i+100}): {data.get('message')}"
                    )
                    
            except Exception as e:
                self.logger.error(f"다중회사 조회 실패: {e}")
        
        return pd.DataFrame(all_data) if all_data else None
    
    def collect_all_financials(
        self,
        stock_codes: List[str],
        years: List[str],
        reprt_codes: List[str] = None,
        use_multi_api: bool = True
    ) -> pd.DataFrame:
        """
        전 종목 재무제표 일괄 수집
        
        Args:
            stock_codes: 종목코드 리스트
            years: 사업연도 리스트 (예: ['2021', '2022', '2023'])
            reprt_codes: 보고서코드 (기본: 사업보고서만)
            use_multi_api: 다중회사 API 사용 여부
        
        Returns:
            통합 재무제표 DataFrame
        """
        if reprt_codes is None:
            reprt_codes = ['11011']  # 사업보고서
        
        all_data = []
        
        if use_multi_api:
            # 다중회사 API 사용 (효율적)
            corp_codes = [
                self.get_corp_code(sc) for sc in stock_codes 
                if self.get_corp_code(sc)
            ]
            
            for year in years:
                for reprt_code in reprt_codes:
                    self.logger.info(
                        f"수집 중: {year}년 {REPORT_CODES.get(reprt_code, reprt_code)}"
                    )
                    
                    df = self.get_multi_company_accounts(corp_codes, year, reprt_code)
                    if df is not None and not df.empty:
                        all_data.append(df)
        else:
            # 단일회사 API 사용 (상세 데이터)
            total = len(stock_codes) * len(years) * len(reprt_codes)
            count = 0
            
            for stock_code in stock_codes:
                corp_code = self.get_corp_code(stock_code)
                if not corp_code:
                    self.logger.warning(f"corp_code 없음: {stock_code}")
                    continue
                
                for year in years:
                    for reprt_code in reprt_codes:
                        count += 1
                        if count % 50 == 0:
                            self.logger.info(
                                f"진행률: {count}/{total} ({count/total*100:.1f}%)"
                            )
                        
                        df = self.get_financial_statement(corp_code, year, reprt_code)
                        if df is not None and not df.empty:
                            df['stock_code'] = stock_code
                            all_data.append(df)
        
        if not all_data:
            return pd.DataFrame()
        
        result = pd.concat(all_data, ignore_index=True)
        self.logger.info(f"총 {len(result)} 행 수집 완료")
        
        return result
    
    def collect(self, stock_codes: List[str], years: List[str]) -> pd.DataFrame:
        """BaseCollector 인터페이스 구현"""
        return self.collect_all_financials(stock_codes, years)
