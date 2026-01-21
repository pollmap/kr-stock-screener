"""
한국투자증권 Open API 수집기
주요 기능: 실시간 시세, 일봉/주봉, 투자지표
"""

import requests
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List
import pandas as pd
import logging

from .base_collector import BaseCollector, retry

logger = logging.getLogger("kr_stock_collector.kis")


class KISCollector(BaseCollector):
    """
    한국투자증권 Open API 수집기
    
    실시간 시세, 일봉 데이터, 투자자별 매매동향 수집
    OAuth 토큰 기반 인증
    """
    
    BASE_URL = "https://openapi.koreainvestment.com:9443"
    
    def __init__(
        self,
        app_key: str,
        app_secret: str,
        cache_dir: str = "cache"
    ):
        """
        Args:
            app_key: 한국투자증권 AppKey
            app_secret: 한국투자증권 AppSecret
            cache_dir: 캐시 디렉토리
        """
        super().__init__(
            name="kis",
            cache_dir=cache_dir,
            cache_expiry_days=1,
            rate_limit_per_minute=20  # KIS는 제한이 빡빡
        )
        self.app_key = app_key
        self.app_secret = app_secret
        self.access_token: Optional[str] = None
        self.token_expires: Optional[datetime] = None
    
    def _get_token(self) -> str:
        """
        OAuth 토큰 발급 (24시간 유효)
        
        Returns:
            access_token
        """
        # 유효한 토큰이 있으면 재사용
        if self.access_token and self.token_expires:
            if datetime.now() < self.token_expires:
                return self.access_token
        
        url = f"{self.BASE_URL}/oauth2/tokenP"
        headers = {"Content-Type": "application/json"}
        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret
        }
        
        try:
            response = requests.post(url, headers=headers, json=body, timeout=30)
            data = response.json()
            
            if 'access_token' in data:
                self.access_token = data['access_token']
                # 23시간 후 만료 (여유 확보)
                self.token_expires = datetime.now() + timedelta(hours=23)
                self.logger.info("KIS 토큰 발급 완료")
                return self.access_token
            else:
                error = data.get('error_description', 'Unknown error')
                raise Exception(f"토큰 발급 실패: {error}")
                
        except Exception as e:
            self.logger.error(f"KIS 토큰 발급 실패: {e}")
            raise
    
    def _get_headers(self, tr_id: str) -> Dict:
        """
        API 호출 헤더 생성
        
        Args:
            tr_id: 거래 ID (API 엔드포인트별 고유값)
        """
        return {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self._get_token()}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id
        }
    
    @retry(max_attempts=3, delay=1.0)
    def get_current_price(self, stock_code: str) -> Optional[Dict]:
        """
        주식 현재가 조회
        
        Args:
            stock_code: 종목코드 (6자리)
        
        Returns:
            현재가 정보 dict
        """
        url = f"{self.BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        headers = self._get_headers("FHKST01010100")
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",  # 주식
            "FID_INPUT_ISCD": stock_code
        }
        
        try:
            if not self.rate_limiter.wait():
                raise Exception("일일 호출 한도 초과")
            
            response = requests.get(url, headers=headers, params=params, timeout=30)
            data = response.json()
            
            if data.get('rt_cd') == '0':
                return data.get('output', {})
            else:
                self.logger.warning(
                    f"현재가 조회 실패 [{stock_code}]: {data.get('msg1')}"
                )
                return None
                
        except Exception as e:
            self.logger.error(f"현재가 조회 실패 [{stock_code}]: {e}")
            return None
    
    @retry(max_attempts=3, delay=1.0)
    def get_daily_price(
        self,
        stock_code: str,
        period: str = 'D',
        adj_price: bool = True
    ) -> Optional[pd.DataFrame]:
        """
        일/주/월봉 시세 조회
        
        Args:
            stock_code: 종목코드 (6자리)
            period: 'D'(일봉), 'W'(주봉), 'M'(월봉)
            adj_price: 수정주가 적용 여부
        
        Returns:
            OHLCV DataFrame
        """
        url = f"{self.BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-price"
        headers = self._get_headers("FHKST01010400")
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code,
            "FID_PERIOD_DIV_CODE": period,
            "FID_ORG_ADJ_PRC": "0" if adj_price else "1"
        }
        
        try:
            if not self.rate_limiter.wait():
                raise Exception("일일 호출 한도 초과")
            
            response = requests.get(url, headers=headers, params=params, timeout=30)
            data = response.json()
            
            if data.get('rt_cd') == '0':
                output = data.get('output', [])
                if output:
                    df = pd.DataFrame(output)
                    df['stock_code'] = stock_code
                    return df
            
            return None
            
        except Exception as e:
            self.logger.error(f"일봉 조회 실패 [{stock_code}]: {e}")
            return None
    
    @retry(max_attempts=3, delay=1.0)
    def get_investor_trend(
        self,
        stock_code: str
    ) -> Optional[pd.DataFrame]:
        """
        투자자별 매매동향 조회
        
        Args:
            stock_code: 종목코드
        
        Returns:
            투자자별 순매수 DataFrame
        """
        url = f"{self.BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-investor"
        headers = self._get_headers("FHKST01010900")
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code
        }
        
        try:
            if not self.rate_limiter.wait():
                raise Exception("일일 호출 한도 초과")
            
            response = requests.get(url, headers=headers, params=params, timeout=30)
            data = response.json()
            
            if data.get('rt_cd') == '0':
                output = data.get('output', [])
                if output:
                    df = pd.DataFrame(output)
                    df['stock_code'] = stock_code
                    return df
            
            return None
            
        except Exception as e:
            self.logger.error(f"투자자 동향 조회 실패 [{stock_code}]: {e}")
            return None
    
    def get_stock_info(self, stock_code: str) -> Optional[Dict]:
        """
        종목 기본 정보 조회
        """
        url = f"{self.BASE_URL}/uapi/domestic-stock/v1/quotations/search-stock-info"
        headers = self._get_headers("CTPF1002R")
        params = {
            "PRDT_TYPE_CD": "300",  # 주식
            "PDNO": stock_code
        }
        
        try:
            if not self.rate_limiter.wait():
                return None
            
            response = requests.get(url, headers=headers, params=params, timeout=30)
            data = response.json()
            
            if data.get('rt_cd') == '0':
                return data.get('output', {})
            return None
            
        except Exception as e:
            self.logger.error(f"종목 정보 조회 실패 [{stock_code}]: {e}")
            return None
    
    def collect_batch_prices(
        self,
        stock_codes: List[str]
    ) -> pd.DataFrame:
        """
        다수 종목 현재가 일괄 조회
        
        ⚠️ Rate limit 주의: 분당 20건
        """
        all_data = []
        total = len(stock_codes)
        
        for i, code in enumerate(stock_codes):
            if (i + 1) % 10 == 0:
                self.logger.info(f"현재가 조회 진행: {i+1}/{total}")
            
            price_info = self.get_current_price(code)
            if price_info:
                price_info['stock_code'] = code
                all_data.append(price_info)
        
        if not all_data:
            return pd.DataFrame()
        
        return pd.DataFrame(all_data)
    
    def collect(self, stock_codes: List[str]) -> pd.DataFrame:
        """BaseCollector 인터페이스 구현"""
        return self.collect_batch_prices(stock_codes)
