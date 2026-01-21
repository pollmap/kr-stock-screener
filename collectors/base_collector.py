"""
공통 수집기 베이스 클래스
- 에러 핸들링
- 재시도 로직
- 캐싱 지원
"""

import os
import json
import hashlib
import logging
import requests
import time
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from functools import wraps

from utils.rate_limiter import RateLimiter

logger = logging.getLogger("kr_stock_collector.base")


def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    재시도 데코레이터
    
    Args:
        max_attempts: 최대 시도 횟수
        delay: 초기 대기 시간 (초)
        backoff: 대기 시간 증가 배수
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            current_delay = delay
            
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts == max_attempts:
                        logger.error(f"{func.__name__} 최종 실패: {e}")
                        raise
                    
                    logger.warning(
                        f"{func.__name__} 재시도 {attempts}/{max_attempts}: {e}"
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            return None
        return wrapper
    return decorator


class BaseCollector(ABC):
    """
    데이터 수집기 베이스 클래스
    
    모든 수집기가 상속해야 하는 추상 클래스
    """
    
    def __init__(
        self,
        name: str,
        cache_dir: str = "cache",
        cache_expiry_days: int = 7,
        rate_limit_per_minute: int = 100
    ):
        """
        Args:
            name: 수집기 이름
            cache_dir: 캐시 디렉토리
            cache_expiry_days: 캐시 만료 일수
            rate_limit_per_minute: 분당 API 호출 제한
        """
        self.name = name
        self.cache_dir = cache_dir
        self.cache_expiry_days = cache_expiry_days
        self.rate_limiter = RateLimiter(calls_per_minute=rate_limit_per_minute)
        
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'KRStockCollector/1.0'
        })
        
        os.makedirs(cache_dir, exist_ok=True)
        
        self.logger = logging.getLogger(f"kr_stock_collector.{name}")
    
    def _get_cache_path(self, key: str) -> str:
        """캐시 파일 경로 생성"""
        hash_key = hashlib.md5(key.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{self.name}_{hash_key}.json")
    
    def _get_from_cache(self, key: str) -> Optional[Any]:
        """캐시에서 데이터 조회"""
        cache_path = self._get_cache_path(key)
        
        if not os.path.exists(cache_path):
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cached = json.load(f)
            
            # 만료 체크
            cached_time = datetime.fromisoformat(cached.get('timestamp', ''))
            if datetime.now() - cached_time > timedelta(days=self.cache_expiry_days):
                os.remove(cache_path)
                return None
            
            self.logger.debug(f"캐시 히트: {key}")
            return cached.get('data')
            
        except Exception as e:
            self.logger.warning(f"캐시 읽기 실패: {e}")
            return None
    
    def _save_to_cache(self, key: str, data: Any) -> None:
        """캐시에 데이터 저장"""
        cache_path = self._get_cache_path(key)
        
        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'timestamp': datetime.now().isoformat(),
                    'data': data
                }, f, ensure_ascii=False, indent=2)
            
            self.logger.debug(f"캐시 저장: {key}")
            
        except Exception as e:
            self.logger.warning(f"캐시 저장 실패: {e}")
    
    def _make_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        timeout: int = 30
    ) -> requests.Response:
        """
        HTTP 요청 수행 (rate limiting 포함)
        
        Args:
            method: HTTP 메서드 (GET/POST)
            url: 요청 URL
            params: 쿼리 파라미터
            data: POST 데이터
            headers: 추가 헤더
            timeout: 타임아웃 (초)
        
        Returns:
            Response 객체
        """
        # Rate limit 대기
        if not self.rate_limiter.wait():
            raise Exception("일일 API 호출 한도 초과")
        
        if headers:
            self._session.headers.update(headers)
        
        if method.upper() == 'GET':
            response = self._session.get(url, params=params, timeout=timeout)
        elif method.upper() == 'POST':
            response = self._session.post(
                url, params=params, json=data, timeout=timeout
            )
        else:
            raise ValueError(f"지원하지 않는 HTTP 메서드: {method}")
        
        return response
    
    @abstractmethod
    def collect(self, *args, **kwargs):
        """데이터 수집 (하위 클래스에서 구현)"""
        pass
    
    def close(self) -> None:
        """세션 종료"""
        self._session.close()
