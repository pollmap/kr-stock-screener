"""
API 호출 속도 제한 모듈
- 데코레이터 기반 rate limiting
- 클래스 기반 rate limiter
"""

import time
import threading
from functools import wraps
from typing import Callable, Any
from collections import deque


def rate_limit(calls_per_minute: int = 100) -> Callable:
    """
    API 호출 속도 제한 데코레이터
    
    Args:
        calls_per_minute: 분당 최대 호출 횟수
    
    Returns:
        데코레이터 함수
    
    Example:
        @rate_limit(calls_per_minute=100)
        def api_call():
            ...
    """
    min_interval = 60.0 / calls_per_minute
    last_call = [0.0]
    lock = threading.Lock()
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            with lock:
                elapsed = time.time() - last_call[0]
                if elapsed < min_interval:
                    time.sleep(min_interval - elapsed)
                result = func(*args, **kwargs)
                last_call[0] = time.time()
                return result
        return wrapper
    return decorator


class RateLimiter:
    """
    슬라이딩 윈도우 기반 Rate Limiter
    
    분당 호출 횟수를 제한하며, 호출 이력을 관리
    """
    
    def __init__(self, calls_per_minute: int = 100, daily_limit: int = 10000):
        """
        Args:
            calls_per_minute: 분당 최대 호출 횟수
            daily_limit: 일일 최대 호출 횟수
        """
        self.calls_per_minute = calls_per_minute
        self.daily_limit = daily_limit
        self.min_interval = 60.0 / calls_per_minute
        
        self.call_times: deque = deque()
        self.daily_count = 0
        self.daily_reset_time = time.time()
        
        self._lock = threading.Lock()
    
    def wait(self) -> bool:
        """
        호출 전 대기 (필요시)
        
        Returns:
            True if can proceed, False if daily limit exceeded
        """
        with self._lock:
            current_time = time.time()
            
            # 일일 리셋 체크 (24시간)
            if current_time - self.daily_reset_time > 86400:
                self.daily_count = 0
                self.daily_reset_time = current_time
            
            # 일일 한도 체크
            if self.daily_count >= self.daily_limit:
                return False
            
            # 1분 윈도우 밖의 호출 제거
            window_start = current_time - 60.0
            while self.call_times and self.call_times[0] < window_start:
                self.call_times.popleft()
            
            # 분당 한도 체크 및 대기
            if len(self.call_times) >= self.calls_per_minute:
                wait_time = self.call_times[0] + 60.0 - current_time
                if wait_time > 0:
                    time.sleep(wait_time)
            
            # 최소 간격 대기
            if self.call_times:
                elapsed = current_time - self.call_times[-1]
                if elapsed < self.min_interval:
                    time.sleep(self.min_interval - elapsed)
            
            # 호출 기록
            self.call_times.append(time.time())
            self.daily_count += 1
            
            return True
    
    def get_remaining_daily_calls(self) -> int:
        """남은 일일 호출 횟수 반환"""
        return max(0, self.daily_limit - self.daily_count)
    
    def reset(self) -> None:
        """카운터 리셋"""
        with self._lock:
            self.call_times.clear()
            self.daily_count = 0
            self.daily_reset_time = time.time()
