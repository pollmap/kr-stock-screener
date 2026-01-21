"""
비동기 수집기 베이스 클래스 (v2.0)
- asyncio + aiohttp 기반
- 자동 재시도 + Exponential Backoff
- Rate Limiting
"""

import asyncio
import aiohttp
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any
import logging
from datetime import datetime
import time

logger = logging.getLogger("kr_stock_collector.async_base")


class AsyncRateLimiter:
    """비동기 Rate Limiter"""
    
    def __init__(self, calls_per_second: float = 5):
        self.calls_per_second = calls_per_second
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)
            self.last_call = time.time()


class AsyncBaseCollector(ABC):
    """비동기 수집기 베이스 클래스"""
    
    def __init__(
        self,
        name: str,
        rate_limit: float = 10,      # 초당 호출 수
        max_retries: int = 3,
        timeout: int = 30,
        max_concurrent: int = 20     # 최대 동시 요청
    ):
        self.name = name
        self.rate_limiter = AsyncRateLimiter(rate_limit)
        self.max_retries = max_retries
        self.timeout = timeout
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.logger = logging.getLogger(f"kr_stock_collector.{name}")
        
        # 통계
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'start_time': None,
            'end_time': None
        }
    
    async def _request(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        **kwargs
    ) -> Optional[Dict]:
        """HTTP 요청 (재시도 포함)"""
        await self.rate_limiter.acquire()
        
        for attempt in range(self.max_retries):
            try:
                async with self.semaphore:
                    async with session.request(
                        method, url, timeout=self.timeout, **kwargs
                    ) as response:
                        if response.status == 200:
                            return await response.json()
                        else:
                            self.logger.warning(
                                f"HTTP {response.status}: {url[:50]}..."
                            )
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout (attempt {attempt+1}): {url[:50]}...")
            except aiohttp.ClientError as e:
                self.logger.warning(f"Client error (attempt {attempt+1}): {e}")
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                break
            
            # Exponential Backoff
            if attempt < self.max_retries - 1:
                wait_time = (2 ** attempt) * 0.5
                await asyncio.sleep(wait_time)
        
        return None
    
    @abstractmethod
    async def fetch_single(
        self,
        session: aiohttp.ClientSession,
        item: Any
    ) -> Optional[Dict]:
        """단일 아이템 수집 (하위 클래스 구현)"""
        pass
    
    async def fetch_batch(
        self,
        session: aiohttp.ClientSession,
        items: List[Any],
        progress_callback=None
    ) -> List[Dict]:
        """배치 수집"""
        results = []
        tasks = []
        
        for item in items:
            task = asyncio.create_task(self.fetch_single(session, item))
            tasks.append((item, task))
        
        for i, (item, task) in enumerate(tasks):
            try:
                result = await task
                if result:
                    results.append(result)
                    self.stats['success'] += 1
                else:
                    self.stats['failed'] += 1
            except Exception as e:
                self.logger.error(f"Task failed for {item}: {e}")
                self.stats['failed'] += 1
            
            self.stats['total'] += 1
            
            if progress_callback and (i + 1) % 50 == 0:
                progress_callback(i + 1, len(items))
        
        return results
    
    async def collect_all(
        self,
        items: List[Any],
        batch_size: int = 100,
        progress_callback=None
    ) -> List[Dict]:
        """전체 수집 (배치 처리)"""
        self.stats['start_time'] = datetime.now()
        all_results = []
        
        connector = aiohttp.TCPConnector(limit=self.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout
        ) as session:
            
            for i in range(0, len(items), batch_size):
                batch = items[i:i+batch_size]
                self.logger.info(
                    f"배치 {i//batch_size + 1}/{(len(items)-1)//batch_size + 1} "
                    f"({len(batch)}개)"
                )
                
                results = await self.fetch_batch(session, batch, progress_callback)
                all_results.extend(results)
                
                # 배치 간 짧은 휴식
                if i + batch_size < len(items):
                    await asyncio.sleep(0.5)
        
        self.stats['end_time'] = datetime.now()
        elapsed = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        self.logger.info(
            f"수집 완료: {self.stats['success']}/{self.stats['total']} "
            f"({elapsed:.1f}초)"
        )
        
        return all_results
    
    def run(self, items: List[Any], batch_size: int = 100) -> List[Dict]:
        """동기 실행 래퍼"""
        return asyncio.run(self.collect_all(items, batch_size))
