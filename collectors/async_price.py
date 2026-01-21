"""
비동기 주가 수집기 (v2.0)
- FinanceDataReader 기반
- 증분 업데이트 지원
"""

import asyncio
import aiohttp
from typing import List, Dict, Optional, Any
import pandas as pd
from datetime import datetime, timedelta
import logging

from .async_base import AsyncBaseCollector

logger = logging.getLogger("kr_stock_collector.async_price")


class AsyncPriceCollector(AsyncBaseCollector):
    """비동기 주가 수집기"""
    
    # 네이버 금융 API 베이스
    NAVER_API = "https://api.finance.naver.com/siseJson.naver"
    
    def __init__(self):
        super().__init__(
            name="async_price",
            rate_limit=20,        # 초당 20회
            max_retries=2,
            timeout=15,
            max_concurrent=30
        )
    
    async def fetch_single(
        self,
        session: aiohttp.ClientSession,
        item: Dict
    ) -> Optional[Dict]:
        """단일 종목 주가 수집"""
        code = item.get('code', '')
        start_date = item.get('start_date', '2024-01-01')
        
        # 네이버 금융 시세 API
        params = {
            'symbol': code,
            'requestType': 1,
            'startTime': start_date.replace('-', ''),
            'endTime': datetime.now().strftime('%Y%m%d'),
            'timeframe': 'day'
        }
        
        try:
            await self.rate_limiter.acquire()
            
            async with session.get(
                self.NAVER_API,
                params=params,
                headers={'User-Agent': 'Mozilla/5.0'}
            ) as response:
                if response.status != 200:
                    return None
                
                text = await response.text()
                
                # 간단한 파싱 (실제로는 더 정교하게)
                if not text or 'error' in text.lower():
                    return None
                
                return {
                    'code': code,
                    'data': text,
                    'collected_at': datetime.now().isoformat()
                }
                
        except Exception as e:
            self.logger.debug(f"[{code}] 수집 실패: {e}")
            return None


class AsyncMarketCapCollector(AsyncBaseCollector):
    """비동기 시가총액 수집기 (KRX)"""
    
    KRX_API = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    
    def __init__(self):
        super().__init__(
            name="async_marketcap",
            rate_limit=5,
            max_retries=3,
            timeout=30,
            max_concurrent=5
        )
    
    async def fetch_all_marketcap(
        self,
        session: aiohttp.ClientSession,
        market: str = 'ALL'
    ) -> List[Dict]:
        """전 종목 시가총액 일괄 수집"""
        
        today = datetime.now().strftime('%Y%m%d')
        
        data = {
            'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
            'mktId': market,
            'trdDd': today,
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false'
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        try:
            async with session.post(
                self.KRX_API,
                data=data,
                headers=headers
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    return result.get('OutBlock_1', [])
        except Exception as e:
            self.logger.error(f"KRX API 오류: {e}")
        
        return []
    
    async def fetch_single(
        self,
        session: aiohttp.ClientSession,
        item: Any
    ) -> Optional[Dict]:
        """단일 수집 (사용 안함)"""
        pass


async def collect_all_prices(
    stock_codes: List[str],
    start_date: str = None,
    progress_callback=None
) -> pd.DataFrame:
    """전 종목 주가 수집 (메인 함수)"""
    
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    items = [{'code': code, 'start_date': start_date} for code in stock_codes]
    
    collector = AsyncPriceCollector()
    results = await collector.collect_all(items, batch_size=100, progress_callback=progress_callback)
    
    if results:
        return pd.DataFrame(results)
    
    return pd.DataFrame()


async def collect_all_marketcap() -> pd.DataFrame:
    """전 종목 시가총액 수집"""
    
    collector = AsyncMarketCapCollector()
    
    connector = aiohttp.TCPConnector(limit=5)
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:
        results = await collector.fetch_all_marketcap(session)
    
    if results:
        df = pd.DataFrame(results)
        # 컬럼명 정리
        col_map = {
            'ISU_SRT_CD': 'code',
            'ISU_ABBRV': 'name',
            'MKT_NM': 'market',
            'SECT_TP_NM': 'sector',
            'TDD_CLSPRC': 'close',
            'FLUC_RT': 'change',
            'ACC_TRDVOL': 'volume',
            'MKTCAP': 'market_cap',
            'LIST_SHRS': 'shares'
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        return df
    
    return pd.DataFrame()


if __name__ == '__main__':
    # 테스트
    async def test():
        df = await collect_all_marketcap()
        print(f"시가총액 수집: {len(df)}건")
        if not df.empty:
            print(df.head())
    
    asyncio.run(test())
