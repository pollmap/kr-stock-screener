"""
컨센서스 데이터 수집기 (v3.0)
- 애널리스트 추정치 크롤링
- 네이버 금융 기반
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import Dict, Optional, List
from datetime import datetime
import logging
import re

logger = logging.getLogger("kr_stock_collector.consensus")


class ConsensusCollector:
    """
    컨센서스(애널리스트 추정치) 수집기
    
    수집 항목:
    - 목표주가 (평균/최고/최저)
    - Forward EPS
    - 투자의견 (Buy/Hold/Sell)
    - 애널리스트 수
    """
    
    NAVER_FINANCE_URL = "https://finance.naver.com/item/main.naver"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def collect_consensus(self, stock_code: str) -> Dict:
        """
        종목별 컨센서스 수집
        
        Args:
            stock_code: 종목코드 (6자리)
        
        Returns:
            컨센서스 데이터 딕셔너리
        """
        try:
            url = f"{self.NAVER_FINANCE_URL}?code={stock_code}"
            response = self.session.get(url, timeout=10)
            
            if response.status_code != 200:
                logger.warning(f"[{stock_code}] HTTP {response.status_code}")
                return {}
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            result = {
                'stock_code': stock_code,
                'collected_at': datetime.now().date()
            }
            
            # 목표주가 영역 파싱
            target_area = soup.select_one('em.target')
            if target_area:
                target_text = target_area.get_text(strip=True)
                target_match = re.search(r'([\d,]+)', target_text)
                if target_match:
                    result['target_price'] = int(target_match.group(1).replace(',', ''))
            
            # 애널리스트 수
            analyst_area = soup.select_one('.analyst_count')
            if analyst_area:
                count_match = re.search(r'(\d+)', analyst_area.get_text())
                if count_match:
                    result['analyst_count'] = int(count_match.group(1))
            
            # 투자의견
            opinion_area = soup.select_one('.consensus_opinion')
            if opinion_area:
                result['recommendation'] = opinion_area.get_text(strip=True)
            
            logger.debug(f"[{stock_code}] 컨센서스 수집 완료")
            return result
            
        except Exception as e:
            logger.error(f"[{stock_code}] 컨센서스 수집 오류: {e}")
            return {}
    
    def collect_batch(self, stock_codes: List[str]) -> pd.DataFrame:
        """
        다중 종목 컨센서스 수집
        """
        results = []
        
        for code in stock_codes:
            data = self.collect_consensus(code)
            if data:
                results.append(data)
        
        if results:
            return pd.DataFrame(results)
        
        return pd.DataFrame()
    
    def get_earnings_surprise(
        self,
        stock_code: str,
        actual_eps: float,
        estimate_eps: float
    ) -> Dict:
        """
        실적 서프라이즈 계산
        
        Args:
            actual_eps: 실제 EPS
            estimate_eps: 추정 EPS
        
        Returns:
            surprise %, beat/miss
        """
        if estimate_eps == 0:
            return {'surprise_pct': 0, 'result': 'N/A'}
        
        surprise = (actual_eps - estimate_eps) / abs(estimate_eps) * 100
        
        if surprise > 5:
            result = 'Beat'
        elif surprise < -5:
            result = 'Miss'
        else:
            result = 'In-Line'
        
        return {
            'stock_code': stock_code,
            'actual_eps': actual_eps,
            'estimate_eps': estimate_eps,
            'surprise_pct': round(surprise, 2),
            'result': result
        }


def filter_earnings_surprises(
    stocks_df: pd.DataFrame,
    min_surprise: float = 5.0
) -> pd.DataFrame:
    """
    실적 서프라이즈 종목 필터링
    
    Args:
        stocks_df: 종목 데이터 (actual_eps, estimate_eps 컬럼 필요)
        min_surprise: 최소 서프라이즈 % (기본 5%)
    
    Returns:
        서프라이즈 종목만
    """
    if stocks_df is None or stocks_df.empty:
        return stocks_df
    
    result = stocks_df.copy()
    
    if 'actual_eps' in result.columns and 'estimate_eps' in result.columns:
        result['surprise_pct'] = (
            (result['actual_eps'] - result['estimate_eps']) / 
            result['estimate_eps'].abs() * 100
        )
        result = result[result['surprise_pct'] >= min_surprise]
        result = result.sort_values('surprise_pct', ascending=False)
    
    return result


if __name__ == '__main__':
    # 테스트
    collector = ConsensusCollector()
    
    # 삼성전자 컨센서스
    data = collector.collect_consensus('005930')
    print("삼성전자 컨센서스:", data)
    
    # 서프라이즈 계산
    surprise = collector.get_earnings_surprise(
        '005930',
        actual_eps=5000,
        estimate_eps=4500
    )
    print("실적 서프라이즈:", surprise)
