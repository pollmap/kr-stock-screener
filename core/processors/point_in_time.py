"""
Point-in-Time 데이터 관리자 (v3.0)
- 백테스팅 Look-Ahead Bias 방지
- 특정 시점에 알 수 있었던 데이터만 반환
"""

import pandas as pd
from datetime import date, datetime
from typing import Optional, List, Dict
import logging

logger = logging.getLogger("kr_stock_collector.pit")


class PointInTimeManager:
    """
    Point-in-Time 데이터 관리
    
    핵심 원칙:
    - 재무제표는 '공표일(announced_at)' 기준으로 조회
    - 2023년 실적은 2024년 3월에 공표됨
    - 2024년 1월 시점에서는 2022년 실적까지만 알 수 있음
    """
    
    # 한국 공시 일정 (대략적)
    ANNOUNCEMENT_DELAYS = {
        'FY': 90,     # 사업보고서: 회계연도 종료 후 90일
        '3Q': 45,     # 3분기보고서: 분기 종료 후 45일
        '2Q': 45,     # 2분기보고서
        '1Q': 45,     # 1분기보고서
    }
    
    def __init__(self, financials_df: pd.DataFrame = None):
        """
        Args:
            financials_df: 재무제표 DataFrame (announced_at 컬럼 필수)
        """
        self.financials = financials_df
    
    def estimate_announcement_date(
        self,
        fiscal_year: str,
        fiscal_quarter: str = 'FY'
    ) -> date:
        """
        공표일 추정 (announced_at이 없을 경우)
        
        Example:
            2023년 FY → 2024년 3월 말
            2023년 3Q → 2023년 11월 중순
        """
        year = int(fiscal_year)
        delay = self.ANNOUNCEMENT_DELAYS.get(fiscal_quarter, 45)
        
        if fiscal_quarter == 'FY':
            # 연간: 다음해 3월 말
            return date(year + 1, 3, 31)
        elif fiscal_quarter == '1Q':
            # 1분기: 5월 중순
            return date(year, 5, 15)
        elif fiscal_quarter == '2Q':
            # 2분기: 8월 중순
            return date(year, 8, 15)
        elif fiscal_quarter == '3Q':
            # 3분기: 11월 중순
            return date(year, 11, 15)
        else:
            return date(year + 1, 3, 31)
    
    def get_available_financials(
        self,
        stock_code: str,
        as_of_date: date
    ) -> pd.DataFrame:
        """
        특정 시점에 알 수 있었던 재무제표만 반환
        
        Args:
            stock_code: 종목코드
            as_of_date: 기준일 (이 날짜에 투자자가 알 수 있었던 정보만)
        
        Returns:
            Point-in-Time 기준 재무제표
        """
        if self.financials is None or self.financials.empty:
            return pd.DataFrame()
        
        # 종목 필터
        code_col = 'stock_code' if 'stock_code' in self.financials.columns else '종목코드'
        df = self.financials[self.financials[code_col] == stock_code].copy()
        
        if df.empty:
            return df
        
        # announced_at 없으면 추정
        if 'announced_at' not in df.columns:
            df['announced_at'] = df.apply(
                lambda x: self.estimate_announcement_date(
                    x.get('fiscal_year', x.get('bsns_year', '')),
                    x.get('fiscal_quarter', 'FY')
                ),
                axis=1
            )
        
        # as_of_date 이전에 공표된 것만
        df['announced_at'] = pd.to_datetime(df['announced_at']).dt.date
        available = df[df['announced_at'] <= as_of_date]
        
        logger.debug(f"[{stock_code}] as_of {as_of_date}: {len(available)}/{len(df)} 건 사용가능")
        
        return available
    
    def get_latest_available(
        self,
        stock_code: str,
        as_of_date: date
    ) -> Optional[pd.Series]:
        """
        특정 시점 기준 최신 재무제표 1건
        """
        available = self.get_available_financials(stock_code, as_of_date)
        
        if available.empty:
            return None
        
        # 가장 최신 공표일 기준
        return available.sort_values('announced_at', ascending=False).iloc[0]
    
    def get_ttm_available(
        self,
        stock_code: str,
        as_of_date: date
    ) -> Dict[str, float]:
        """
        특정 시점 기준 TTM 계산
        
        Returns:
            TTM 재무데이터 딕셔너리
        """
        available = self.get_available_financials(stock_code, as_of_date)
        
        # 분기 데이터만 필터 (1Q, 2Q, 3Q, 4Q)
        quarter_col = 'fiscal_quarter' if 'fiscal_quarter' in available.columns else None
        if quarter_col and quarter_col in available.columns:
            quarters = available[available[quarter_col].isin(['1Q', '2Q', '3Q', '4Q'])]
            quarters = quarters.sort_values('announced_at', ascending=False).head(4)
            
            if len(quarters) >= 4:
                return {
                    'revenue_ttm': quarters['revenue'].sum() if 'revenue' in quarters.columns else None,
                    'operating_income_ttm': quarters['operating_income'].sum() if 'operating_income' in quarters.columns else None,
                    'net_income_ttm': quarters['net_income'].sum() if 'net_income' in quarters.columns else None,
                }
        
        return {}


def validate_no_lookahead(
    trade_date: date,
    data_date: date,
    announced_date: date = None
) -> bool:
    """
    Look-Ahead Bias 검증
    
    Args:
        trade_date: 매매일
        data_date: 데이터 기준일 (예: 2023년 실적)
        announced_date: 공표일 (예: 2024-03-31)
    
    Returns:
        True if 사용 가능 (Bias 없음)
    """
    if announced_date is None:
        # 보수적 추정: 연말 실적은 다음해 4월까지 알 수 없음
        announced_date = date(data_date.year + 1, 4, 1)
    
    is_valid = trade_date >= announced_date
    
    if not is_valid:
        logger.warning(
            f"Look-Ahead Bias 감지: 매매일 {trade_date}에 "
            f"{data_date} 데이터(공표일: {announced_date}) 사용 불가"
        )
    
    return is_valid


if __name__ == '__main__':
    # 테스트
    pit = PointInTimeManager()
    
    # 2024년 1월 1일 시점에서 2023년 실적은 아직 공표 전
    est_date = pit.estimate_announcement_date('2023', 'FY')
    print(f"2023년 사업보고서 공표 예상일: {est_date}")
    
    # Look-Ahead 검증
    is_ok = validate_no_lookahead(
        trade_date=date(2024, 1, 15),
        data_date=date(2023, 12, 31)
    )
    print(f"2024-01-15에 2023년 실적 사용 가능? {is_ok}")  # False
