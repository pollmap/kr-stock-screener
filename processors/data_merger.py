"""
데이터 병합 모듈
- 다중 소스 데이터 통합
- 종목코드 기반 조인
- 시계열 데이터 정렬
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
import logging

logger = logging.getLogger("kr_stock_collector.merger")


class DataMerger:
    """
    다중 소스 데이터 병합 클래스
    
    재무제표 + 주가 + 투자지표 + 거시경제 데이터 통합
    """
    
    def __init__(self):
        self.merge_log: List[str] = []
    
    def _log(self, message: str) -> None:
        """병합 로그 기록"""
        self.merge_log.append(message)
        logger.info(message)
    
    def merge_stock_info(
        self,
        stock_list: pd.DataFrame,
        financial_data: pd.DataFrame,
        on: str = 'stock_code'
    ) -> pd.DataFrame:
        """
        종목 기본정보와 재무데이터 병합
        
        Args:
            stock_list: 종목 리스트 (Code, Name, Market, Sector 등)
            financial_data: 재무제표 데이터
            on: 조인 키
        
        Returns:
            병합된 DataFrame
        """
        # 컬럼명 통일
        if 'Code' in stock_list.columns and on == 'stock_code':
            stock_list = stock_list.rename(columns={'Code': 'stock_code'})
        
        # 종목코드 패딩
        stock_list['stock_code'] = stock_list['stock_code'].astype(str).str.zfill(6)
        
        if 'stock_code' in financial_data.columns:
            financial_data['stock_code'] = financial_data['stock_code'].astype(str).str.zfill(6)
        
        # 병합
        merged = pd.merge(
            financial_data,
            stock_list,
            on=on,
            how='left'
        )
        
        self._log(f"종목정보 병합: {len(merged)}건 (원본: {len(financial_data)}건)")
        
        return merged
    
    def merge_price_data(
        self,
        financial_data: pd.DataFrame,
        price_data: pd.DataFrame,
        date_col: str = 'date'
    ) -> pd.DataFrame:
        """
        재무데이터와 주가데이터 병합
        
        Args:
            financial_data: 재무제표 + 종목정보
            price_data: 주가 OHLCV 데이터
            date_col: 날짜 컬럼명
        
        Returns:
            병합된 DataFrame
        """
        # 종목코드 통일
        if 'stock_code' in price_data.columns:
            price_data['stock_code'] = price_data['stock_code'].astype(str).str.zfill(6)
        
        # 최신 주가만 사용 (종목별 최신일)
        if date_col in price_data.columns:
            latest_prices = price_data.sort_values(date_col).groupby('stock_code').last().reset_index()
        else:
            latest_prices = price_data.drop_duplicates(subset=['stock_code'], keep='last')
        
        # 필요 컬럼만 선택
        price_cols = ['stock_code', 'close', 'volume', 'change']
        price_cols = [c for c in price_cols if c in latest_prices.columns]
        latest_prices = latest_prices[price_cols]
        
        # 병합
        merged = pd.merge(
            financial_data,
            latest_prices,
            on='stock_code',
            how='left',
            suffixes=('', '_price')
        )
        
        self._log(f"주가 병합: {len(merged)}건")
        
        return merged
    
    def merge_indicators(
        self,
        financial_data: pd.DataFrame,
        indicator_data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        재무데이터와 투자지표 병합 (PER, PBR 등)
        
        Args:
            financial_data: 재무제표 데이터
            indicator_data: pykrx 투자지표 데이터
        
        Returns:
            병합된 DataFrame
        """
        # 종목코드 통일
        if 'stock_code' in indicator_data.columns:
            indicator_data['stock_code'] = indicator_data['stock_code'].astype(str).str.zfill(6)
        
        # 최신 지표만 사용
        if 'date' in indicator_data.columns:
            latest_indicators = indicator_data.sort_values('date').groupby('stock_code').last().reset_index()
        else:
            latest_indicators = indicator_data.drop_duplicates(subset=['stock_code'], keep='last')
        
        # 필요 컬럼만
        indicator_cols = ['stock_code', 'per', 'pbr', 'eps', 'bps', 'div_yield', 'dps']
        indicator_cols = [c for c in indicator_cols if c in latest_indicators.columns]
        latest_indicators = latest_indicators[indicator_cols]
        
        # 병합
        merged = pd.merge(
            financial_data,
            latest_indicators,
            on='stock_code',
            how='left',
            suffixes=('', '_krx')
        )
        
        self._log(f"투자지표 병합: {len(merged)}건")
        
        return merged
    
    def merge_market_cap(
        self,
        financial_data: pd.DataFrame,
        market_cap_data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        재무데이터와 시가총액 병합
        
        Args:
            financial_data: 재무제표 데이터
            market_cap_data: 시가총액 데이터
        
        Returns:
            병합된 DataFrame
        """
        # 종목코드 통일
        if 'stock_code' in market_cap_data.columns:
            market_cap_data['stock_code'] = market_cap_data['stock_code'].astype(str).str.zfill(6)
        
        # 필요 컬럼만
        cap_cols = ['stock_code', 'market_cap', 'shares']
        cap_cols = [c for c in cap_cols if c in market_cap_data.columns]
        
        if not cap_cols:
            self._log("시가총액 컬럼 없음")
            return financial_data
        
        market_cap_subset = market_cap_data[cap_cols].drop_duplicates(subset=['stock_code'])
        
        # 병합
        merged = pd.merge(
            financial_data,
            market_cap_subset,
            on='stock_code',
            how='left'
        )
        
        self._log(f"시가총액 병합: {len(merged)}건")
        
        return merged
    
    def create_master_dataset(
        self,
        stock_list: pd.DataFrame,
        financial_data: pd.DataFrame,
        price_data: pd.DataFrame = None,
        indicator_data: pd.DataFrame = None,
        market_cap_data: pd.DataFrame = None
    ) -> pd.DataFrame:
        """
        전체 마스터 데이터셋 생성
        
        모든 소스 데이터를 통합하여 분석용 마스터 테이블 생성
        
        Args:
            stock_list: 종목 기본정보
            financial_data: 재무제표
            price_data: 주가 데이터 (옵션)
            indicator_data: 투자지표 (옵션)
            market_cap_data: 시가총액 (옵션)
        
        Returns:
            통합 마스터 DataFrame
        """
        self._log("마스터 데이터셋 생성 시작")
        
        # 1. 종목정보 + 재무제표
        master = self.merge_stock_info(stock_list, financial_data)
        
        # 2. + 주가
        if price_data is not None and not price_data.empty:
            master = self.merge_price_data(master, price_data)
        
        # 3. + 투자지표
        if indicator_data is not None and not indicator_data.empty:
            master = self.merge_indicators(master, indicator_data)
        
        # 4. + 시가총액
        if market_cap_data is not None and not market_cap_data.empty:
            master = self.merge_market_cap(master, market_cap_data)
        
        self._log(f"마스터 데이터셋 생성 완료: {len(master)}건, {len(master.columns)}컬럼")
        
        return master
    
    def pivot_financial_accounts(
        self,
        financial_data: pd.DataFrame,
        index_cols: List[str] = None,
        account_col: str = 'account_nm',
        value_col: str = 'thstrm_amount'
    ) -> pd.DataFrame:
        """
        재무제표 세로 -> 가로 피벗
        
        계정과목이 행으로 되어있는 데이터를 컬럼으로 변환
        
        Args:
            financial_data: 재무제표 (세로형)
            index_cols: 피벗 인덱스 컬럼들
            account_col: 계정과목 컬럼
            value_col: 금액 컬럼
        
        Returns:
            피벗된 DataFrame (가로형)
        """
        if index_cols is None:
            index_cols = ['corp_code', 'stock_code', 'bsns_year']
        
        # 인덱스 컬럼 체크
        available_index = [c for c in index_cols if c in financial_data.columns]
        
        if not available_index or account_col not in financial_data.columns:
            self._log("피벗 불가: 필요 컬럼 없음")
            return financial_data
        
        try:
            pivoted = financial_data.pivot_table(
                index=available_index,
                columns=account_col,
                values=value_col,
                aggfunc='first'
            ).reset_index()
            
            # 컬럼명 정리
            pivoted.columns.name = None
            
            self._log(f"피벗 완료: {len(pivoted)}행 x {len(pivoted.columns)}열")
            
            return pivoted
            
        except Exception as e:
            self._log(f"피벗 실패: {e}")
            return financial_data
    
    def align_time_series(
        self,
        dataframes: List[pd.DataFrame],
        date_col: str = 'date',
        freq: str = 'D'
    ) -> pd.DataFrame:
        """
        시계열 데이터 정렬 및 병합
        
        Args:
            dataframes: 시계열 DataFrame 리스트
            date_col: 날짜 컬럼
            freq: 주기 ('D': 일, 'W': 주, 'M': 월)
        
        Returns:
            정렬된 통합 DataFrame
        """
        if not dataframes:
            return pd.DataFrame()
        
        result = None
        
        for df in dataframes:
            if df.empty or date_col not in df.columns:
                continue
            
            df = df.copy()
            df[date_col] = pd.to_datetime(df[date_col])
            df = df.set_index(date_col)
            
            if result is None:
                result = df
            else:
                result = result.join(df, how='outer', rsuffix='_dup')
        
        if result is not None:
            result = result.reset_index()
            self._log(f"시계열 정렬 완료: {len(result)}건")
        
        return result if result is not None else pd.DataFrame()
    
    def get_merge_report(self) -> str:
        """병합 작업 리포트 반환"""
        return "\n".join(self.merge_log)
