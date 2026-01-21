"""
데이터 정제 모듈
- 결측치 처리
- 이상치 탐지 및 처리
- 데이터 타입 변환
- 중복 제거
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Union
import logging

logger = logging.getLogger("kr_stock_collector.cleaner")


class DataCleaner:
    """
    재무/주가 데이터 정제 클래스
    """
    
    # 재무제표 금액 컬럼
    FINANCIAL_AMOUNT_COLS = [
        'thstrm_amount', 'frmtrm_amount', 'bfefrmtrm_amount',
        '당기순이익', '매출액', '영업이익', '자산총계', '부채총계', '자본총계'
    ]
    
    # 투자지표 수치 컬럼
    INDICATOR_COLS = ['per', 'pbr', 'eps', 'bps', 'dps', 'div_yield']
    
    # 주가 수치 컬럼
    PRICE_COLS = ['open', 'high', 'low', 'close', 'volume', 'value', 'change']
    
    def __init__(self):
        self.cleaning_log: List[str] = []
    
    def _log(self, message: str) -> None:
        """정제 로그 기록"""
        self.cleaning_log.append(message)
        logger.info(message)
    
    def clean_financial_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        재무제표 데이터 정제
        
        Args:
            df: OpenDART 재무제표 DataFrame
        
        Returns:
            정제된 DataFrame
        """
        if df.empty:
            return df
        
        df = df.copy()
        original_len = len(df)
        
        # 1. 중복 제거
        if 'corp_code' in df.columns and 'account_nm' in df.columns:
            df = df.drop_duplicates(
                subset=['corp_code', 'bsns_year', 'reprt_code', 'account_nm', 'fs_div'],
                keep='last'
            )
            self._log(f"중복 제거: {original_len - len(df)}건")
        
        # 2. 금액 컬럼 숫자 변환
        for col in self.FINANCIAL_AMOUNT_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(',', ''),
                    errors='coerce'
                )
        
        # 3. 연도 숫자 변환
        if 'bsns_year' in df.columns:
            df['bsns_year'] = pd.to_numeric(df['bsns_year'], errors='coerce')
        
        # 4. 종목코드 6자리 패딩
        if 'stock_code' in df.columns:
            df['stock_code'] = df['stock_code'].astype(str).str.zfill(6)
        
        # 5. 기업코드 8자리 패딩
        if 'corp_code' in df.columns:
            df['corp_code'] = df['corp_code'].astype(str).str.zfill(8)
        
        self._log(f"재무제표 정제 완료: {len(df)}건")
        
        return df
    
    def clean_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        주가 데이터 정제
        
        Args:
            df: 주가 DataFrame
        
        Returns:
            정제된 DataFrame
        """
        if df.empty:
            return df
        
        df = df.copy()
        
        # 1. 숫자 컬럼 변환
        for col in self.PRICE_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 2. 날짜 변환
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        elif 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        # 3. 종목코드 패딩
        if 'stock_code' in df.columns:
            df['stock_code'] = df['stock_code'].astype(str).str.zfill(6)
        
        # 4. 음수 거래량 제거
        if 'volume' in df.columns:
            invalid_volume = df['volume'] < 0
            if invalid_volume.any():
                df = df[~invalid_volume]
                self._log(f"음수 거래량 제거: {invalid_volume.sum()}건")
        
        # 5. 가격 이상치 처리 (시가 > 고가 등)
        if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
            invalid_price = (
                (df['high'] < df['low']) |
                (df['open'] < 0) |
                (df['close'] < 0)
            )
            if invalid_price.any():
                df = df[~invalid_price]
                self._log(f"가격 이상치 제거: {invalid_price.sum()}건")
        
        self._log(f"주가 정제 완료: {len(df)}건")
        
        return df
    
    def clean_indicator_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        투자지표 데이터 정제 (PER, PBR 등)
        
        Args:
            df: 투자지표 DataFrame
        
        Returns:
            정제된 DataFrame
        """
        if df.empty:
            return df
        
        df = df.copy()
        
        # 1. 숫자 컬럼 변환
        for col in self.INDICATOR_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 2. 종목코드 패딩
        if 'stock_code' in df.columns:
            df['stock_code'] = df['stock_code'].astype(str).str.zfill(6)
        
        # 3. PER/PBR 이상치 처리 (극단값 제한)
        if 'per' in df.columns:
            # PER이 0 이하거나 1000 초과면 NaN 처리
            df.loc[(df['per'] <= 0) | (df['per'] > 1000), 'per'] = np.nan
        
        if 'pbr' in df.columns:
            # PBR이 0 이하거나 100 초과면 NaN 처리
            df.loc[(df['pbr'] <= 0) | (df['pbr'] > 100), 'pbr'] = np.nan
        
        # 4. 배당수익률 범위 체크 (0~100%)
        if 'div_yield' in df.columns:
            df.loc[(df['div_yield'] < 0) | (df['div_yield'] > 100), 'div_yield'] = np.nan
        
        self._log(f"투자지표 정제 완료: {len(df)}건")
        
        return df
    
    def clean_macro_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        거시경제 지표 정제
        """
        if df.empty:
            return df
        
        df = df.copy()
        
        # 값 숫자 변환
        if 'DATA_VALUE' in df.columns:
            df['DATA_VALUE'] = pd.to_numeric(df['DATA_VALUE'], errors='coerce')
        
        if 'value' in df.columns:
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        # 날짜 변환
        if 'TIME' in df.columns:
            df['TIME'] = df['TIME'].astype(str)
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        return df
    
    def remove_outliers(
        self,
        df: pd.DataFrame,
        columns: List[str],
        method: str = 'iqr',
        threshold: float = 1.5
    ) -> pd.DataFrame:
        """
        이상치 제거
        
        Args:
            df: DataFrame
            columns: 이상치 검사할 컬럼들
            method: 'iqr' (사분위수) 또는 'zscore'
            threshold: IQR 배수 또는 Z-score 임계값
        
        Returns:
            이상치 제거된 DataFrame
        """
        df = df.copy()
        
        for col in columns:
            if col not in df.columns:
                continue
            
            if method == 'iqr':
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                
                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR
                
                outliers = (df[col] < lower_bound) | (df[col] > upper_bound)
                
            elif method == 'zscore':
                mean = df[col].mean()
                std = df[col].std()
                
                if std == 0:
                    continue
                
                z_scores = np.abs((df[col] - mean) / std)
                outliers = z_scores > threshold
            
            else:
                raise ValueError(f"지원하지 않는 방법: {method}")
            
            if outliers.any():
                df = df[~outliers]
                self._log(f"이상치 제거 [{col}]: {outliers.sum()}건")
        
        return df
    
    def fill_missing_values(
        self,
        df: pd.DataFrame,
        columns: List[str],
        method: str = 'ffill'
    ) -> pd.DataFrame:
        """
        결측치 채우기
        
        Args:
            df: DataFrame
            columns: 결측치 처리할 컬럼들
            method: 'ffill', 'bfill', 'mean', 'median', 'zero'
        
        Returns:
            결측치 처리된 DataFrame
        """
        df = df.copy()
        
        for col in columns:
            if col not in df.columns:
                continue
            
            missing_count = df[col].isna().sum()
            if missing_count == 0:
                continue
            
            if method == 'ffill':
                df[col] = df[col].ffill()
            elif method == 'bfill':
                df[col] = df[col].bfill()
            elif method == 'mean':
                df[col] = df[col].fillna(df[col].mean())
            elif method == 'median':
                df[col] = df[col].fillna(df[col].median())
            elif method == 'zero':
                df[col] = df[col].fillna(0)
            
            filled = missing_count - df[col].isna().sum()
            self._log(f"결측치 처리 [{col}]: {filled}건 ({method})")
        
        return df
    
    def get_cleaning_report(self) -> str:
        """정제 작업 리포트 반환"""
        return "\n".join(self.cleaning_log)
