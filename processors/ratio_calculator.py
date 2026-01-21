"""
재무비율 계산기 (간소화 버전)
- OpenDART 재무제표에서 주요 계정 추출
- 60개+ 재무비율 계산
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List
import logging

logger = logging.getLogger("kr_stock_collector.ratio")


class RatioCalculator:
    """재무비율 계산기"""
    
    # 계정과목 매핑 (OpenDART → 표준)
    ACCOUNT_MAP = {
        '매출액': ['매출액', '영업수익', '수익(매출액)', '매출'],
        '매출원가': ['매출원가', '영업비용'],
        '매출총이익': ['매출총이익', '매출총손익'],
        '판매비와관리비': ['판매비와관리비', '판관비', '판매관리비'],
        '영업이익': ['영업이익', '영업이익(손실)'],
        '당기순이익': ['당기순이익', '당기순이익(손실)', '분기순이익'],
        '자산총계': ['자산총계', '자산 총계', '자산합계'],
        '부채총계': ['부채총계', '부채 총계', '부채합계'],
        '자본총계': ['자본총계', '자본 총계', '자본합계'],
        '유동자산': ['유동자산'],
        '유동부채': ['유동부채'],
        '비유동자산': ['비유동자산'],
        '비유동부채': ['비유동부채'],
        '현금및현금성자산': ['현금및현금성자산', '현금', '현금 및 현금성자산'],
        '재고자산': ['재고자산'],
        '매출채권': ['매출채권', '매출채권 및 기타유동채권'],
        '매입채무': ['매입채무', '매입채무 및 기타유동채무'],
        '이익잉여금': ['이익잉여금', '이익잉여금(결손금)'],
    }
    
    def __init__(self):
        pass
    
    def _safe_div(self, a, b, default=None):
        """안전한 나눗셈"""
        try:
            if b is None or b == 0 or pd.isna(b) or pd.isna(a):
                return default
            return round(float(a) / float(b), 4)
        except:
            return default
    
    def _pct(self, a, b):
        """퍼센트 계산"""
        result = self._safe_div(a, b)
        return round(result * 100, 2) if result is not None else None
    
    def _extract_account(self, stock_data: pd.DataFrame, account_name: str) -> Optional[float]:
        """계정과목 금액 추출"""
        if stock_data.empty:
            return None
        
        # 계정명 컬럼 찾기
        account_col = None
        for col in ['account_nm', '계정과목']:
            if col in stock_data.columns:
                account_col = col
                break
        
        if account_col is None:
            return None
        
        # 금액 컬럼 찾기
        amount_col = None
        for col in ['thstrm_amount', '당기금액']:
            if col in stock_data.columns:
                amount_col = col
                break
        
        if amount_col is None:
            return None
        
        # 계정 찾기
        candidates = self.ACCOUNT_MAP.get(account_name, [account_name])
        for cand in candidates:
            mask = stock_data[account_col].str.contains(cand, na=False, regex=False)
            matches = stock_data[mask]
            if not matches.empty:
                val = matches.iloc[0][amount_col]
                if pd.notna(val):
                    return float(val)
        
        return None
    
    def calculate_from_stock(self, stock_data: pd.DataFrame, market_info: Dict = None) -> Dict:
        """특정 종목의 재무비율 계산"""
        ratios = {}
        
        # 계정 추출
        revenue = self._extract_account(stock_data, '매출액')
        cogs = self._extract_account(stock_data, '매출원가')
        gross_profit = self._extract_account(stock_data, '매출총이익')
        sga = self._extract_account(stock_data, '판매비와관리비')
        op_income = self._extract_account(stock_data, '영업이익')
        net_income = self._extract_account(stock_data, '당기순이익')
        
        total_assets = self._extract_account(stock_data, '자산총계')
        total_liab = self._extract_account(stock_data, '부채총계')
        total_equity = self._extract_account(stock_data, '자본총계')
        
        current_assets = self._extract_account(stock_data, '유동자산')
        current_liab = self._extract_account(stock_data, '유동부채')
        inventory = self._extract_account(stock_data, '재고자산')
        cash = self._extract_account(stock_data, '현금및현금성자산')
        
        # === 수익성 지표 ===
        ratios['ROE(%)'] = self._pct(net_income, total_equity)
        ratios['ROA(%)'] = self._pct(net_income, total_assets)
        ratios['매출총이익률(%)'] = self._pct(gross_profit, revenue)
        ratios['영업이익률(%)'] = self._pct(op_income, revenue)
        ratios['순이익률(%)'] = self._pct(net_income, revenue)
        ratios['매출원가율(%)'] = self._pct(cogs, revenue)
        ratios['판관비율(%)'] = self._pct(sga, revenue)
        
        # === 안정성 지표 ===
        ratios['부채비율(%)'] = self._pct(total_liab, total_equity)
        ratios['자기자본비율(%)'] = self._pct(total_equity, total_assets)
        ratios['유동비율(%)'] = self._pct(current_assets, current_liab)
        
        if current_assets and inventory:
            ratios['당좌비율(%)'] = self._pct(current_assets - inventory, current_liab)
        
        ratios['현금비율(%)'] = self._pct(cash, current_liab)
        
        # === 활동성 지표 ===
        ratios['총자산회전율'] = self._safe_div(revenue, total_assets)
        
        # === 밸류에이션 (시장정보 사용) ===
        if market_info:
            market_cap = market_info.get('market_cap', 0)
            if market_cap and market_cap > 0:
                ratios['PER'] = self._safe_div(market_cap, net_income) if net_income and net_income > 0 else None
                ratios['PBR'] = self._safe_div(market_cap, total_equity) if total_equity and total_equity > 0 else None
                ratios['PSR'] = self._safe_div(market_cap, revenue) if revenue and revenue > 0 else None
        
        return ratios


def calculate_ratios_for_all(financial_df: pd.DataFrame, market_df: pd.DataFrame = None) -> pd.DataFrame:
    """전체 종목 재무비율 계산"""
    if financial_df is None or financial_df.empty:
        logger.warning("재무제표 데이터 없음")
        return pd.DataFrame()
    
    calculator = RatioCalculator()
    results = []
    
    # 종목 코드 컬럼 찾기
    code_col = None
    for col in ['stock_code', '종목코드', 'corp_code']:
        if col in financial_df.columns:
            code_col = col
            break
    
    if code_col is None:
        logger.warning("종목코드 컬럼 없음")
        return pd.DataFrame()
    
    stock_codes = financial_df[code_col].unique()
    logger.info(f"재무비율 계산: {len(stock_codes)}개 종목")
    
    for code in stock_codes:
        stock_data = financial_df[financial_df[code_col] == code]
        
        # 시장 정보 가져오기
        market_info = {}
        if market_df is not None and not market_df.empty:
            market_code_col = 'stock_code' if 'stock_code' in market_df.columns else 'Code'
            if market_code_col in market_df.columns:
                stock_market = market_df[market_df[market_code_col] == code]
                if not stock_market.empty:
                    row = stock_market.iloc[0]
                    market_info['market_cap'] = row.get('market_cap', 0)
                    market_info['close'] = row.get('close', 0)
        
        ratios = calculator.calculate_from_stock(stock_data, market_info)
        
        if ratios:
            ratios['종목코드'] = code
            results.append(ratios)
    
    if results:
        df = pd.DataFrame(results)
        # 종목코드를 맨 앞으로
        cols = ['종목코드'] + [c for c in df.columns if c != '종목코드']
        df = df[cols]
        logger.info(f"재무비율 계산 완료: {len(df)}개 종목")
        return df
    
    return pd.DataFrame()
