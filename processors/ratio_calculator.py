"""
재무비율 계산기 Pro v2 (Institutional Grade)
- GPM Fallback: 매출액-매출원가
- ROIC 실효세율 적용
- 필수 계정 누락 로깅
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List
import logging

logger = logging.getLogger("kr_stock_collector.ratio")


class RatioCalculator:
    """재무비율 계산기 Pro (기관투자자 수준)"""
    
    ACCOUNT_MAP = {
        # 손익계산서
        '매출액': ['매출액', '영업수익', '수익(매출액)', '매출', '영업수익(매출액)', '순영업수익'],
        '매출원가': ['매출원가', '영업비용', '영업원가'],
        '매출총이익': ['매출총이익', '매출총손익'],
        '판매비와관리비': ['판매비와관리비', '판관비', '판매관리비'],
        '영업이익': ['영업이익', '영업이익(손실)', '영업손익'],
        '이자비용': ['이자비용', '금융비용', '금융원가', '이자비용(금융비용)'],
        '법인세비용차감전순이익': ['법인세비용차감전순이익', '법인세비용차감전순손익', '세전순이익', '법인세차감전순이익'],
        '법인세비용': ['법인세비용', '법인세'],
        '당기순이익': ['당기순이익', '당기순이익(손실)', '분기순이익', '당기순손익'],
        
        # 재무상태표 - 자산
        '자산총계': ['자산총계', '자산 총계', '자산합계'],
        '유동자산': ['유동자산'],
        '현금및현금성자산': ['현금및현금성자산', '현금', '현금및예치금', '현금 및 현금성자산'],
        '단기금융상품': ['단기금융상품', '단기금융자산'],
        '매출채권': ['매출채권', '매출채권 및 기타유동채권', '매출채권및기타채권', '매출채권 및 기타채권'],
        '재고자산': ['재고자산'],
        '비유동자산': ['비유동자산'],
        '유형자산': ['유형자산'],
        
        # 재무상태표 - 부채
        '부채총계': ['부채총계', '부채 총계', '부채합계'],
        '유동부채': ['유동부채'],
        '단기차입금': ['단기차입금', '단기금융부채'],
        '매입채무': ['매입채무', '매입채무및기타채무', '매입채무 및 기타채무'],
        '비유동부채': ['비유동부채'],
        '장기차입금': ['장기차입금', '장기금융부채', '장기차입금및사채'],
        '사채': ['사채'],
        
        # 재무상태표 - 자본
        '자본총계': ['자본총계', '자본 총계', '자본합계'],
        '이익잉여금': ['이익잉여금', '이익잉여금(결손금)'],
        
        # 현금흐름표
        '영업활동현금흐름': ['영업활동현금흐름', '영업활동으로인한현금흐름', '영업활동 현금흐름', '영업활동으로 인한 현금흐름'],
        '투자활동현금흐름': ['투자활동현금흐름', '투자활동으로인한현금흐름'],
        '유형자산취득': ['유형자산의취득', '유형자산취득', '유형자산의 취득'],
        '감가상각비': ['감가상각비', '유무형자산상각비', '감가상각비및무형자산상각비'],
    }
    
    # 필수 계정 목록
    REQUIRED_ACCOUNTS = ['매출액', '영업이익', '당기순이익', '자산총계', '부채총계', '자본총계']
    
    def __init__(self):
        pass
    
    def _safe_div(self, a, b, default=None):
        try:
            if b is None or b == 0 or pd.isna(b) or pd.isna(a):
                return default
            return round(float(a) / float(b), 4)
        except:
            return default
    
    def _pct(self, a, b):
        result = self._safe_div(a, b)
        return round(result * 100, 2) if result is not None else None
    
    def _extract_account(self, stock_data: pd.DataFrame, account_name: str, period: str = 'thstrm') -> Optional[float]:
        if stock_data.empty:
            return None
        
        account_col = 'account_nm' if 'account_nm' in stock_data.columns else '계정과목'
        if account_col not in stock_data.columns:
            return None
        
        amount_cols = {
            'thstrm': ['thstrm_amount', '당기금액'],
            'frmtrm': ['frmtrm_amount', '전기금액'],
            'bfefrmtrm': ['bfefrmtrm_amount', '전전기금액']
        }
        
        amount_col = None
        for col in amount_cols.get(period, []):
            if col in stock_data.columns:
                amount_col = col
                break
        
        if amount_col is None:
            return None
        
        candidates = self.ACCOUNT_MAP.get(account_name, [account_name])
        for cand in candidates:
            mask = stock_data[account_col].str.strip() == cand
            matches = stock_data[mask]
            
            if matches.empty:
                mask = stock_data[account_col].str.contains(cand, na=False, regex=False)
                matches = stock_data[mask]
            
            if not matches.empty:
                val = matches.iloc[0][amount_col]
                if pd.notna(val):
                    try:
                        if isinstance(val, str):
                            val = val.replace(',', '').replace(' ', '')
                        return float(val)
                    except:
                        pass
        
        return None
    
    def _check_missing_accounts(self, stock_data: pd.DataFrame, code: str) -> List[str]:
        """필수 계정 누락 확인"""
        missing = []
        for acc in self.REQUIRED_ACCOUNTS:
            if self._extract_account(stock_data, acc) is None:
                missing.append(acc)
        
        if missing:
            logger.warning(f"[{code}] 필수 계정 누락: {missing}")
        
        return missing
    
    def calculate_from_stock(self, stock_data: pd.DataFrame, market_info: Dict = None, code: str = '') -> Dict:
        """기관투자자 수준 재무비율 계산"""
        ratios = {}
        
        # 필수 계정 누락 체크
        self._check_missing_accounts(stock_data, code)
        
        # ===== 계정 추출 =====
        revenue = self._extract_account(stock_data, '매출액')
        cogs = self._extract_account(stock_data, '매출원가')
        gross_profit = self._extract_account(stock_data, '매출총이익')
        sga = self._extract_account(stock_data, '판매비와관리비')
        op_income = self._extract_account(stock_data, '영업이익')
        interest_exp = self._extract_account(stock_data, '이자비용')
        ebit = self._extract_account(stock_data, '법인세비용차감전순이익')
        tax = self._extract_account(stock_data, '법인세비용')
        net_income = self._extract_account(stock_data, '당기순이익')
        
        # 전기
        prev_revenue = self._extract_account(stock_data, '매출액', 'frmtrm')
        prev_op_income = self._extract_account(stock_data, '영업이익', 'frmtrm')
        prev_net_income = self._extract_account(stock_data, '당기순이익', 'frmtrm')
        
        # 재무상태표
        total_assets = self._extract_account(stock_data, '자산총계')
        current_assets = self._extract_account(stock_data, '유동자산')
        cash = self._extract_account(stock_data, '현금및현금성자산')
        short_invest = self._extract_account(stock_data, '단기금융상품')
        receivables = self._extract_account(stock_data, '매출채권')
        inventory = self._extract_account(stock_data, '재고자산')
        
        total_liab = self._extract_account(stock_data, '부채총계')
        current_liab = self._extract_account(stock_data, '유동부채')
        payables = self._extract_account(stock_data, '매입채무')
        
        st_debt = self._extract_account(stock_data, '단기차입금') or 0
        lt_debt = self._extract_account(stock_data, '장기차입금') or 0
        bonds = self._extract_account(stock_data, '사채') or 0
        total_debt = st_debt + lt_debt + bonds
        
        total_equity = self._extract_account(stock_data, '자본총계')
        retained = self._extract_account(stock_data, '이익잉여금')
        
        # 현금흐름표
        ocf = self._extract_account(stock_data, '영업활동현금흐름')
        capex = self._extract_account(stock_data, '유형자산취득')
        depreciation = self._extract_account(stock_data, '감가상각비')
        
        if capex and capex < 0:
            capex = abs(capex)
        
        cash_total = (cash or 0) + (short_invest or 0)
        
        # ===== GPM Fallback 로직 (핵심 수정) =====
        if gross_profit is None and revenue and cogs:
            gross_profit = revenue - cogs
            ratios['[계산]매출총이익'] = gross_profit
        
        ratios['매출총이익률(%)'] = self._pct(gross_profit, revenue)
        
        # ===== 수익성 지표 =====
        ratios['ROE(%)'] = self._pct(net_income, total_equity)
        ratios['ROA(%)'] = self._pct(net_income, total_assets)
        ratios['영업이익률(%)'] = self._pct(op_income, revenue)
        ratios['순이익률(%)'] = self._pct(net_income, revenue)
        ratios['매출원가율(%)'] = self._pct(cogs, revenue)
        ratios['판관비율(%)'] = self._pct(sga, revenue)
        
        # ===== ROIC 실효세율 적용 (핵심 수정) =====
        effective_tax_rate = None
        if tax and ebit and ebit > 0:
            effective_tax_rate = tax / ebit
            
            # 비정상 세율 체크 (0~35% 범위)
            if effective_tax_rate < 0 or effective_tax_rate > 0.35:
                effective_tax_rate = 0.22  # 기본값
                ratios['[기본]세율적용'] = True
        else:
            effective_tax_rate = 0.22
        
        ratios['실효세율(%)'] = round(effective_tax_rate * 100, 2)
        
        if op_income and total_equity and total_debt is not None:
            nopat = op_income * (1 - effective_tax_rate)
            invested_cap = total_equity + total_debt - cash_total
            ratios['NOPAT'] = round(nopat, 0)
            ratios['ROIC(%)'] = self._pct(nopat, invested_cap)
        
        # EBITDA
        ebitda = None
        if op_income:
            ebitda = op_income + (depreciation or 0)
            ratios['EBITDA'] = ebitda
            ratios['EBITDA마진(%)'] = self._pct(ebitda, revenue)
        
        # ===== 안정성 지표 =====
        ratios['부채비율(%)'] = self._pct(total_liab, total_equity)
        ratios['자기자본비율(%)'] = self._pct(total_equity, total_assets)
        
        net_debt = total_debt - cash_total
        ratios['순부채비율(%)'] = self._pct(net_debt, total_equity)
        ratios['유동비율(%)'] = self._pct(current_assets, current_liab)
        
        if current_assets and inventory and current_liab:
            ratios['당좌비율(%)'] = self._pct(current_assets - inventory, current_liab)
        
        ratios['현금비율(%)'] = self._pct(cash_total, current_liab)
        
        if interest_exp and interest_exp > 0:
            ratios['이자보상배율'] = self._safe_div(op_income, interest_exp)
        
        ratios['차입금의존도(%)'] = self._pct(total_debt, total_assets)
        
        # ===== 성장성 지표 (YoY) =====
        if prev_revenue and prev_revenue != 0:
            ratios['매출성장률(%)'] = self._pct(revenue - prev_revenue, abs(prev_revenue))
        if prev_op_income and prev_op_income != 0:
            ratios['영업이익성장률(%)'] = self._pct(op_income - prev_op_income, abs(prev_op_income))
        if prev_net_income and prev_net_income != 0:
            ratios['순이익성장률(%)'] = self._pct(net_income - prev_net_income, abs(prev_net_income))
        
        # ===== 활동성 지표 =====
        ratios['총자산회전율'] = self._safe_div(revenue, total_assets)
        if receivables:
            ratios['매출채권회전율'] = self._safe_div(revenue, receivables)
        if inventory and cogs:
            ratios['재고자산회전율'] = self._safe_div(cogs, inventory)
        
        # ===== 현금흐름 지표 =====
        ratios['영업현금흐름'] = ocf
        
        if ocf is not None:
            fcf = ocf - (capex or 0)
            ratios['잉여현금흐름(FCF)'] = fcf
            ratios['FCF/매출(%)'] = self._pct(fcf, revenue)
            ratios['FCF/순이익'] = self._safe_div(fcf, net_income) if net_income else None
        
        if net_income and net_income != 0:
            ratios['OCF/순이익'] = self._safe_div(ocf, net_income)
        
        # ===== 밸류에이션 =====
        if market_info:
            market_cap = market_info.get('market_cap', 0)
            
            if market_cap and market_cap > 0:
                ratios['PER'] = self._safe_div(market_cap, net_income) if net_income and net_income > 0 else None
                ratios['PBR'] = self._safe_div(market_cap, total_equity) if total_equity and total_equity > 0 else None
                ratios['PSR'] = self._safe_div(market_cap, revenue) if revenue and revenue > 0 else None
                ratios['PCR'] = self._safe_div(market_cap, ocf) if ocf and ocf > 0 else None
                
                if ebitda and ebitda > 0:
                    ev = market_cap + total_debt - cash_total
                    ratios['EV'] = ev
                    ratios['EV/EBITDA'] = self._safe_div(ev, ebitda)
                    ratios['EV/Sales'] = self._safe_div(ev, revenue)
        
        # ===== Altman Z-Score =====
        if total_assets and total_assets > 0 and total_liab and total_liab > 0:
            wc = (current_assets or 0) - (current_liab or 0)
            mkt_cap = market_info.get('market_cap', 0) if market_info else 0
            
            z1 = 1.2 * (wc / total_assets)
            z2 = 1.4 * ((retained or 0) / total_assets)
            z3 = 3.3 * ((op_income or 0) / total_assets)
            z4 = 0.6 * (mkt_cap / total_liab) if mkt_cap else 0
            z5 = 1.0 * ((revenue or 0) / total_assets)
            
            z_score = z1 + z2 + z3 + z4 + z5
            ratios['Altman_Z'] = round(z_score, 2)
            
            # Z-Score 해석
            if z_score >= 2.99:
                ratios['Z등급'] = '안전'
            elif z_score >= 1.81:
                ratios['Z등급'] = '주의'
            else:
                ratios['Z등급'] = '위험'
        
        return ratios


def calculate_ratios_for_all(financial_df: pd.DataFrame, market_df: pd.DataFrame = None) -> pd.DataFrame:
    """전체 종목 재무비율 계산"""
    if financial_df is None or financial_df.empty:
        logger.warning("재무제표 데이터 없음")
        return pd.DataFrame()
    
    calculator = RatioCalculator()
    results = []
    
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
        
        market_info = {}
        if market_df is not None and not market_df.empty:
            mkt_code_col = 'stock_code' if 'stock_code' in market_df.columns else 'Code'
            if mkt_code_col in market_df.columns:
                stock_mkt = market_df[market_df[mkt_code_col] == code]
                if not stock_mkt.empty:
                    row = stock_mkt.iloc[0]
                    market_info['market_cap'] = row.get('market_cap', 0)
                    market_info['close'] = row.get('close', 0)
        
        ratios = calculator.calculate_from_stock(stock_data, market_info, code)
        
        if ratios:
            ratios['종목코드'] = code
            results.append(ratios)
    
    if results:
        df = pd.DataFrame(results)
        cols = ['종목코드'] + [c for c in df.columns if c != '종목코드']
        df = df[cols]
        logger.info(f"재무비율 계산 완료: {len(df)}개 종목, {len(df.columns)-1}개 지표")
        return df
    
    return pd.DataFrame()
