"""
재무비율 계산 모듈 (Pro-Level)
- 수익성 지표 (15개)
- 안정성 지표 (10개)
- 성장성 지표 (10개)
- 활동성 지표 (8개)
- 밸류에이션 지표 (12개)
- 현금흐름 품질 지표 (8개)
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, List, Any
import logging

logger = logging.getLogger("kr_stock_collector.calculator")


class FinancialCalculator:
    """
    재무비율 계산 클래스 (Pro-Level)
    
    총 60개+ 투자지표 계산
    """
    
    # =====================================
    # 재무제표 계정과목 매핑 (확장판)
    # =====================================
    ACCOUNT_MAPPING = {
        # 재무상태표 - 자산
        '자산총계': ['자산총계', '자산총액', 'Total Assets', '자산 총계'],
        '유동자산': ['유동자산', 'Current Assets'],
        '현금및현금성자산': ['현금및현금성자산', '현금및예금', '현금', 'Cash'],
        '단기금융상품': ['단기금융상품', 'Short-term Financial Instruments'],
        '매출채권': ['매출채권', '매출채권및기타채권', 'Trade Receivables', '매출채권 및 기타수취채권'],
        '재고자산': ['재고자산', 'Inventories', '저장품'],
        '비유동자산': ['비유동자산', 'Non-current Assets'],
        '유형자산': ['유형자산', 'Property, Plant and Equipment', 'PPE'],
        '무형자산': ['무형자산', 'Intangible Assets'],
        '투자자산': ['장기투자자산', '투자자산', '관계기업투자'],
        '영업권': ['영업권', 'Goodwill'],
        
        # 재무상태표 - 부채
        '부채총계': ['부채총계', '부채총액', 'Total Liabilities', '부채 총계'],
        '유동부채': ['유동부채', 'Current Liabilities'],
        '비유동부채': ['비유동부채', 'Non-current Liabilities'],
        '매입채무': ['매입채무', 'Trade Payables', '매입채무 및 기타채무'],
        '단기차입금': ['단기차입금', 'Short-term Borrowings'],
        '장기차입금': ['장기차입금', 'Long-term Borrowings'],
        '사채': ['사채', 'Bonds'],
        '총차입금': ['총차입금', '차입금', 'Total Borrowings'],
        
        # 재무상태표 - 자본
        '자본총계': ['자본총계', '자본총액', 'Total Equity', '자본 총계'],
        '자본금': ['자본금', 'Capital Stock', '납입자본'],
        '이익잉여금': ['이익잉여금', 'Retained Earnings'],
        '기타포괄손익': ['기타포괄손익누계액', 'Other Comprehensive Income'],
        
        # 손익계산서
        '매출액': ['매출액', '수익(매출액)', 'Revenue', '영업수익', '매출'],
        '매출원가': ['매출원가', 'Cost of Sales', '영업비용'],
        '매출총이익': ['매출총이익', 'Gross Profit'],
        '판매비와관리비': ['판매비와관리비', '판관비', 'SG&A', 'Selling and Admin'],
        '영업이익': ['영업이익', 'Operating Income', 'Operating Profit'],
        '금융수익': ['금융수익', '이자수익', 'Finance Income'],
        '금융비용': ['금융비용', 'Finance Costs'],
        '이자비용': ['이자비용', 'Interest Expense'],
        '법인세비용차감전순이익': ['법인세비용차감전순이익', '세전이익', 'EBT', 'Profit Before Tax'],
        '법인세비용': ['법인세비용', 'Income Tax Expense'],
        '당기순이익': ['당기순이익', '분기순이익', 'Net Income', 'Profit'],
        '감가상각비': ['감가상각비', 'Depreciation', '유형자산감가상각비'],
        '무형자산상각비': ['무형자산상각비', 'Amortization'],
        '연구개발비': ['연구개발비', 'R&D', '경상연구개발비'],
        
        # 현금흐름표
        '영업활동현금흐름': ['영업활동으로인한현금흐름', '영업활동현금흐름', 'CFO'],
        '투자활동현금흐름': ['투자활동으로인한현금흐름', '투자활동현금흐름', 'CFI'],
        '재무활동현금흐름': ['재무활동으로인한현금흐름', '재무활동현금흐름', 'CFF'],
        '유형자산취득': ['유형자산취득', '유형자산의 취득', 'CAPEX'],
        '배당금지급': ['배당금지급', '배당금의 지급', 'Dividends Paid'],
    }
    
    def __init__(self):
        self._cache = {}
    
    def _get_value(
        self,
        df: pd.DataFrame,
        account: str,
        amount_col: str = 'thstrm_amount'
    ) -> Optional[float]:
        """계정과목 금액 조회"""
        names = self.ACCOUNT_MAPPING.get(account, [account])
        
        for name in names:
            if 'account_nm' in df.columns:
                mask = df['account_nm'].str.contains(name, case=False, na=False, regex=False)
                if mask.any():
                    val = df.loc[mask, amount_col].iloc[0]
                    if pd.notna(val):
                        try:
                            return float(str(val).replace(',', ''))
                        except:
                            return None
        return None
    
    def _safe_div(self, a: Optional[float], b: Optional[float]) -> Optional[float]:
        """안전한 나눗셈"""
        if a is None or b is None or b == 0:
            return None
        return a / b
    
    def _pct(self, value: Optional[float]) -> Optional[float]:
        """비율을 퍼센트로"""
        if value is None:
            return None
        return value * 100
    
    # =====================================
    # 수익성 지표 (15개)
    # =====================================
    def calc_profitability(self, df: pd.DataFrame) -> Dict[str, Optional[float]]:
        """
        수익성 지표 계산
        
        Returns:
            {
                'roe': 자기자본이익률,
                'roa': 총자산이익률,
                'roic': 투하자본이익률,
                'gross_margin': 매출총이익률,
                'operating_margin': 영업이익률,
                'net_margin': 순이익률,
                'ebitda_margin': EBITDA마진,
                'cogs_ratio': 매출원가율,
                'sga_ratio': 판관비율,
                'rd_ratio': R&D비율,
                'tax_rate': 법인세율,
                'roe_dupont_npm': 듀퐁 순이익률,
                'roe_dupont_ato': 듀퐁 자산회전율,
                'roe_dupont_lev': 듀퐁 레버리지,
            }
        """
        # 필요 계정
        net_income = self._get_value(df, '당기순이익')
        equity = self._get_value(df, '자본총계')
        total_assets = self._get_value(df, '자산총계')
        revenue = self._get_value(df, '매출액')
        gross_profit = self._get_value(df, '매출총이익')
        operating_income = self._get_value(df, '영업이익')
        cogs = self._get_value(df, '매출원가')
        sga = self._get_value(df, '판매비와관리비')
        rd = self._get_value(df, '연구개발비')
        depreciation = self._get_value(df, '감가상각비') or 0
        amortization = self._get_value(df, '무형자산상각비') or 0
        ebt = self._get_value(df, '법인세비용차감전순이익')
        tax = self._get_value(df, '법인세비용')
        total_debt = self._get_value(df, '부채총계') or 0
        
        result = {}
        
        # ROE = 당기순이익 / 자본총계
        result['roe'] = self._pct(self._safe_div(net_income, equity))
        
        # ROA = 당기순이익 / 자산총계
        result['roa'] = self._pct(self._safe_div(net_income, total_assets))
        
        # ROIC = NOPAT / 투하자본 (간이: 영업이익*(1-세율) / (자본+차입금))
        if operating_income and tax and ebt and ebt != 0:
            tax_rate = tax / ebt
            nopat = operating_income * (1 - tax_rate)
            invested_capital = (equity or 0) + (self._get_value(df, '총차입금') or 0)
            result['roic'] = self._pct(self._safe_div(nopat, invested_capital))
        else:
            result['roic'] = None
        
        # 매출총이익률
        result['gross_margin'] = self._pct(self._safe_div(gross_profit, revenue))
        
        # 영업이익률
        result['operating_margin'] = self._pct(self._safe_div(operating_income, revenue))
        
        # 순이익률
        result['net_margin'] = self._pct(self._safe_div(net_income, revenue))
        
        # EBITDA마진
        if operating_income is not None:
            ebitda = operating_income + depreciation + amortization
            result['ebitda_margin'] = self._pct(self._safe_div(ebitda, revenue))
        else:
            result['ebitda_margin'] = None
        
        # 매출원가율
        result['cogs_ratio'] = self._pct(self._safe_div(cogs, revenue))
        
        # 판관비율
        result['sga_ratio'] = self._pct(self._safe_div(sga, revenue))
        
        # R&D비율
        result['rd_ratio'] = self._pct(self._safe_div(rd, revenue))
        
        # 법인세율
        result['tax_rate'] = self._pct(self._safe_div(tax, ebt))
        
        # 듀퐁분해 (ROE = 순이익률 x 자산회전율 x 레버리지)
        result['roe_dupont_npm'] = self._safe_div(net_income, revenue)  # 순이익률
        result['roe_dupont_ato'] = self._safe_div(revenue, total_assets)  # 자산회전율
        result['roe_dupont_lev'] = self._safe_div(total_assets, equity)  # 레버리지
        
        return result
    
    # =====================================
    # 안정성 지표 (10개)
    # =====================================
    def calc_stability(self, df: pd.DataFrame) -> Dict[str, Optional[float]]:
        """
        안정성 지표 계산
        
        Returns:
            {
                'debt_ratio': 부채비율,
                'equity_ratio': 자기자본비율,
                'net_debt_ratio': 순부채비율,
                'current_ratio': 유동비율,
                'quick_ratio': 당좌비율,
                'cash_ratio': 현금비율,
                'interest_coverage': 이자보상배율,
                'debt_dependency': 차입금의존도,
                'non_current_ratio': 비유동비율,
                'fixed_ratio': 비유동장기적합률,
            }
        """
        # 필요 계정
        total_debt = self._get_value(df, '부채총계')
        equity = self._get_value(df, '자본총계')
        total_assets = self._get_value(df, '자산총계')
        current_assets = self._get_value(df, '유동자산')
        current_liabilities = self._get_value(df, '유동부채')
        non_current_liabilities = self._get_value(df, '비유동부채') or 0
        non_current_assets = self._get_value(df, '비유동자산')
        cash = self._get_value(df, '현금및현금성자산') or 0
        short_term_fin = self._get_value(df, '단기금융상품') or 0
        inventory = self._get_value(df, '재고자산') or 0
        borrowings = self._get_value(df, '총차입금')
        operating_income = self._get_value(df, '영업이익')
        interest_expense = self._get_value(df, '이자비용')
        
        result = {}
        
        # 부채비율 = 부채/자본
        result['debt_ratio'] = self._pct(self._safe_div(total_debt, equity))
        
        # 자기자본비율 = 자본/자산
        result['equity_ratio'] = self._pct(self._safe_div(equity, total_assets))
        
        # 순부채비율 = (차입금-현금)/자본
        if borrowings is not None and equity and equity != 0:
            net_debt = borrowings - cash - short_term_fin
            result['net_debt_ratio'] = self._pct(net_debt / equity)
        else:
            result['net_debt_ratio'] = None
        
        # 유동비율 = 유동자산/유동부채
        result['current_ratio'] = self._pct(self._safe_div(current_assets, current_liabilities))
        
        # 당좌비율 = (유동자산-재고)/유동부채
        if current_assets is not None and current_liabilities and current_liabilities != 0:
            result['quick_ratio'] = self._pct((current_assets - inventory) / current_liabilities)
        else:
            result['quick_ratio'] = None
        
        # 현금비율 = 현금/유동부채
        if current_liabilities and current_liabilities != 0:
            result['cash_ratio'] = self._pct((cash + short_term_fin) / current_liabilities)
        else:
            result['cash_ratio'] = None
        
        # 이자보상배율 = 영업이익/이자비용
        result['interest_coverage'] = self._safe_div(operating_income, interest_expense)
        
        # 차입금의존도 = 차입금/자산
        result['debt_dependency'] = self._pct(self._safe_div(borrowings, total_assets))
        
        # 비유동비율 = 비유동자산/자본
        result['non_current_ratio'] = self._pct(self._safe_div(non_current_assets, equity))
        
        # 비유동장기적합률 = 비유동자산/(자본+비유동부채)
        if non_current_assets is not None and (equity or 0) + non_current_liabilities != 0:
            result['fixed_ratio'] = self._pct(non_current_assets / ((equity or 0) + non_current_liabilities))
        else:
            result['fixed_ratio'] = None
        
        return result
    
    # =====================================
    # 활동성 지표 (8개)
    # =====================================
    def calc_activity(self, df: pd.DataFrame) -> Dict[str, Optional[float]]:
        """
        활동성 지표 계산
        
        Returns:
            {
                'asset_turnover': 총자산회전율,
                'receivable_turnover': 매출채권회전율,
                'inventory_turnover': 재고자산회전율,
                'payable_turnover': 매입채무회전율,
                'working_capital_turnover': 운전자본회전율,
                'receivable_days': 매출채권회수기간,
                'inventory_days': 재고자산보유기간,
                'ccc': 현금전환주기,
            }
        """
        revenue = self._get_value(df, '매출액')
        cogs = self._get_value(df, '매출원가') or revenue  # COGS없으면 매출로 대체
        total_assets = self._get_value(df, '자산총계')
        receivables = self._get_value(df, '매출채권')
        inventory = self._get_value(df, '재고자산')
        payables = self._get_value(df, '매입채무')
        current_assets = self._get_value(df, '유동자산')
        current_liabilities = self._get_value(df, '유동부채')
        
        result = {}
        
        # 총자산회전율 = 매출/자산
        result['asset_turnover'] = self._safe_div(revenue, total_assets)
        
        # 매출채권회전율 = 매출/매출채권
        result['receivable_turnover'] = self._safe_div(revenue, receivables)
        
        # 재고자산회전율 = 매출원가/재고
        result['inventory_turnover'] = self._safe_div(cogs, inventory)
        
        # 매입채무회전율 = 매출원가/매입채무
        result['payable_turnover'] = self._safe_div(cogs, payables)
        
        # 운전자본회전율 = 매출/(유동자산-유동부채)
        if current_assets and current_liabilities:
            wc = current_assets - current_liabilities
            result['working_capital_turnover'] = self._safe_div(revenue, wc) if wc != 0 else None
        else:
            result['working_capital_turnover'] = None
        
        # 매출채권회수기간 = 365/회전율
        recv_turn = result['receivable_turnover']
        result['receivable_days'] = 365 / recv_turn if recv_turn and recv_turn > 0 else None
        
        # 재고자산보유기간 = 365/회전율
        inv_turn = result['inventory_turnover']
        result['inventory_days'] = 365 / inv_turn if inv_turn and inv_turn > 0 else None
        
        # 현금전환주기 = 매출채권회수기간 + 재고보유기간 - 매입채무지급기간
        pay_turn = result['payable_turnover']
        payable_days = 365 / pay_turn if pay_turn and pay_turn > 0 else 0
        
        if result['receivable_days'] and result['inventory_days']:
            result['ccc'] = result['receivable_days'] + result['inventory_days'] - payable_days
        else:
            result['ccc'] = None
        
        return result
    
    # =====================================
    # 밸류에이션 지표 (12개)
    # =====================================
    def calc_valuation(
        self,
        df: pd.DataFrame,
        market_cap: float = None,
        shares: int = None,
        stock_price: float = None
    ) -> Dict[str, Optional[float]]:
        """
        밸류에이션 지표 계산
        
        Args:
            df: 재무제표
            market_cap: 시가총액
            shares: 발행주식수
            stock_price: 주가
        """
        net_income = self._get_value(df, '당기순이익')
        equity = self._get_value(df, '자본총계')
        revenue = self._get_value(df, '매출액')
        operating_income = self._get_value(df, '영업이익')
        cfo = self._get_value(df, '영업활동현금흐름')
        total_debt = self._get_value(df, '부채총계') or 0
        cash = self._get_value(df, '현금및현금성자산') or 0
        depreciation = self._get_value(df, '감가상각비') or 0
        amortization = self._get_value(df, '무형자산상각비') or 0
        capex = abs(self._get_value(df, '유형자산취득') or 0)
        dividends = abs(self._get_value(df, '배당금지급') or 0)
        
        result = {}
        
        if market_cap and market_cap > 0:
            # PER = 시가총액/순이익
            result['per'] = self._safe_div(market_cap, net_income) if net_income and net_income > 0 else None
            
            # PBR = 시가총액/자본
            result['pbr'] = self._safe_div(market_cap, equity) if equity and equity > 0 else None
            
            # PSR = 시가총액/매출
            result['psr'] = self._safe_div(market_cap, revenue) if revenue and revenue > 0 else None
            
            # PCR = 시가총액/영업현금흐름
            result['pcr'] = self._safe_div(market_cap, cfo) if cfo and cfo > 0 else None
            
            # EV = 시가총액 + 순부채
            ev = market_cap + total_debt - cash
            
            # EBITDA
            if operating_income is not None:
                ebitda = operating_income + depreciation + amortization
                result['ev_ebitda'] = self._safe_div(ev, ebitda) if ebitda > 0 else None
            else:
                result['ev_ebitda'] = None
            
            # EV/Sales
            result['ev_sales'] = self._safe_div(ev, revenue) if revenue and revenue > 0 else None
            
            # EV/EBIT
            result['ev_ebit'] = self._safe_div(ev, operating_income) if operating_income and operating_income > 0 else None
            
            # Earnings Yield = 순이익/시가총액
            result['earnings_yield'] = self._pct(self._safe_div(net_income, market_cap))
            
            # FCF Yield = FCF/시가총액
            if cfo is not None:
                fcf = cfo - capex
                result['fcf_yield'] = self._pct(self._safe_div(fcf, market_cap))
            else:
                result['fcf_yield'] = None
        else:
            result.update({
                'per': None, 'pbr': None, 'psr': None, 'pcr': None,
                'ev_ebitda': None, 'ev_sales': None, 'ev_ebit': None,
                'earnings_yield': None, 'fcf_yield': None
            })
        
        # 배당수익률
        if dividends > 0 and market_cap and market_cap > 0:
            result['dividend_yield'] = self._pct(dividends / market_cap)
        else:
            result['dividend_yield'] = None
        
        # 배당성향
        if dividends > 0 and net_income and net_income > 0:
            result['payout_ratio'] = self._pct(dividends / net_income)
        else:
            result['payout_ratio'] = None
        
        # EPS, BPS
        if shares and shares > 0:
            result['eps'] = self._safe_div(net_income, shares)
            result['bps'] = self._safe_div(equity, shares)
        else:
            result['eps'] = None
            result['bps'] = None
        
        return result
    
    # =====================================
    # 현금흐름 품질 지표 (8개)
    # =====================================
    def calc_cashflow_quality(self, df: pd.DataFrame) -> Dict[str, Optional[float]]:
        """
        현금흐름 품질 지표 계산
        
        Returns:
            {
                'cfo_to_ni': 영업CF/순이익 (Accrual),
                'fcf_to_revenue': FCF/매출,
                'fcf_to_ni': FCF/순이익,
                'capex_to_da': CAPEX/감가상각,
                'capex_to_revenue': CAPEX/매출,
                'cash_generation': 현금창출력,
                'cash_dividend_ability': 현금배당능력,
                'altman_z': Altman Z-Score (제조업),
            }
        """
        net_income = self._get_value(df, '당기순이익')
        revenue = self._get_value(df, '매출액')
        cfo = self._get_value(df, '영업활동현금흐름')
        capex = abs(self._get_value(df, '유형자산취득') or 0)
        depreciation = self._get_value(df, '감가상각비') or 0
        amortization = self._get_value(df, '무형자산상각비') or 0
        da = depreciation + amortization
        
        # Altman Z용
        total_assets = self._get_value(df, '자산총계')
        current_assets = self._get_value(df, '유동자산')
        current_liabilities = self._get_value(df, '유동부채')
        retained_earnings = self._get_value(df, '이익잉여금')
        operating_income = self._get_value(df, '영업이익')
        equity = self._get_value(df, '자본총계')
        total_liabilities = self._get_value(df, '부채총계')
        
        result = {}
        
        # 영업CF/순이익 (1 이상이면 좋음, 발생주의 품질)
        result['cfo_to_ni'] = self._safe_div(cfo, net_income)
        
        # FCF = 영업CF - CAPEX
        fcf = (cfo or 0) - capex
        
        # FCF/매출
        result['fcf_to_revenue'] = self._pct(self._safe_div(fcf, revenue))
        
        # FCF/순이익
        result['fcf_to_ni'] = self._safe_div(fcf, net_income)
        
        # CAPEX/감가상각 (1 이하면 투자 부족)
        result['capex_to_da'] = self._safe_div(capex, da) if da > 0 else None
        
        # CAPEX/매출
        result['capex_to_revenue'] = self._pct(self._safe_div(capex, revenue))
        
        # 현금창출력 = 영업CF/자산
        result['cash_generation'] = self._pct(self._safe_div(cfo, total_assets))
        
        # 현금배당능력 = 영업CF/유동부채
        result['cash_dividend_ability'] = self._pct(self._safe_div(cfo, current_liabilities))
        
        # Altman Z-Score (제조업 기준)
        # Z = 1.2*A + 1.4*B + 3.3*C + 0.6*D + 1.0*E
        if all(v is not None and total_assets and total_assets > 0 for v in 
               [current_assets, current_liabilities, retained_earnings, operating_income, equity, total_liabilities, revenue]):
            try:
                wc = current_assets - current_liabilities
                A = wc / total_assets
                B = retained_earnings / total_assets
                C = operating_income / total_assets
                D = equity / total_liabilities if total_liabilities > 0 else 0
                E = revenue / total_assets
                
                z = 1.2*A + 1.4*B + 3.3*C + 0.6*D + 1.0*E
                result['altman_z'] = round(z, 2)
            except:
                result['altman_z'] = None
        else:
            result['altman_z'] = None
        
        return result
    
    # =====================================
    # 성장성 지표 (10개)
    # =====================================
    def calc_growth(
        self,
        current_df: pd.DataFrame,
        previous_df: pd.DataFrame = None
    ) -> Dict[str, Optional[float]]:
        """
        성장성 지표 계산 (전기 대비)
        """
        def growth_rate(curr, prev):
            if curr is None or prev is None or prev == 0:
                return None
            return ((curr - prev) / abs(prev)) * 100
        
        result = {}
        
        if previous_df is None or previous_df.empty:
            return {
                'revenue_growth': None, 'operating_income_growth': None,
                'net_income_growth': None, 'asset_growth': None,
                'equity_growth': None, 'eps_growth': None,
                'sgr': None
            }
        
        # 현재 vs 전기
        curr_revenue = self._get_value(current_df, '매출액')
        prev_revenue = self._get_value(previous_df, '매출액')
        
        curr_oi = self._get_value(current_df, '영업이익')
        prev_oi = self._get_value(previous_df, '영업이익')
        
        curr_ni = self._get_value(current_df, '당기순이익')
        prev_ni = self._get_value(previous_df, '당기순이익')
        
        curr_assets = self._get_value(current_df, '자산총계')
        prev_assets = self._get_value(previous_df, '자산총계')
        
        curr_equity = self._get_value(current_df, '자본총계')
        prev_equity = self._get_value(previous_df, '자본총계')
        
        result['revenue_growth'] = growth_rate(curr_revenue, prev_revenue)
        result['operating_income_growth'] = growth_rate(curr_oi, prev_oi)
        result['net_income_growth'] = growth_rate(curr_ni, prev_ni)
        result['asset_growth'] = growth_rate(curr_assets, prev_assets)
        result['equity_growth'] = growth_rate(curr_equity, prev_equity)
        
        # 지속가능성장률 SGR = ROE * (1 - 배당성향)
        roe = self._safe_div(curr_ni, curr_equity)
        dividends = abs(self._get_value(current_df, '배당금지급') or 0)
        payout = self._safe_div(dividends, curr_ni) if curr_ni and curr_ni > 0 else 0
        
        if roe is not None:
            result['sgr'] = self._pct(roe * (1 - (payout or 0)))
        else:
            result['sgr'] = None
        
        return result
    
    # =====================================
    # 전체 지표 일괄 계산
    # =====================================
    def calculate_all(
        self,
        df: pd.DataFrame,
        previous_df: pd.DataFrame = None,
        market_cap: float = None,
        shares: int = None,
        stock_price: float = None
    ) -> Dict[str, Optional[float]]:
        """
        전체 재무비율 일괄 계산 (60개+)
        """
        result = {}
        
        result.update(self.calc_profitability(df))
        result.update(self.calc_stability(df))
        result.update(self.calc_activity(df))
        result.update(self.calc_valuation(df, market_cap, shares, stock_price))
        result.update(self.calc_cashflow_quality(df))
        result.update(self.calc_growth(df, previous_df))
        
        return result
    
    def get_indicator_categories(self) -> Dict[str, List[str]]:
        """지표 카테고리 목록 반환"""
        return {
            '수익성': ['roe', 'roa', 'roic', 'gross_margin', 'operating_margin', 
                      'net_margin', 'ebitda_margin', 'cogs_ratio', 'sga_ratio', 
                      'rd_ratio', 'tax_rate'],
            '안정성': ['debt_ratio', 'equity_ratio', 'net_debt_ratio', 'current_ratio',
                      'quick_ratio', 'cash_ratio', 'interest_coverage', 'debt_dependency',
                      'non_current_ratio', 'fixed_ratio'],
            '활동성': ['asset_turnover', 'receivable_turnover', 'inventory_turnover',
                      'payable_turnover', 'working_capital_turnover', 'receivable_days',
                      'inventory_days', 'ccc'],
            '밸류에이션': ['per', 'pbr', 'psr', 'pcr', 'ev_ebitda', 'ev_sales', 'ev_ebit',
                        'earnings_yield', 'fcf_yield', 'dividend_yield', 'payout_ratio',
                        'eps', 'bps'],
            '현금흐름품질': ['cfo_to_ni', 'fcf_to_revenue', 'fcf_to_ni', 'capex_to_da',
                         'capex_to_revenue', 'cash_generation', 'cash_dividend_ability',
                         'altman_z'],
            '성장성': ['revenue_growth', 'operating_income_growth', 'net_income_growth',
                     'asset_growth', 'equity_growth', 'sgr']
        }
