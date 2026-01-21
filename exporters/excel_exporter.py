"""
엑셀 파일 생성 모듈 (v5 - 수식 오류 수정)
- 수식에서 = 제거 (텍스트로 표시)
- 제작자: 이찬희(금은동 8기)
- 전체 컬럼 한글화
"""

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter
import pandas as pd
from typing import Dict, Optional
from datetime import datetime
import logging
import os

try:
    from config.account_explanations import ACCOUNT_EXPLANATIONS
except ImportError:
    ACCOUNT_EXPLANATIONS = {}

logger = logging.getLogger("kr_stock_collector.exporter")


class ExcelExporter:
    """엑셀 파일 생성 클래스"""
    
    HEADER_FONT = Font(bold=True, color='FFFFFF', size=10)
    HEADER_FILL = PatternFill('solid', fgColor='4472C4')
    ALT_FILL = PatternFill('solid', fgColor='F2F2F2')
    BORDER = Border(
        left=Side(style='thin', color='D9D9D9'),
        right=Side(style='thin', color='D9D9D9'),
        top=Side(style='thin', color='D9D9D9'),
        bottom=Side(style='thin', color='D9D9D9')
    )
    
    COLUMN_KOREAN = {
        'stock_code': '종목코드', 'Code': '종목코드', 'Name': '기업명',
        'Market': '시장', 'Sector': '업종', 'Industry': '산업',
        'market_cap': '시가총액', 'shares': '상장주식수', 'date': '기준일',
        'open': '시가', 'high': '고가', 'low': '저가', 'close': '종가',
        'volume': '거래량', 'value': '거래대금', 'change': '등락률',
        'bps': 'BPS', 'per': 'PER', 'pbr': 'PBR', 'eps': 'EPS',
        'div_yield': '배당수익률', 'dps': 'DPS',
        'corp_code': '기업코드', 'corp_name': '기업명', 'bsns_year': '사업연도',
        'reprt_code': '보고서', 'account_nm': '계정과목',
        'thstrm_amount': '당기금액', 'frmtrm_amount': '전기금액',
        'bfefrmtrm_amount': '전전기금액', 'fs_div': '재무제표구분',
        'indicator': '지표', 'category': '카테고리', 'source': '출처',
        'yoy_pct': 'YoY(%)',
    }
    
    def __init__(self, output_dir: str = "outputs"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.wb = Workbook()
        self.wb.remove(self.wb.active)
        self.stock_names = {}
        self.created_time = datetime.now()
    
    def set_stock_names(self, stock_list: pd.DataFrame) -> None:
        if stock_list is not None and 'Code' in stock_list.columns and 'Name' in stock_list.columns:
            self.stock_names = dict(zip(stock_list['Code'], stock_list['Name']))
    
    def _add_company_name(self, df: pd.DataFrame, code_col: str = 'stock_code') -> pd.DataFrame:
        if code_col in df.columns and self.stock_names:
            name_col = df[code_col].map(self.stock_names)
            idx = df.columns.get_loc(code_col) + 1
            df.insert(idx, '기업명', name_col)
        return df
    
    def _korean_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        rename_map = {k: v for k, v in self.COLUMN_KOREAN.items() if k in df.columns}
        return df.rename(columns=rename_map)
    
    def _auto_width(self, ws, min_w: int = 8, max_w: int = 35) -> None:
        for col in ws.columns:
            max_len = 0
            col_letter = get_column_letter(col[0].column)
            for cell in col:
                try:
                    if cell.value:
                        val = str(cell.value)
                        length = sum(1.5 if '\uac00' <= c <= '\ud7a3' else 1 for c in val)
                        max_len = max(max_len, length)
                except:
                    pass
            ws.column_dimensions[col_letter].width = min(max(max_len + 2, min_w), max_w)
    
    def _apply_table_style(self, ws, header_row: int = 1) -> None:
        for cell in ws[header_row]:
            cell.font = self.HEADER_FONT
            cell.fill = self.HEADER_FILL
            cell.alignment = Alignment(horizontal='center', vertical='center')
        
        for row_idx, row in enumerate(ws.iter_rows(min_row=header_row + 1), start=1):
            for cell in row:
                cell.border = self.BORDER
                if row_idx % 2 == 0:
                    cell.fill = self.ALT_FILL
                if isinstance(cell.value, (int, float)):
                    if abs(cell.value) >= 1000:
                        cell.number_format = '#,##0'
                    elif cell.value != 0 and abs(cell.value) < 100 and cell.value != int(cell.value):
                        cell.number_format = '0.00'
    
    def add_usage_guide_sheet(self) -> None:
        """활용 가이드 시트 (수식 오류 수정)"""
        ws = self.wb.create_sheet("📚 활용가이드", 0)
        
        # = 기호 제거해서 수식 오류 방지
        content = [
            ("═══════════════════════════════════════════════════════════════", "", ""),
            ("📊 충북대학교 가치투자학회 종목 스크리닝 시스템", "", ""),
            (f"   제작자: 이찬희(금은동 8기)  |  생성: {self.created_time.strftime('%Y-%m-%d %H:%M')}", "", ""),
            ("═══════════════════════════════════════════════════════════════", "", ""),
            ("", "", ""),
            
            ("━━━ 📈 1. 투자 스크리닝 전략 ━━━", "", ""),
            ("", "", ""),
            ("【 그레이엄 스타일 (안전마진) 】", "", ""),
            ("", "투자지표 시트에서 필터:", ""),
            ("", "  - PER 10 미만 (저평가)", ""),
            ("", "  - PBR 1 미만 (청산가치 이하)", ""),
            ("", "  - 배당수익률 3% 초과", ""),
            ("", "", ""),
            ("【 버핏 스타일 (경쟁우위) 】", "", ""),
            ("", "재무제표에서:", ""),
            ("", "  - ROE 15% 초과", ""),
            ("", "  - 영업이익률 10% 초과", ""),
            ("", "  - 부채비율 50% 미만", ""),
            ("", "", ""),
            ("【 피터 린치 스타일 (성장) 】", "", ""),
            ("", "  - 매출성장률 20% 초과", ""),
            ("", "  - PEG 1 미만 (저평가 성장주)", ""),
            ("", "", ""),
            
            ("━━━ 💰 2. 재무분석 가이드 ━━━", "", ""),
            ("", "", ""),
            ("【 수익성 지표 】", "", ""),
            ("지표", "계산", "기준"),
            ("매출총이익률", "매출총이익 / 매출액", "30%+ 양호"),
            ("영업이익률", "영업이익 / 매출액", "10%+ 우량"),
            ("ROE", "당기순이익 / 자본총계", "15%+ 우수"),
            ("ROA", "당기순이익 / 자산총계", "5%+ 양호"),
            ("", "", ""),
            ("【 안정성 지표 】", "", ""),
            ("부채비율", "부채총계 / 자본총계", "100% 이하"),
            ("유동비율", "유동자산 / 유동부채", "100%+ 양호"),
            ("이자보상배율", "영업이익 / 이자비용", "3배+ 안전"),
            ("", "", ""),
            
            ("━━━ 🌍 3. 거시경제 활용 ━━━", "", ""),
            ("", "", ""),
            ("【 금리 해석 】", "", ""),
            ("", "금리 인상기 -> 가치주/금융주 유리", ""),
            ("", "금리 인하기 -> 성장주/기술주 유리", ""),
            ("", "", ""),
            ("【 신호 해석 】", "", ""),
            ("VIX 30 초과", "시장 공포, 매수 기회 검토", ""),
            ("10Y-2Y 마이너스", "경기침체 신호, 방어주 비중확대", ""),
            ("HY스프레드 상승", "신용위험 확대, 우량주 선호", ""),
            ("", "", ""),
            
            ("━━━ 💼 4. 취업 활용 ━━━", "", ""),
            ("", "", ""),
            ("", "- 2,500개 기업 재무데이터 분석 경험", ""),
            ("", "- OpenDART/FRED API 활용 자동화", ""),
            ("", "- Python 데이터 수집 시스템 개발", ""),
            ("", "", ""),
            
            ("━━━ 📑 5. 시트별 안내 ━━━", "", ""),
            ("시트", "내용", "팁"),
            ("📋 종목리스트", "전체 종목/시장/시총", "시장 필터"),
            ("📑 재무제표", "3년치 재무데이터", "계정과목 필터"),
            ("📈 투자지표", "PER/PBR/배당률", "복합조건 필터"),
            ("🌍 거시경제", "금리/물가/환율 최신값", "카테고리 필터"),
            ("📖 계정설명", "계정과목 한글설명", "검색"),
            ("", "", ""),
            
            ("━━━ ⚠️ 주의사항 ━━━", "", ""),
            ("", "- 과거 실적이 미래를 보장하지 않습니다", ""),
            ("", "- 업종별 적정 수치가 다릅니다", ""),
            ("", "- 일회성 손익 확인 필요", ""),
            ("═══════════════════════════════════════════════════════════════", "", ""),
        ]
        
        for idx, (col1, col2, col3) in enumerate(content, 1):
            ws.cell(row=idx, column=1, value=col1)
            ws.cell(row=idx, column=2, value=col2)
            ws.cell(row=idx, column=3, value=col3)
            
            if col1.startswith("📊"):
                ws.cell(row=idx, column=1).font = Font(bold=True, size=14, color='1F4E79')
            elif col1.startswith(("━━━", "═══")):
                ws.cell(row=idx, column=1).font = Font(bold=True, size=11, color='4472C4')
            elif col1.startswith("【"):
                ws.cell(row=idx, column=1).font = Font(bold=True, size=10)
        
        ws.column_dimensions['A'].width = 30
        ws.column_dimensions['B'].width = 40
        ws.column_dimensions['C'].width = 20
    
    def add_account_explanation_sheet(self) -> None:
        """계정과목 설명 시트"""
        ws = self.wb.create_sheet("📖 계정설명")
        
        headers = ['계정명', '영문명', '분류', '설명', '활용법']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=header)
            cell.font = self.HEADER_FONT
            cell.fill = self.HEADER_FILL
        
        row = 2
        for name, info in ACCOUNT_EXPLANATIONS.items():
            ws.cell(row=row, column=1, value=info.get('한글명', name))
            ws.cell(row=row, column=2, value=info.get('영문명', ''))
            ws.cell(row=row, column=3, value=info.get('분류', ''))
            ws.cell(row=row, column=4, value=info.get('설명', ''))
            ws.cell(row=row, column=5, value=info.get('활용', ''))
            
            if row % 2 == 0:
                for col in range(1, 6):
                    ws.cell(row=row, column=col).fill = self.ALT_FILL
            row += 1
        
        ws.auto_filter.ref = f"A1:E{row-1}"
        self._auto_width(ws)
        ws.freeze_panes = 'B2'
    
    def add_summary_sheet(self, summary: Dict) -> None:
        ws = self.wb.create_sheet("📊 요약", 1)
        
        ws['A1'] = "📊 수집 결과 요약"
        ws['A1'].font = Font(bold=True, size=14)
        ws.merge_cells('A1:B1')
        
        data = [
            ('생성일시', self.created_time.strftime('%Y-%m-%d %H:%M:%S')),
            ('제작자', '이찬희(금은동 8기)'),
            ('', ''),
            ('총 종목 수', f"{summary.get('total_stocks', 0):,}개"),
            ('재무제표', f"{summary.get('financial_count', 0):,}건"),
            ('투자지표', f"{summary.get('indicator_count', 0):,}건"),
            ('주가 데이터', f"{summary.get('price_count', 0):,}건"),
            ('거시경제', f"{summary.get('macro_count', 0):,}건"),
        ]
        
        for idx, (label, value) in enumerate(data, start=3):
            ws.cell(row=idx, column=1, value=label).font = Font(bold=True)
            ws.cell(row=idx, column=2, value=value)
        
        ws.column_dimensions['A'].width = 15
        ws.column_dimensions['B'].width = 35
    
    def add_stock_list_sheet(self, df: pd.DataFrame, cap_df: pd.DataFrame = None) -> None:
        if df.empty:
            return
        
        ws = self.wb.create_sheet("📋 종목리스트")
        
        if cap_df is not None and not cap_df.empty:
            if 'stock_code' in cap_df.columns:
                cap_df = cap_df.rename(columns={'stock_code': 'Code'})
            df = df.merge(cap_df, on='Code', how='left')
        
        df = self._korean_columns(df)
        
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)
        
        self._apply_table_style(ws)
        ws.auto_filter.ref = ws.dimensions
        self._auto_width(ws)
        ws.freeze_panes = 'C2'
    
    def add_financial_sheet(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        
        ws = self.wb.create_sheet("📑 재무제표")
        
        if 'corp_name' not in df.columns and 'stock_code' in df.columns:
            df = self._add_company_name(df.copy(), 'stock_code')
        
        df = self._korean_columns(df)
        
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)
        
        self._apply_table_style(ws)
        ws.auto_filter.ref = ws.dimensions
        self._auto_width(ws)
        ws.freeze_panes = 'D2'
    
    def add_indicator_sheet(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        
        ws = self.wb.create_sheet("📈 투자지표")
        
        df = self._add_company_name(df.copy(), 'stock_code')
        df = self._korean_columns(df)
        
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)
        
        self._apply_table_style(ws)
        ws.auto_filter.ref = ws.dimensions
        self._auto_width(ws)
        ws.freeze_panes = 'C2'
    
    def add_price_sheet(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        
        ws = self.wb.create_sheet("💹 주가")
        
        df = self._add_company_name(df.copy(), 'stock_code')
        df = self._korean_columns(df)
        
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)
        
        self._apply_table_style(ws)
        ws.auto_filter.ref = ws.dimensions
        self._auto_width(ws)
        ws.freeze_panes = 'C2'
    
    def add_macro_sheet(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        
        ws = self.wb.create_sheet("🌍 거시경제")
        
        df = self._korean_columns(df)
        
        # 컬럼 순서 정리
        priority = ['카테고리', '지표', '기준일', 'value', 'YoY(%)', '출처']
        available = [c for c in priority if c in df.columns]
        others = [c for c in df.columns if c not in priority]
        if available:
            df = df[available + others]
        
        # value -> 값 변경
        df = df.rename(columns={'value': '값'})
        
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)
        
        self._apply_table_style(ws)
        ws.auto_filter.ref = ws.dimensions
        self._auto_width(ws)
        ws.freeze_panes = 'B2'
    
    def save(self, filename: str = None) -> str:
        if filename is None:
            timestamp = self.created_time.strftime('%Y%m%d_%H%M%S')
            filename = f"종목스크리너_{timestamp}.xlsx"
        
        if not filename.endswith('.xlsx'):
            filename += '.xlsx'
        
        filepath = os.path.join(self.output_dir, filename)
        self.wb.save(filepath)
        logger.info(f"엑셀 파일 저장: {filepath}")
        return filepath
    
    def export_all(
        self,
        financial_data: pd.DataFrame = None,
        price_data: pd.DataFrame = None,
        indicator_data: pd.DataFrame = None,
        macro_data: pd.DataFrame = None,
        stock_list: pd.DataFrame = None,
        market_cap_df: pd.DataFrame = None,
        filename: str = None
    ) -> str:
        
        self.set_stock_names(stock_list)
        
        summary = {
            'timestamp': self.created_time.strftime('%Y-%m-%d %H:%M:%S'),
            'total_stocks': len(stock_list) if stock_list is not None else 0,
            'financial_count': len(financial_data) if financial_data is not None else 0,
            'price_count': len(price_data) if price_data is not None else 0,
            'indicator_count': len(indicator_data) if indicator_data is not None else 0,
            'macro_count': len(macro_data) if macro_data is not None else 0,
        }
        
        self.add_usage_guide_sheet()
        self.add_summary_sheet(summary)
        
        if stock_list is not None and not stock_list.empty:
            self.add_stock_list_sheet(stock_list, market_cap_df)
        
        if financial_data is not None and not financial_data.empty:
            self.add_financial_sheet(financial_data)
        
        if indicator_data is not None and not indicator_data.empty:
            self.add_indicator_sheet(indicator_data)
        
        if price_data is not None and not price_data.empty:
            self.add_price_sheet(price_data)
        
        if macro_data is not None and not macro_data.empty:
            self.add_macro_sheet(macro_data)
        
        self.add_account_explanation_sheet()
        
        return self.save(filename)
