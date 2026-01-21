"""
엑셀 파일 생성 모듈 (깔끔 버전)
- 필터링하기 쉬운 구조
- 심플하고 직관적인 포맷
- 재무제표 정리된 형태
"""

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter
from openpyxl.comments import Comment
import pandas as pd
from typing import Dict, Optional
from datetime import datetime
import logging
import os

logger = logging.getLogger("kr_stock_collector.exporter")


class ExcelExporter:
    """
    엑셀 파일 생성 클래스 (깔끔 버전)
    - 심플한 디자인
    - 필터링 최적화
    """
    
    # 간단한 스타일
    HEADER_FONT = Font(bold=True, color='FFFFFF', size=10)
    HEADER_FILL = PatternFill('solid', fgColor='4472C4')
    ALT_FILL = PatternFill('solid', fgColor='F2F2F2')
    BORDER = Border(
        left=Side(style='thin', color='D9D9D9'),
        right=Side(style='thin', color='D9D9D9'),
        top=Side(style='thin', color='D9D9D9'),
        bottom=Side(style='thin', color='D9D9D9')
    )
    
    def __init__(self, output_dir: str = "outputs"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.wb = Workbook()
        self.wb.remove(self.wb.active)
    
    def _auto_width(self, ws, min_w: int = 8, max_w: int = 35) -> None:
        """컬럼 너비 자동"""
        for col in ws.columns:
            max_len = 0
            col_letter = get_column_letter(col[0].column)
            for cell in col:
                try:
                    if cell.value:
                        val = str(cell.value)
                        # 한글은 1.5배
                        length = sum(1.5 if '\uac00' <= c <= '\ud7a3' else 1 for c in val)
                        max_len = max(max_len, length)
                except:
                    pass
            ws.column_dimensions[col_letter].width = min(max(max_len + 2, min_w), max_w)
    
    def _apply_table_style(self, ws, header_row: int = 1) -> None:
        """테이블 스타일 적용"""
        # 헤더
        for cell in ws[header_row]:
            cell.font = self.HEADER_FONT
            cell.fill = self.HEADER_FILL
            cell.alignment = Alignment(horizontal='center', vertical='center')
        
        # 데이터 행 교차 색상
        for row_idx, row in enumerate(ws.iter_rows(min_row=header_row + 1), start=1):
            for cell in row:
                cell.border = self.BORDER
                if row_idx % 2 == 0:
                    cell.fill = self.ALT_FILL
                # 숫자 포맷
                if isinstance(cell.value, (int, float)):
                    if abs(cell.value) >= 1000:
                        cell.number_format = '#,##0'
                    elif abs(cell.value) < 100 and cell.value != int(cell.value):
                        cell.number_format = '0.00'
    
    def add_summary_sheet(self, summary: Dict) -> None:
        """요약 시트"""
        ws = self.wb.create_sheet("요약", 0)
        
        ws['A1'] = "📊 수집 결과 요약"
        ws['A1'].font = Font(bold=True, size=14)
        ws.merge_cells('A1:B1')
        
        data = [
            ('수집 일시', summary.get('timestamp', '')),
            ('총 종목 수', f"{summary.get('total_stocks', 0):,}"),
            ('재무제표', f"{summary.get('financial_count', 0):,}건"),
            ('투자지표', f"{summary.get('indicator_count', 0):,}건"),
            ('주가', f"{summary.get('price_count', 0):,}건"),
            ('거시경제', f"{summary.get('macro_count', 0):,}건"),
        ]
        
        for idx, (label, value) in enumerate(data, start=3):
            ws.cell(row=idx, column=1, value=label).font = Font(bold=True)
            ws.cell(row=idx, column=2, value=value)
        
        ws.column_dimensions['A'].width = 15
        ws.column_dimensions['B'].width = 20
    
    def add_stock_list_sheet(self, df: pd.DataFrame, cap_df: pd.DataFrame = None) -> None:
        """종목 리스트 시트"""
        if df.empty:
            return
        
        ws = self.wb.create_sheet("종목리스트")
        
        # 시총 병합
        if cap_df is not None and not cap_df.empty:
            if 'stock_code' in cap_df.columns:
                cap_df = cap_df.rename(columns={'stock_code': 'Code'})
            df = df.merge(cap_df, on='Code', how='left')
        
        # 데이터 쓰기
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)
        
        self._apply_table_style(ws)
        ws.auto_filter.ref = ws.dimensions
        self._auto_width(ws)
        ws.freeze_panes = 'C2'
        
        logger.info(f"종목리스트 시트: {len(df)}건")
    
    def add_financial_sheet(self, df: pd.DataFrame) -> None:
        """재무제표 시트 (정리된 형태)"""
        if df.empty:
            return
        
        ws = self.wb.create_sheet("재무제표")
        
        # 컬럼 순서 정리 (종목코드, 종목명 앞으로)
        priority_cols = ['stock_code', 'corp_code', 'corp_name', 'bsns_year', 
                        'reprt_code', 'account_nm', 'thstrm_amount']
        ordered_cols = [c for c in priority_cols if c in df.columns]
        other_cols = [c for c in df.columns if c not in priority_cols]
        df = df[ordered_cols + other_cols]
        
        # 컬럼명 한글화
        col_rename = {
            'stock_code': '종목코드',
            'corp_code': '기업코드',
            'corp_name': '종목명',
            'bsns_year': '사업연도',
            'reprt_code': '보고서코드',
            'account_nm': '계정과목',
            'thstrm_amount': '당기금액',
            'frmtrm_amount': '전기금액',
            'bfefrmtrm_amount': '전전기금액',
        }
        df = df.rename(columns=col_rename)
        
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)
        
        self._apply_table_style(ws)
        ws.auto_filter.ref = ws.dimensions
        self._auto_width(ws)
        ws.freeze_panes = 'D2'
        
        logger.info(f"재무제표 시트: {len(df)}건")
    
    def add_indicator_sheet(self, df: pd.DataFrame) -> None:
        """투자지표 시트"""
        if df.empty:
            return
        
        ws = self.wb.create_sheet("투자지표")
        
        # 컬럼명 한글화
        col_rename = {
            'stock_code': '종목코드',
            'bps': 'BPS',
            'per': 'PER',
            'pbr': 'PBR',
            'eps': 'EPS',
            'div_yield': '배당수익률',
            'dps': 'DPS',
            'date': '기준일',
        }
        df = df.rename(columns=col_rename)
        
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)
        
        self._apply_table_style(ws)
        ws.auto_filter.ref = ws.dimensions
        self._auto_width(ws)
        ws.freeze_panes = 'B2'
        
        logger.info(f"투자지표 시트: {len(df)}건")
    
    def add_price_sheet(self, df: pd.DataFrame) -> None:
        """주가 시트"""
        if df.empty:
            return
        
        ws = self.wb.create_sheet("주가")
        
        col_rename = {
            'stock_code': '종목코드',
            'open': '시가',
            'high': '고가',
            'low': '저가',
            'close': '종가',
            'volume': '거래량',
            'value': '거래대금',
            'change': '등락률',
            'date': '기준일',
        }
        df = df.rename(columns=col_rename)
        
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)
        
        self._apply_table_style(ws)
        ws.auto_filter.ref = ws.dimensions
        self._auto_width(ws)
        ws.freeze_panes = 'B2'
        
        logger.info(f"주가 시트: {len(df)}건")
    
    def add_macro_sheet(self, df: pd.DataFrame) -> None:
        """거시경제 시트"""
        if df.empty:
            return
        
        ws = self.wb.create_sheet("거시경제")
        
        # 컬럼 정리
        col_rename = {
            'TIME': '날짜',
            'date': '날짜',
            'DATA_VALUE': '값',
            'value': '값',
            'STAT_NAME': '통계명',
            'indicator': '지표명',
            'category': '카테고리',
            'source': '출처',
        }
        df = df.rename(columns=col_rename)
        
        # 주요 컬럼만/순서 정리
        keep_cols = ['날짜', '카테고리', '지표명', '값', '출처']
        available = [c for c in keep_cols if c in df.columns]
        if available:
            df = df[available]
        
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)
        
        self._apply_table_style(ws)
        ws.auto_filter.ref = ws.dimensions
        self._auto_width(ws)
        ws.freeze_panes = 'B2'
        
        logger.info(f"거시경제 시트: {len(df)}건")
    
    def add_guide_sheet(self) -> None:
        """가이드 시트"""
        ws = self.wb.create_sheet("사용가이드", 0)
        
        guide = [
            ("📊 사용법", ""),
            ("", ""),
            ("1. 필터 사용", "각 시트 헤더의 ▼ 클릭 → 조건 선택"),
            ("2. 정렬", "헤더 클릭 → 오름차순/내림차순"),
            ("3. 조건 검색", "데이터 > 필터 > 조건 입력"),
            ("", ""),
            ("📌 추천 스크리닝", ""),
            ("저평가주", "PER < 10, PBR < 1"),
            ("우량주", "배당수익률 > 3%"),
            ("", ""),
            ("⚠️ 주의", "과거 데이터는 미래를 보장하지 않습니다"),
        ]
        
        for idx, (label, desc) in enumerate(guide, 1):
            ws.cell(row=idx, column=1, value=label)
            ws.cell(row=idx, column=2, value=desc)
            if label.startswith("📊") or label.startswith("📌") or label.startswith("⚠️"):
                ws.cell(row=idx, column=1).font = Font(bold=True, size=12)
        
        ws.column_dimensions['A'].width = 20
        ws.column_dimensions['B'].width = 45
    
    def save(self, filename: str = None) -> str:
        """저장"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"stock_screener_{timestamp}.xlsx"
        
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
        """전체 내보내기"""
        
        summary = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_stocks': len(stock_list) if stock_list is not None else 0,
            'financial_count': len(financial_data) if financial_data is not None else 0,
            'price_count': len(price_data) if price_data is not None else 0,
            'indicator_count': len(indicator_data) if indicator_data is not None else 0,
            'macro_count': len(macro_data) if macro_data is not None else 0,
        }
        
        # 시트 추가
        self.add_guide_sheet()
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
        
        return self.save(filename)
