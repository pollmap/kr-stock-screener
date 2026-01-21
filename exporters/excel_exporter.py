"""
엑셀 파일 생성 모듈 (Pro-Level)
- 금융 모델링 컬러 코딩
- 초보자용 주석 및 해석 가이드
- 필터링 최적화 구조
- 종목 기본정보 (시총, 주식수) 포함
"""

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, NamedStyle
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter
from openpyxl.comments import Comment
from openpyxl.worksheet.datavalidation import DataValidation
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
import os

# 지표 설명 가져오기
try:
    from config.indicator_descriptions import (
        INVESTMENT_INDICATORS, MACRO_INDICATORS, FINANCIAL_ACCOUNTS,
        get_indicator_description
    )
except ImportError:
    INVESTMENT_INDICATORS = {}
    MACRO_INDICATORS = {}
    FINANCIAL_ACCOUNTS = {}
    def get_indicator_description(code):
        return {'name': code, 'description': '', 'interpretation': ''}

logger = logging.getLogger("kr_stock_collector.exporter")


class ExcelExporter:
    """
    엑셀 파일 생성 클래스 (Pro-Level)
    
    - 금융 모델링 표준 컬러 코딩
    - 초보자용 주석/설명
    - 필터 활성화
    - 시트별 최적화 포맷
    """
    
    # 컬러 팔레트
    COLORS = {
        'header': Font(color='FFFFFF', bold=True, size=11),
        'subheader': Font(color='1F4E79', bold=True),
        'positive': Font(color='006400'),
        'negative': Font(color='DC143C'),
        'link': Font(color='0066CC', underline='single'),
    }
    
    FILLS = {
        'header': PatternFill('solid', fgColor='1F4E79'),      # 진한 파랑
        'header_alt': PatternFill('solid', fgColor='2E75B6'),  # 중간 파랑
        'subheader': PatternFill('solid', fgColor='D6DCE5'),   # 연한 회색
        'positive': PatternFill('solid', fgColor='C6EFCE'),    # 연한 초록
        'negative': PatternFill('solid', fgColor='FFC7CE'),    # 연한 빨강
        'neutral': PatternFill('solid', fgColor='FFEB9C'),     # 연한 노랑
        'alternate': PatternFill('solid', fgColor='F2F2F2'),   # 줄무늬
        'guide': PatternFill('solid', fgColor='FFF2CC'),       # 가이드 배경
    }
    
    BORDERS = {
        'thin': Border(
            left=Side(style='thin', color='B4B4B4'),
            right=Side(style='thin', color='B4B4B4'),
            top=Side(style='thin', color='B4B4B4'),
            bottom=Side(style='thin', color='B4B4B4')
        ),
        'header': Border(
            bottom=Side(style='medium', color='1F4E79')
        )
    }
    
    def __init__(self, output_dir: str = "outputs"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        self.wb = Workbook()
        self.wb.remove(self.wb.active)
    
    def _auto_column_width(self, ws, min_width: int = 10, max_width: int = 40) -> None:
        """컬럼 너비 자동 조정"""
        for column in ws.columns:
            max_length = 0
            column_letter = get_column_letter(column[0].column)
            
            for cell in column:
                try:
                    if cell.value:
                        # 한글은 2배 너비
                        cell_length = len(str(cell.value))
                        if any('\uac00' <= c <= '\ud7a3' for c in str(cell.value)):
                            cell_length = int(cell_length * 1.5)
                        max_length = max(max_length, cell_length)
                except:
                    pass
            
            adjusted_width = min(max(max_length + 3, min_width), max_width)
            ws.column_dimensions[column_letter].width = adjusted_width
    
    def _add_header_comment(self, cell, indicator_code: str) -> None:
        """헤더에 설명 주석 추가"""
        desc = get_indicator_description(indicator_code.lower().replace(' ', '_'))
        
        if desc.get('description'):
            comment_text = f"📊 {desc.get('name', indicator_code)}\n\n"
            comment_text += f"📖 설명:\n{desc.get('description', '')}\n\n"
            
            if desc.get('interpretation'):
                comment_text += f"💡 해석:\n{desc.get('interpretation', '')}\n\n"
            
            if desc.get('caution'):
                comment_text += f"⚠️ 주의:\n{desc.get('caution', '')}"
            
            comment = Comment(comment_text, "Stock Screener")
            comment.width = 350
            comment.height = 200
            cell.comment = comment
    
    def _apply_conditional_color(self, cell, value: Any, indicator_code: str) -> None:
        """값에 따른 조건부 색상"""
        if not isinstance(value, (int, float)):
            return
        
        code = indicator_code.lower()
        
        # ROE, ROA 등 수익성: 높을수록 좋음
        if code in ['roe', 'roa', 'roic', 'operating_margin', 'net_margin']:
            if value >= 15:
                cell.fill = self.FILLS['positive']
            elif value < 0:
                cell.fill = self.FILLS['negative']
        
        # 부채비율: 낮을수록 좋음
        elif code == 'debt_ratio':
            if value <= 100:
                cell.fill = self.FILLS['positive']
            elif value >= 200:
                cell.fill = self.FILLS['negative']
        
        # PER: 적정 범위
        elif code == 'per':
            if 5 <= value <= 15:
                cell.fill = self.FILLS['positive']
            elif value > 50 or value < 0:
                cell.fill = self.FILLS['negative']
        
        # PBR: 1 이하면 저평가 가능성
        elif code == 'pbr':
            if value <= 1:
                cell.fill = self.FILLS['positive']
            elif value > 5:
                cell.fill = self.FILLS['neutral']
        
        # Altman Z: 부도 위험
        elif code == 'altman_z':
            if value >= 3:
                cell.fill = self.FILLS['positive']
            elif value < 1.8:
                cell.fill = self.FILLS['negative']
            else:
                cell.fill = self.FILLS['neutral']
        
        # 성장률: 양수면 좋음
        elif 'growth' in code:
            if value > 0:
                cell.font = self.COLORS['positive']
            else:
                cell.font = self.COLORS['negative']
    
    def add_guide_sheet(self) -> None:
        """
        📚 사용 가이드 시트 추가 (첫 번째 위치)
        초보자를 위한 완전한 가이드
        """
        ws = self.wb.create_sheet("📚 사용가이드", 0)
        
        # 제목
        ws['A1'] = "📊 국내 주식 재무데이터 분석 가이드"
        ws['A1'].font = Font(bold=True, size=18, color='1F4E79')
        ws.merge_cells('A1:F1')
        
        ws['A3'] = "이 파일은 CUFA 가치투자 동아리를 위한 종목 스크리닝 도구입니다."
        ws['A3'].font = Font(size=11)
        
        # 사용 방법
        row = 5
        ws.cell(row=row, column=1, value="🎯 사용 방법").font = Font(bold=True, size=14, color='1F4E79')
        row += 2
        
        guide_items = [
            "1️⃣  각 시트의 헤더(첫 행)에 마우스를 올리면 지표 설명이 나타납니다.",
            "2️⃣  데이터 > 필터 기능으로 원하는 조건의 종목을 찾으세요.",
            "3️⃣  색상으로 빠르게 판단: 🟢 초록=양호, 🔴 빨강=주의, 🟡 노랑=회색지대",
            "4️⃣  '지표설명' 시트에서 각 지표의 의미를 확인하세요.",
            "5️⃣  여러 지표를 종합적으로 판단하세요. 한 지표만으로 결론내지 마세요!",
        ]
        
        for item in guide_items:
            ws.cell(row=row, column=1, value=item).font = Font(size=11)
            row += 1
        
        # 추천 스크리닝 전략
        row += 2
        ws.cell(row=row, column=1, value="💡 추천 스크리닝 전략").font = Font(bold=True, size=14, color='1F4E79')
        row += 2
        
        strategies = [
            ("가치투자 (저평가)", "PER < 10, PBR < 1, ROE > 10%"),
            ("성장투자", "매출성장률 > 20%, 영업이익성장률 > 20%"),
            ("배당투자", "배당수익률 > 3%, 배당성향 < 60%"),
            ("안전투자", "부채비율 < 50%, 이자보상배율 > 5배"),
            ("퀄리티", "ROE > 15%, 영업CF/순이익 > 1"),
        ]
        
        for name, condition in strategies:
            ws.cell(row=row, column=1, value=f"  • {name}").font = Font(bold=True)
            ws.cell(row=row, column=2, value=condition)
            row += 1
        
        # 주의사항
        row += 2
        ws.cell(row=row, column=1, value="⚠️ 주의사항").font = Font(bold=True, size=14, color='DC143C')
        row += 2
        
        warnings = [
            "• 과거 데이터는 미래를 보장하지 않습니다.",
            "• 업종별로 적정 수치가 다릅니다. 동종업계와 비교하세요.",
            "• 일회성 손익이 있을 수 있으니 여러 해 추이를 확인하세요.",
            "• 투자 결정은 본인의 판단과 책임하에 하세요.",
        ]
        
        for warning in warnings:
            ws.cell(row=row, column=1, value=warning).font = Font(size=11, color='DC143C')
            row += 1
        
        # 시트 목록
        row += 2
        ws.cell(row=row, column=1, value="📁 시트 구성").font = Font(bold=True, size=14, color='1F4E79')
        row += 2
        
        sheets_info = [
            ("📊 Summary", "수집 요약 정보"),
            ("📋 종목리스트", "전체 종목 기본정보 (시총, 주식수 포함)"),
            ("📑 재무제표", "재무상태표, 손익계산서 데이터"),
            ("📈 투자지표", "ROE, PER, PBR 등 60개+ 지표"),
            ("💹 주가", "OHLCV 시세 데이터"),
            ("🌍 거시경제", "한국/글로벌 80개+ 경제지표"),
            ("📖 지표설명", "모든 지표의 상세 설명"),
        ]
        
        for sheet_name, desc in sheets_info:
            ws.cell(row=row, column=1, value=sheet_name).font = Font(bold=True)
            ws.cell(row=row, column=2, value=desc)
            row += 1
        
        # 너비 조정
        ws.column_dimensions['A'].width = 40
        ws.column_dimensions['B'].width = 50
    
    def add_indicator_guide_sheet(self) -> None:
        """
        📖 지표 설명 시트 추가
        """
        ws = self.wb.create_sheet("📖 지표설명")
        
        # 헤더
        headers = ['지표코드', '지표명', '카테고리', '계산식', '설명', '해석 방법', '주의사항']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=header)
            cell.font = self.COLORS['header']
            cell.fill = self.FILLS['header']
            cell.alignment = Alignment(horizontal='center')
        
        # 데이터
        row = 2
        for code, info in INVESTMENT_INDICATORS.items():
            ws.cell(row=row, column=1, value=code)
            ws.cell(row=row, column=2, value=info.get('name', ''))
            ws.cell(row=row, column=3, value=info.get('category', ''))
            ws.cell(row=row, column=4, value=info.get('formula', ''))
            ws.cell(row=row, column=5, value=info.get('description', ''))
            ws.cell(row=row, column=6, value=info.get('interpretation', ''))
            ws.cell(row=row, column=7, value=info.get('caution', ''))
            
            if row % 2 == 0:
                for col in range(1, 8):
                    ws.cell(row=row, column=col).fill = self.FILLS['alternate']
            row += 1
        
        # 필터 활성화
        ws.auto_filter.ref = f"A1:G{row-1}"
        
        self._auto_column_width(ws)
        ws.freeze_panes = 'B2'
    
    def add_summary_sheet(self, summary_data: Dict) -> None:
        """📊 요약 시트"""
        ws = self.wb.create_sheet("📊 Summary", 1)
        
        ws['A1'] = "📊 국내 주식 재무데이터 수집 결과"
        ws['A1'].font = Font(bold=True, size=16, color='1F4E79')
        ws.merge_cells('A1:D1')
        
        row = 3
        info_items = [
            ('📅 수집 일시', summary_data.get('timestamp', '')),
            ('📊 총 종목 수', f"{summary_data.get('total_stocks', 0):,}개"),
            ('📆 데이터 기간', summary_data.get('period', '')),
            ('📑 재무제표 건수', f"{summary_data.get('financial_count', 0):,}건"),
            ('📈 투자지표 건수', f"{summary_data.get('indicator_count', 0):,}건"),
            ('💹 주가 데이터', f"{summary_data.get('price_count', 0):,}건"),
            ('🌍 거시경제 건수', f"{summary_data.get('macro_count', 0):,}건"),
        ]
        
        for label, value in info_items:
            ws.cell(row=row, column=1, value=label).font = Font(bold=True)
            ws.cell(row=row, column=2, value=value)
            row += 1
        
        ws.column_dimensions['A'].width = 20
        ws.column_dimensions['B'].width = 25
    
    def add_stock_list_sheet(
        self,
        df: pd.DataFrame,
        market_cap_df: pd.DataFrame = None
    ) -> None:
        """
        📋 종목 리스트 시트 (시총, 주식수 포함)
        """
        if df.empty:
            return
        
        ws = self.wb.create_sheet("📋 종목리스트")
        
        # 시총 정보 병합
        if market_cap_df is not None and not market_cap_df.empty:
            df = df.merge(market_cap_df, on='Code', how='left')
        
        # 헤더
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                cell = ws.cell(row=r_idx, column=c_idx, value=value)
                
                if r_idx == 1:
                    cell.font = self.COLORS['header']
                    cell.fill = self.FILLS['header']
                    cell.alignment = Alignment(horizontal='center')
                else:
                    if c_idx > 2 and isinstance(value, (int, float)):
                        cell.number_format = '#,##0'
                    if r_idx % 2 == 0:
                        cell.fill = self.FILLS['alternate']
        
        # 필터 활성화
        ws.auto_filter.ref = ws.dimensions
        
        self._auto_column_width(ws)
        ws.freeze_panes = 'C2'
        
        logger.info(f"종목리스트 시트: {len(df)}건")
    
    def add_indicator_sheet(
        self,
        df: pd.DataFrame,
        sheet_name: str = "📈 투자지표"
    ) -> None:
        """
        📈 투자지표 시트 (주석 및 조건부 서식 포함)
        """
        if df.empty:
            return
        
        ws = self.wb.create_sheet(sheet_name)
        
        # 헤더 매핑
        header_mapping = {col: col for col in df.columns}
        
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                cell = ws.cell(row=r_idx, column=c_idx, value=value)
                
                if r_idx == 1:
                    cell.font = self.COLORS['header']
                    cell.fill = self.FILLS['header']
                    cell.alignment = Alignment(horizontal='center')
                    
                    # 헤더에 주석 추가
                    col_name = str(value) if value else ''
                    self._add_header_comment(cell, col_name)
                else:
                    # 숫자 포맷
                    if isinstance(value, (int, float)):
                        if abs(value) >= 1000:
                            cell.number_format = '#,##0'
                        elif abs(value) < 100:
                            cell.number_format = '0.00'
                        
                        # 조건부 색상
                        col_name = df.columns[c_idx - 1]
                        self._apply_conditional_color(cell, value, col_name)
                    
                    if r_idx % 2 == 0:
                        cell.fill = self.FILLS['alternate']
        
        ws.auto_filter.ref = ws.dimensions
        self._auto_column_width(ws)
        ws.freeze_panes = 'C2'
        
        logger.info(f"투자지표 시트: {len(df)}건")
    
    def add_financial_sheet(
        self,
        df: pd.DataFrame,
        sheet_name: str = "📑 재무제표"
    ) -> None:
        """📑 재무제표 시트"""
        if df.empty:
            return
        
        ws = self.wb.create_sheet(sheet_name)
        
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                cell = ws.cell(row=r_idx, column=c_idx, value=value)
                
                if r_idx == 1:
                    cell.font = self.COLORS['header']
                    cell.fill = self.FILLS['header']
                else:
                    if isinstance(value, (int, float)) and c_idx > 3:
                        cell.number_format = '#,##0'
                        
                        # 음수는 빨간색
                        if value < 0:
                            cell.font = self.COLORS['negative']
                    
                    if r_idx % 2 == 0:
                        cell.fill = self.FILLS['alternate']
        
        ws.auto_filter.ref = ws.dimensions
        self._auto_column_width(ws)
        ws.freeze_panes = 'D2'
        
        logger.info(f"재무제표 시트: {len(df)}건")
    
    def add_price_sheet(
        self,
        df: pd.DataFrame,
        sheet_name: str = "💹 주가"
    ) -> None:
        """💹 주가 시트"""
        if df.empty:
            return
        
        ws = self.wb.create_sheet(sheet_name)
        
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                cell = ws.cell(row=r_idx, column=c_idx, value=value)
                
                if r_idx == 1:
                    cell.font = self.COLORS['header']
                    cell.fill = self.FILLS['header']
                else:
                    if isinstance(value, (int, float)):
                        cell.number_format = '#,##0'
                        
                        col_name = str(df.columns[c_idx - 1]).lower()
                        if 'change' in col_name or '등락' in col_name:
                            if value > 0:
                                cell.font = Font(color='FF0000')  # 상승 빨강
                            elif value < 0:
                                cell.font = Font(color='0000FF')  # 하락 파랑
        
        ws.auto_filter.ref = ws.dimensions
        self._auto_column_width(ws)
        ws.freeze_panes = 'B2'
        
        logger.info(f"주가 시트: {len(df)}건")
    
    def add_macro_sheet(
        self,
        df: pd.DataFrame,
        sheet_name: str = "🌍 거시경제"
    ) -> None:
        """🌍 거시경제 시트"""
        if df.empty:
            return
        
        ws = self.wb.create_sheet(sheet_name)
        
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                cell = ws.cell(row=r_idx, column=c_idx, value=value)
                
                if r_idx == 1:
                    cell.font = self.COLORS['header']
                    cell.fill = self.FILLS['header']
                else:
                    if isinstance(value, (int, float)):
                        cell.number_format = '#,##0.00'
                    
                    if r_idx % 2 == 0:
                        cell.fill = self.FILLS['alternate']
        
        ws.auto_filter.ref = ws.dimensions
        self._auto_column_width(ws)
        ws.freeze_panes = 'B2'
        
        logger.info(f"거시경제 시트: {len(df)}건")
    
    def save(self, filename: str = None) -> str:
        """파일 저장"""
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
        """전체 데이터 일괄 내보내기"""
        
        # Summary 정보
        summary = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_stocks': len(stock_list) if stock_list is not None else 0,
            'period': '최근 5년',
            'financial_count': len(financial_data) if financial_data is not None else 0,
            'price_count': len(price_data) if price_data is not None else 0,
            'indicator_count': len(indicator_data) if indicator_data is not None else 0,
            'macro_count': len(macro_data) if macro_data is not None else 0,
        }
        
        # 가이드 시트 먼저
        self.add_guide_sheet()
        self.add_summary_sheet(summary)
        
        # 데이터 시트
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
        
        # 지표 설명 시트 (마지막)
        self.add_indicator_guide_sheet()
        
        return self.save(filename)
