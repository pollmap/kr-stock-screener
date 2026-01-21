"""
엑셀 파일 생성 모듈 (v7 - 모든 데이터 출력 보장)
- 수집된 모든 데이터가 엑셀에 나오도록 보장
- 한글화 완료
- 제작자: 이찬희(금은동 8기)
"""

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter
import pandas as pd
from typing import Dict
from datetime import datetime
import logging
import os

try:
    from config.account_explanations import ACCOUNT_EXPLANATIONS
except ImportError:
    ACCOUNT_EXPLANATIONS = {}

logger = logging.getLogger("kr_stock_collector.exporter")


class ExcelExporter:
    """엑셀 파일 생성"""
    
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
        self.stock_names = {}
        self.created_time = datetime.now()
    
    def set_stock_names(self, stock_list: pd.DataFrame) -> None:
        if stock_list is not None and 'Code' in stock_list.columns and 'Name' in stock_list.columns:
            self.stock_names = dict(zip(stock_list['Code'], stock_list['Name']))
    
    def _add_company_name(self, df: pd.DataFrame, code_col: str = 'stock_code') -> pd.DataFrame:
        """기업명 추가"""
        df = df.copy()
        if code_col in df.columns and self.stock_names:
            if '기업명' not in df.columns and 'corp_name' not in df.columns:
                df['기업명'] = df[code_col].map(self.stock_names)
                # 종목코드 다음에 기업명 배치
                cols = list(df.columns)
                if '기업명' in cols:
                    cols.remove('기업명')
                    idx = cols.index(code_col) + 1 if code_col in cols else 0
                    cols.insert(idx, '기업명')
                    df = df[cols]
        return df
    
    def _auto_width(self, ws, min_w: int = 8, max_w: int = 30) -> None:
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
                    if abs(cell.value) >= 1000000:
                        cell.number_format = '#,##0'
                    elif abs(cell.value) >= 1:
                        cell.number_format = '#,##0.00'
    
    def _write_df_to_sheet(self, ws, df: pd.DataFrame) -> None:
        """DataFrame을 시트에 쓰기"""
        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)
        
        self._apply_table_style(ws)
        if len(df) > 0:
            ws.auto_filter.ref = ws.dimensions
        self._auto_width(ws)
        ws.freeze_panes = 'B2'
    
    def add_guide_sheet(self) -> None:
        """📚 활용가이드"""
        ws = self.wb.create_sheet("📚 활용가이드", 0)
        
        guide = [
            "═══════════════════════════════════════════════════════════════",
            "📊 충북대학교 가치투자학회 종목 스크리닝 시스템",
            f"   제작자: 이찬희(금은동 8기)  |  생성: {self.created_time.strftime('%Y-%m-%d %H:%M')}",
            "═══════════════════════════════════════════════════════════════",
            "",
            "━━━ 📑 시트 안내 ━━━",
            "📋 종목리스트 → 전체 종목/시장/시총",
            "📑 재무제표 → 3년치 재무데이터",
            "📊 시장데이터 → 주가/시총/거래량",
            "📈 재무비율 → ROE/ROA/부채비율 등 20개+",
            "🌍 거시경제 → 한국/글로벌 80개+ 지표",
            "",
            "━━━ 💡 스크리닝 팁 ━━━",
            "저평가 → PER<10, PBR<1",
            "우량주 → ROE>15%, 부채비율<100%",
            "배당주 → 배당수익률>3%",
            "",
            "⚠️ 과거 실적은 미래를 보장하지 않습니다",
        ]
        
        for idx, text in enumerate(guide, 1):
            ws.cell(row=idx, column=1, value=text)
            if text.startswith("📊"):
                ws.cell(row=idx, column=1).font = Font(bold=True, size=14)
            elif text.startswith(("━━━", "═══")):
                ws.cell(row=idx, column=1).font = Font(bold=True, color='4472C4')
        
        ws.column_dimensions['A'].width = 60
    
    def add_summary_sheet(self, summary: Dict) -> None:
        """📊 요약"""
        ws = self.wb.create_sheet("📊 요약", 1)
        
        ws['A1'] = "📊 수집 결과 요약"
        ws['A1'].font = Font(bold=True, size=14)
        
        data = [
            ('생성일시', self.created_time.strftime('%Y-%m-%d %H:%M:%S')),
            ('제작자', '이찬희(금은동 8기)'),
            ('', ''),
            ('총 종목 수', f"{summary.get('total_stocks', 0):,}개"),
            ('재무제표', f"{summary.get('financial_count', 0):,}건"),
            ('시장데이터', f"{summary.get('market_count', 0):,}건"),
            ('재무비율', f"{summary.get('ratio_count', 0):,}건"),
            ('거시경제', f"{summary.get('macro_count', 0):,}건"),
        ]
        
        for idx, (label, value) in enumerate(data, start=3):
            ws.cell(row=idx, column=1, value=label).font = Font(bold=True)
            ws.cell(row=idx, column=2, value=value)
        
        ws.column_dimensions['A'].width = 15
        ws.column_dimensions['B'].width = 35
    
    def add_stock_list_sheet(self, df: pd.DataFrame, market_df: pd.DataFrame = None) -> None:
        """📋 종목리스트"""
        if df is None or df.empty:
            logger.warning("종목리스트 데이터 없음")
            return
        
        ws = self.wb.create_sheet("📋 종목리스트")
        
        result = df.copy()
        
        # 시장 데이터 병합
        if market_df is not None and not market_df.empty:
            market_copy = market_df.copy()
            if 'stock_code' in market_copy.columns:
                market_copy = market_copy.rename(columns={'stock_code': 'Code'})
            
            merge_cols = ['Code']
            for col in ['market_cap', 'close', 'volume', 'corp_name']:
                if col in market_copy.columns and col not in result.columns:
                    merge_cols.append(col)
            
            if len(merge_cols) > 1:
                result = result.merge(market_copy[merge_cols].drop_duplicates(), on='Code', how='left')
        
        # 한글 컬럼명
        col_map = {
            'Code': '종목코드', 'Name': '기업명', 'Market': '시장',
            'market_cap': '시가총액', 'close': '종가', 'volume': '거래량',
            'Sector': '업종', 'Industry': '산업'
        }
        result = result.rename(columns={k: v for k, v in col_map.items() if k in result.columns})
        
        self._write_df_to_sheet(ws, result)
        logger.info(f"📋 종목리스트: {len(result)}건")
    
    def add_financial_sheet(self, df: pd.DataFrame) -> None:
        """📑 재무제표"""
        if df is None or df.empty:
            logger.warning("재무제표 데이터 없음")
            return
        
        ws = self.wb.create_sheet("📑 재무제표")
        
        result = df.copy()
        result = self._add_company_name(result, 'stock_code')
        
        # 한글 컬럼명
        col_map = {
            'stock_code': '종목코드', 'corp_name': '기업명', 'bsns_year': '사업연도',
            'account_nm': '계정과목', 'thstrm_amount': '당기금액',
            'frmtrm_amount': '전기금액', 'bfefrmtrm_amount': '전전기금액',
        }
        result = result.rename(columns={k: v for k, v in col_map.items() if k in result.columns})
        
        self._write_df_to_sheet(ws, result)
        logger.info(f"📑 재무제표: {len(result)}건")
    
    def add_market_sheet(self, df: pd.DataFrame) -> None:
        """📊 시장데이터"""
        if df is None or df.empty:
            logger.warning("시장데이터 없음")
            return
        
        ws = self.wb.create_sheet("📊 시장데이터")
        
        result = df.copy()
        result = self._add_company_name(result, 'stock_code')
        
        # 중복 제거
        result = result.loc[:, ~result.columns.duplicated()]
        
        # 한글 컬럼명
        col_map = {
            'stock_code': '종목코드', 'corp_name': '기업명',
            'close': '종가', 'volume': '거래량', 'change': '등락률',
            'market_cap': '시가총액', 'shares': '상장주식수', 'market': '시장',
            'date': '기준일'
        }
        result = result.rename(columns={k: v for k, v in col_map.items() if k in result.columns})
        
        self._write_df_to_sheet(ws, result)
        logger.info(f"📊 시장데이터: {len(result)}건")
    
    def add_ratio_sheet(self, df: pd.DataFrame) -> None:
        """📈 재무비율"""
        if df is None or df.empty:
            logger.warning("재무비율 데이터 없음 - 시트 생성 건너뜀")
            return
        
        ws = self.wb.create_sheet("📈 재무비율")
        
        result = df.copy()
        result = self._add_company_name(result, '종목코드')
        
        self._write_df_to_sheet(ws, result)
        logger.info(f"📈 재무비율: {len(result)}건")
    
    def add_macro_sheet(self, df: pd.DataFrame) -> None:
        """🌍 거시경제"""
        if df is None or df.empty:
            logger.warning("거시경제 데이터 없음")
            return
        
        ws = self.wb.create_sheet("🌍 거시경제")
        
        result = df.copy()
        
        # 한글 컬럼명
        col_map = {
            'indicator': '지표', 'category': '카테고리', 'date': '기준일',
            'value': '값', 'yoy_pct': 'YoY(%)', 'source': '출처'
        }
        result = result.rename(columns={k: v for k, v in col_map.items() if k in result.columns})
        
        # 컬럼 순서 정리
        priority = ['카테고리', '지표', '기준일', '값', 'YoY(%)', '출처']
        cols = [c for c in priority if c in result.columns]
        cols += [c for c in result.columns if c not in cols]
        result = result[cols]
        
        self._write_df_to_sheet(ws, result)
        logger.info(f"🌍 거시경제: {len(result)}건")
    
    def add_account_sheet(self) -> None:
        """📖 계정설명"""
        if not ACCOUNT_EXPLANATIONS:
            return
        
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
            row += 1
        
        self._auto_width(ws)
    
    def save(self, filename: str = None) -> str:
        if filename is None:
            timestamp = self.created_time.strftime('%Y%m%d_%H%M%S')
            filename = f"종목스크리너_{timestamp}.xlsx"
        
        if not filename.endswith('.xlsx'):
            filename += '.xlsx'
        
        filepath = os.path.join(self.output_dir, filename)
        self.wb.save(filepath)
        logger.info(f"엑셀 저장: {filepath}")
        return filepath
    
    def export_all(
        self,
        financial_data: pd.DataFrame = None,
        market_data: pd.DataFrame = None,
        ratio_data: pd.DataFrame = None,
        macro_data: pd.DataFrame = None,
        stock_list: pd.DataFrame = None,
        filename: str = None
    ) -> str:
        """전체 내보내기"""
        
        logger.info("=== 엑셀 내보내기 시작 ===")
        logger.info(f"재무제표: {len(financial_data) if financial_data is not None else 0}건")
        logger.info(f"시장데이터: {len(market_data) if market_data is not None else 0}건")
        logger.info(f"재무비율: {len(ratio_data) if ratio_data is not None else 0}건")
        logger.info(f"거시경제: {len(macro_data) if macro_data is not None else 0}건")
        
        self.set_stock_names(stock_list)
        
        summary = {
            'total_stocks': len(stock_list) if stock_list is not None else 0,
            'financial_count': len(financial_data) if financial_data is not None else 0,
            'market_count': len(market_data) if market_data is not None else 0,
            'ratio_count': len(ratio_data) if ratio_data is not None else 0,
            'macro_count': len(macro_data) if macro_data is not None else 0,
        }
        
        # 시트 생성 (순서대로)
        self.add_guide_sheet()
        self.add_summary_sheet(summary)
        self.add_stock_list_sheet(stock_list, market_data)
        self.add_financial_sheet(financial_data)
        self.add_market_sheet(market_data)
        self.add_ratio_sheet(ratio_data)
        self.add_macro_sheet(macro_data)
        self.add_account_sheet()
        
        return self.save(filename)
