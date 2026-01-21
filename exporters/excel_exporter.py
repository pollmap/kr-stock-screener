"""
엑셀 파일 생성 Pro v3 (Institutional Grade)
- BOK 데이터 출력 수정
- Top Picks 시트 제거
- DCF 기본값 설정
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
    """엑셀 파일 생성 Pro (기관투자자 수준)"""
    
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
        df = df.copy()
        if code_col in df.columns and self.stock_names:
            if '기업명' not in df.columns and 'corp_name' not in df.columns:
                df['기업명'] = df[code_col].map(self.stock_names)
                cols = list(df.columns)
                if '기업명' in cols:
                    cols.remove('기업명')
                    idx = cols.index(code_col) + 1 if code_col in cols else 0
                    cols.insert(idx, '기업명')
                    df = df[cols]
        return df
    
    def _auto_width(self, ws, min_w: int = 8, max_w: int = 25) -> None:
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
            "═══════════════════════════════════════════════════════════",
            "📊 CUFA 충북대학교 가치투자학회 종목 스크리닝 시스템",
            f"   제작자: 이찬희 (금은동 8기 / CUFA 2대 회장)",
            f"   생성: {self.created_time.strftime('%Y-%m-%d %H:%M')}",
            "═══════════════════════════════════════════════════════════",
            "",
            "━━━ 📑 시트 안내 ━━━",
            "📊 요약 → 수집 결과 통계",
            "📋 종목리스트 → 전체 상장사",
            "📑 재무제표 → BS/IS/CF 3년치",
            "📊 시장데이터 → 주가/시총/거래량",
            "📈 재무비율 → 60개+ 지표 (GPM Fallback 적용)",
            "🌍 거시경제 → 한국(BOK) + 글로벌(FRED)",
            "💰 DCF 계산기 → 내재가치 산정 템플릿",
            "",
            "━━━ 🎯 스크리닝 전략 ━━━",
            "【 그레이엄 】 PER<10, PBR<1, 배당>3%",
            "【 버핏 】 ROE>15%, ROIC>12%, OCF/순이익>1",
            "【 린치 】 매출성장>15%, PEG<1",
            "",
            "━━━ 📊 핵심 지표 해석 ━━━",
            "ROE: 15%+ 우수 | ROIC: 12%+ (자본효율)",
            "부채비율: 100% 이하 | 이자보상배율: 3배+",
            "OCF/순이익: 1+ (이익의 질) | Z-Score: 2.99+ 안전",
            "",
            "⚠️ 과거 실적은 미래를 보장하지 않습니다",
        ]
        
        for idx, text in enumerate(guide, 1):
            cell = ws.cell(row=idx, column=1, value=text)
            if text.startswith("📊 CUFA"):
                cell.font = Font(bold=True, size=14, color='1F4E79')
            elif text.startswith(("━━━", "═══")):
                cell.font = Font(bold=True, color='4472C4')
        
        ws.column_dimensions['A'].width = 60
    
    def add_summary_sheet(self, summary: Dict) -> None:
        ws = self.wb.create_sheet("📊 요약", 1)
        
        ws['A1'] = "📊 수집 결과 요약"
        ws['A1'].font = Font(bold=True, size=14)
        
        data = [
            ('생성일시', self.created_time.strftime('%Y-%m-%d %H:%M:%S')),
            ('제작자', '이찬희 (금은동 8기)'),
            ('', ''),
            ('총 종목 수', f"{summary.get('total_stocks', 0):,}개"),
            ('재무제표', f"{summary.get('financial_count', 0):,}건"),
            ('시장데이터', f"{summary.get('market_count', 0):,}건"),
            ('재무비율', f"{summary.get('ratio_count', 0):,}건"),
            ('거시경제(한국)', f"{summary.get('macro_kr_count', 0):,}건"),
            ('거시경제(글로벌)', f"{summary.get('macro_global_count', 0):,}건"),
        ]
        
        for idx, (label, value) in enumerate(data, start=3):
            ws.cell(row=idx, column=1, value=label).font = Font(bold=True)
            ws.cell(row=idx, column=2, value=value)
        
        ws.column_dimensions['A'].width = 18
        ws.column_dimensions['B'].width = 40
    
    def add_stock_list_sheet(self, df: pd.DataFrame, market_df: pd.DataFrame = None) -> None:
        if df is None or df.empty:
            return
        
        ws = self.wb.create_sheet("📋 종목리스트")
        result = df.copy()
        
        if market_df is not None and not market_df.empty:
            market_copy = market_df.copy()
            if 'stock_code' in market_copy.columns:
                market_copy = market_copy.rename(columns={'stock_code': 'Code'})
            
            merge_cols = ['Code', 'market_cap', 'close', 'volume', 'corp_name']
            merge_cols = [c for c in merge_cols if c in market_copy.columns]
            if merge_cols:
                result = result.merge(market_copy[merge_cols].drop_duplicates(), on='Code', how='left')
        
        col_map = {'Code': '종목코드', 'Name': '기업명', 'Market': '시장',
                   'market_cap': '시가총액', 'close': '종가', 'volume': '거래량'}
        result = result.rename(columns={k: v for k, v in col_map.items() if k in result.columns})
        
        self._write_df_to_sheet(ws, result)
        logger.info(f"📋 종목리스트: {len(result)}건")
    
    def add_financial_sheet(self, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            return
        
        ws = self.wb.create_sheet("📑 재무제표")
        result = df.copy()
        result = self._add_company_name(result, 'stock_code')
        
        col_map = {'stock_code': '종목코드', 'corp_name': '기업명', 'bsns_year': '사업연도',
                   'account_nm': '계정과목', 'thstrm_amount': '당기금액',
                   'frmtrm_amount': '전기금액', 'bfefrmtrm_amount': '전전기금액'}
        result = result.rename(columns={k: v for k, v in col_map.items() if k in result.columns})
        
        self._write_df_to_sheet(ws, result)
        logger.info(f"📑 재무제표: {len(result)}건")
    
    def add_market_sheet(self, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            return
        
        ws = self.wb.create_sheet("📊 시장데이터")
        result = df.copy()
        result = self._add_company_name(result, 'stock_code')
        result = result.loc[:, ~result.columns.duplicated()]
        
        col_map = {'stock_code': '종목코드', 'corp_name': '기업명', 'close': '종가',
                   'volume': '거래량', 'change': '등락률', 'market_cap': '시가총액'}
        result = result.rename(columns={k: v for k, v in col_map.items() if k in result.columns})
        
        self._write_df_to_sheet(ws, result)
        logger.info(f"📊 시장데이터: {len(result)}건")
    
    def add_ratio_sheet(self, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            logger.warning("재무비율 데이터 없음")
            return
        
        ws = self.wb.create_sheet("📈 재무비율")
        result = df.copy()
        result = self._add_company_name(result, '종목코드')
        
        self._write_df_to_sheet(ws, result)
        logger.info(f"📈 재무비율: {len(result)}건, {len(result.columns)}개 지표")
    
    def add_macro_sheet(self, kr_df: pd.DataFrame, global_df: pd.DataFrame) -> None:
        """🌍 거시경제 (BOK + FRED) - 수정됨"""
        ws = self.wb.create_sheet("🌍 거시경제")
        
        # 디버깅 로그
        logger.info(f"===== 거시경제 시트 생성 =====")
        logger.info(f"BOK 수신: {len(kr_df) if kr_df is not None and not kr_df.empty else 0}건")
        logger.info(f"FRED 수신: {len(global_df) if global_df is not None and not global_df.empty else 0}건")
        
        all_data = []
        
        # BOK 데이터
        if kr_df is not None and not kr_df.empty:
            kr_data = kr_df.copy()
            if 'source' not in kr_data.columns:
                kr_data['source'] = 'BOK(한국)'
            all_data.append(kr_data)
            logger.info(f"BOK 추가: {len(kr_data)}건")
        else:
            logger.warning("BOK 데이터 없음 또는 None")
        
        # FRED 데이터
        if global_df is not None and not global_df.empty:
            global_data = global_df.copy()
            if 'source' not in global_data.columns:
                global_data['source'] = 'FRED(글로벌)'
            all_data.append(global_data)
            logger.info(f"FRED 추가: {len(global_data)}건")
        else:
            logger.warning("FRED 데이터 없음 또는 None")
        
        if not all_data:
            logger.error("거시경제 데이터 전체 없음!")
            ws['A1'] = "데이터 없음"
            return
        
        result = pd.concat(all_data, ignore_index=True)
        logger.info(f"거시경제 통합: {len(result)}건")
        
        # 컬럼 정리
        col_map = {'indicator': '지표', 'category': '카테고리', 'date': '기준일',
                   'value': '값', 'yoy_pct': 'YoY(%)', 'source': '출처'}
        result = result.rename(columns={k: v for k, v in col_map.items() if k in result.columns})
        
        priority = ['출처', '카테고리', '지표', '기준일', '값', 'YoY(%)']
        cols = [c for c in priority if c in result.columns]
        cols += [c for c in result.columns if c not in cols]
        result = result[cols]
        
        self._write_df_to_sheet(ws, result)
        logger.info(f"🌍 거시경제 시트 완료: {len(result)}건")
    
    def add_dcf_sheet(self) -> None:
        """💰 DCF 계산기 (기본값 설정됨)"""
        ws = self.wb.create_sheet("💰 DCF 계산기")
        
        ws['A1'] = "💰 DCF (할인현금흐름) 내재가치 계산기"
        ws['A1'].font = Font(bold=True, size=14)
        
        inputs = [
            ('', ''),
            ('━━━ 입력 항목 ━━━', ''),
            ('종목코드/기업명', '직접 입력'),
            ('', ''),
            ('현재 FCF (억원)', 1000),      # 기본값 설정
            ('성장률 1~5년차 (%)', 10),     # 보수적 기본값
            ('성장률 6~10년차 (%)', 5),
            ('영구성장률 (%)', 2),           # GDP 수준
            ('할인율 WACC (%)', 10),         # KOSPI 기대수익률
            ('발행주식수 (만주)', 1000),
            ('현재 주가 (원)', 50000),
            ('', ''),
            ('━━━ 계산 방법 ━━━', ''),
            ('1~5년 FCF 현재가치', '=FCF×(1+g)^n / (1+r)^n 합계'),
            ('6~10년 FCF 현재가치', '=위와 동일'),
            ('영구가치', '=FCF_10 × (1+g영구) / (r-g영구)'),
            ('기업가치(EV)', '=1~5년 + 6~10년 + 영구가치'),
            ('순부채', '=차입금 - 현금'),
            ('주주가치', '=EV - 순부채'),
            ('주당 내재가치', '=주주가치 / 주식수'),
            ('', ''),
            ('━━━ 투자 판단 ━━━', ''),
            ('안전마진', '=(내재가치-현재주가) / 내재가치'),
            ('판단 기준', '안전마진 30%+ → 매수 검토'),
        ]
        
        for idx, (label, value) in enumerate(inputs, 1):
            cell_a = ws.cell(row=idx, column=1, value=label)
            ws.cell(row=idx, column=2, value=value)
            
            if label.startswith("━━━"):
                cell_a.font = Font(bold=True, color='4472C4')
        
        ws.column_dimensions['A'].width = 30
        ws.column_dimensions['B'].width = 35
        
        logger.info("💰 DCF 계산기 시트 추가")
    
    def add_account_sheet(self) -> None:
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
        macro_kr_data: pd.DataFrame = None,
        macro_global_data: pd.DataFrame = None,
        stock_list: pd.DataFrame = None,
        filename: str = None
    ) -> str:
        
        logger.info("=== 엑셀 내보내기 시작 ===")
        logger.info(f"macro_kr_data type: {type(macro_kr_data)}")
        logger.info(f"macro_global_data type: {type(macro_global_data)}")
        
        self.set_stock_names(stock_list)
        
        summary = {
            'total_stocks': len(stock_list) if stock_list is not None else 0,
            'financial_count': len(financial_data) if financial_data is not None else 0,
            'market_count': len(market_data) if market_data is not None else 0,
            'ratio_count': len(ratio_data) if ratio_data is not None else 0,
            'macro_kr_count': len(macro_kr_data) if macro_kr_data is not None and not macro_kr_data.empty else 0,
            'macro_global_count': len(macro_global_data) if macro_global_data is not None and not macro_global_data.empty else 0,
        }
        
        # 시트 생성 (Top Picks 제거됨)
        self.add_guide_sheet()
        self.add_summary_sheet(summary)
        self.add_stock_list_sheet(stock_list, market_data)
        self.add_financial_sheet(financial_data)
        self.add_market_sheet(market_data)
        self.add_ratio_sheet(ratio_data)
        self.add_macro_sheet(macro_kr_data, macro_global_data)
        self.add_dcf_sheet()
        self.add_account_sheet()
        
        return self.save(filename)
