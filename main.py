#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
국내 주식 재무데이터 수집 시스템 (Pro-Level v2)
메인 실행 파일

특징:
- 3년치 재무제표 수집
- 진행상황 표시 + 예상 시간
- 140+ 지표 (투자 60+ / 한국 30+ / 글로벌 50+)
- 초보자용 엑셀 주석/가이드

사용법:
    python main.py                              # 전체 수집 (5년)
    python main.py --select financial,indicators # 선택적 수집
    python main.py --interactive                # 대화형 메뉴
    python main.py --quick                      # 빠른 테스트 (100종목)
"""

import os
import sys
import argparse
from datetime import datetime
from typing import List, Optional
import yaml
import logging
import pandas as pd

# 프로젝트 루트
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main():
    """메인 실행"""
    args = parse_args()
    
    # 디렉토리 생성
    os.makedirs('outputs', exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    os.makedirs('cache', exist_ok=True)
    
    # =====================================
    # 1. 의존성 체크
    # =====================================
    from utils.setup_checker import SetupChecker
    
    if args.check_deps:
        config = load_config_safe()
        checker = SetupChecker(auto_install=True)
        checker.run_full_check(config)
        return
    
    if not args.skip_dep_check:
        checker = SetupChecker(auto_install=True)
        pkg_results = checker.check_packages()
        if checker.missing_packages or checker.outdated_packages:
            print("\n📥 패키지 설치 중...")
            checker.install_missing()
    
    # 로깅
    from utils import setup_logger
    logger = setup_logger(log_dir='logs')
    
    # =====================================
    # 2. 설정 로드
    # =====================================
    try:
        config = load_config()
    except FileNotFoundError:
        print("❌ config/api_keys.yaml 파일을 찾을 수 없습니다.")
        return
    
    # =====================================
    # 3. 스크리닝 설정
    # =====================================
    from config.screening_config import (
        ScreeningConfig, show_interactive_menu, get_screening_summary
    )
    
    if args.interactive:
        screening = show_interactive_menu()
    elif args.select:
        selections = [s.strip() for s in args.select.split(',')]
        screening = ScreeningConfig.from_selection(selections)
    else:
        screening = ScreeningConfig.preset_full()
    
    # =====================================
    # 4. 수집기 초기화
    # =====================================
    from collectors import FDRCollector, PyKrxCollector, OpenDartCollector, BOKCollector, FREDCollector
    from processors import DataCleaner, FinancialCalculator
    from exporters import ExcelExporter
    from utils.progress_tracker import ProgressTracker
    
    fdr = FDRCollector(cache_dir='cache')
    krx = PyKrxCollector(cache_dir='cache')
    cleaner = DataCleaner()
    calculator = FinancialCalculator()
    
    # 조건부 초기화
    dart = None
    if screening.balance_sheet or screening.income_statement:
        try:
            dart = OpenDartCollector(api_key=config['opendart']['api_key'], cache_dir='cache')
        except:
            pass
    
    bok = None
    if any([screening.kr_interest_rate, screening.kr_inflation, screening.kr_exchange_rate, screening.kr_trade]):
        try:
            bok = BOKCollector(api_key=config['bok']['api_key'], cache_dir='cache')
        except:
            pass
    
    fred = None
    if any([screening.us_rates, screening.volatility, screening.commodities]):
        try:
            fred = FREDCollector(api_key=config['fred']['api_key'], cache_dir='cache')
        except:
            pass
    
    # =====================================
    # 5. 종목 리스트 조회
    # =====================================
    try:
        stock_list = fdr.get_all_stock_list(args.market)
        stock_codes = stock_list['Code'].tolist()
        
        if args.quick:
            stock_codes = stock_codes[:100]
            stock_list = stock_list.head(100)
    except Exception as e:
        print(f"❌ 종목 리스트 조회 실패: {e}")
        return
    
    # =====================================
    # 6. 진행상황 트래커 초기화
    # =====================================
    total_steps = 7
    tracker = ProgressTracker(total_steps=total_steps, show_eta=True)
    
    # 초기 예상 시간 표시
    tracker.show_initial_estimate(
        stock_count=len(stock_codes),
        years=len(args.years),
        screening_summary=get_screening_summary(screening)
    )
    
    # 결과 저장
    financial_data = None
    price_data = None
    indicator_data = None
    macro_data = None
    market_cap_df = None
    
    # =====================================
    # 7. 종목 기본정보 (시총, 주식수) - FDR 사용
    # =====================================
    tracker.start_step("종목 기본정보 수집", len(stock_codes))
    
    try:
        # FDR로 KRX 시가총액 데이터 수집 (pykrx 대신)
        import FinanceDataReader as fdr_lib
        
        # KRX 전체 종목 시가총액 조회
        krx_cap = fdr_lib.StockListing('KRX')
        
        if krx_cap is not None and not krx_cap.empty:
            market_cap_df = krx_cap.copy()
            
            # 컬럼명 통일
            col_map = {
                'Code': 'stock_code', 'Symbol': 'stock_code',
                'Name': 'corp_name', 'Market': 'market',
                'Marcap': 'market_cap', 'Stocks': 'shares',
                'Close': 'close', 'Volume': 'volume',
                'ChagesRatio': 'change', 'ChangeRatio': 'change'
            }
            market_cap_df = market_cap_df.rename(columns={k: v for k, v in col_map.items() if k in market_cap_df.columns})
            
            tracker.update(len(market_cap_df), "FDR 시총 완료")
            tracker.finish_step(f"{len(market_cap_df):,}개 종목 시총 수집 (FDR)")
        else:
            tracker.finish_step("시총 데이터 없음")
    except Exception as e:
        logger.error(f"시총 수집 실패: {e}")
        tracker.finish_step("시총 수집 실패")
    
    # =====================================
    # 8. 재무제표 수집 (3년)
    # =====================================
    if dart and (screening.balance_sheet or screening.income_statement):
        tracker.start_step("재무제표 수집 (3년)", len(stock_codes) * len(args.years))
        
        try:
            years = [str(y) for y in args.years]
            financial_data = dart.collect_all_financials(
                stock_codes=stock_codes,
                years=years,
                use_multi_api=True
            )
            
            if financial_data is not None and not financial_data.empty:
                financial_data = cleaner.clean_financial_data(financial_data)
                tracker.update(len(financial_data), "정제 완료")
                tracker.finish_step(f"{len(financial_data):,}건 수집")
            else:
                tracker.finish_step("데이터 없음")
        except Exception as e:
            logger.error(f"재무제표 오류: {e}")
            tracker.finish_step("수집 실패")
    else:
        tracker.skip_step("재무제표 수집", "설정에서 제외됨")
    
    # =====================================
    # 9. 투자지표 수집 - FDR 데이터 활용
    # =====================================
    if screening.price or screening.valuation:
        tracker.start_step("투자지표 수집", len(stock_codes))
        
        try:
            # FDR에서 가져온 market_cap_df에 PER, PBR 등이 포함되어 있음
            if market_cap_df is not None and not market_cap_df.empty:
                # 투자지표 관련 컬럼 추출
                indicator_cols = ['stock_code', 'corp_name', 'close', 'market_cap', 'shares']
                # PER, PBR 등이 있으면 추가
                for col in ['PER', 'PBR', 'EPS', 'BPS', 'DIV', 'DPS']:
                    if col in market_cap_df.columns:
                        indicator_cols.append(col)
                
                avail_cols = [c for c in indicator_cols if c in market_cap_df.columns]
                indicator_data = market_cap_df[avail_cols].copy()
                
                # 컬럼명 소문자로 통일
                indicator_data.columns = [c.lower() if c not in ['stock_code', 'corp_name'] else c for c in indicator_data.columns]
                
                tracker.update(len(indicator_data))
                tracker.finish_step(f"{len(indicator_data):,}건 (FDR)")
            else:
                # FDR에 없으면 빈 데이터
                indicator_data = pd.DataFrame()
                tracker.finish_step("데이터 없음 (FDR)")
        except Exception as e:
            logger.error(f"투자지표 오류: {e}")
            indicator_data = pd.DataFrame()
            tracker.finish_step("건너뜀")
    else:
        tracker.skip_step("투자지표 수집", "설정에서 제외됨")
    
    # =====================================
    # 10. 주가 시세 수집 - FDR 데이터 활용
    # =====================================
    if screening.price:
        tracker.start_step("주가 시세 수집", len(stock_codes))
        
        try:
            # FDR market_cap_df에서 주가 정보 추출
            if market_cap_df is not None and not market_cap_df.empty:
                price_cols = ['stock_code', 'corp_name', 'close', 'volume', 'change', 'market_cap']
                avail_cols = [c for c in price_cols if c in market_cap_df.columns]
                price_data = market_cap_df[avail_cols].copy()
                
                # 날짜 추가
                from datetime import datetime as dt
                price_data['date'] = dt.now().strftime('%Y-%m-%d')
                
                tracker.update(len(price_data))
                tracker.finish_step(f"{len(price_data):,}건 (FDR)")
            else:
                price_data = pd.DataFrame()
                tracker.finish_step("데이터 없음")
        except Exception as e:
            logger.error(f"주가 오류: {e}")
            price_data = pd.DataFrame()
            tracker.finish_step("처리 실패")
    else:
        tracker.skip_step("주가 시세 수집", "설정에서 제외됨")

    
    # =====================================
    # 11. 한국 경제지표 수집 (최신값만)
    # =====================================
    macro_kr_data = None
    if bok:
        tracker.start_step("한국 경제지표 수집", 30)
        
        try:
            # 최신값만 수집 (시계열 X)
            macro_kr_data = bok.collect()
            if macro_kr_data is not None and not macro_kr_data.empty:
                tracker.update(len(macro_kr_data))
                tracker.finish_step(f"{len(macro_kr_data):,}개 지표")
            else:
                tracker.finish_step("데이터 없음")
        except Exception as e:
            logger.error(f"한국경제 오류: {e}")
            tracker.finish_step("수집 실패")
    else:
        tracker.skip_step("한국 경제지표", "설정에서 제외됨")
    
    # =====================================
    # 12. 글로벌 경제지표 수집 (최신값만)
    # =====================================
    macro_global_data = None
    if fred:
        tracker.start_step("글로벌 경제지표 수집", 50)
        
        try:
            # 최신값만 수집 (시계열 X)
            macro_global_data = fred.collect()
            if macro_global_data is not None and not macro_global_data.empty:
                tracker.update(len(macro_global_data))
                tracker.finish_step(f"{len(macro_global_data):,}개 지표")
            else:
                tracker.finish_step("데이터 없음")
        except Exception as e:
            logger.error(f"글로벌경제 오류: {e}")
            tracker.finish_step("수집 실패")
    else:
        tracker.skip_step("글로벌 경제지표", "설정에서 제외됨")
    
    # 거시경제 병합
    macro_parts = []
    if macro_kr_data is not None and not macro_kr_data.empty:
        macro_parts.append(macro_kr_data)
    if macro_global_data is not None and not macro_global_data.empty:
        macro_parts.append(macro_global_data)
    
    macro_data = pd.concat(macro_parts, ignore_index=True) if macro_parts else None
    
    # =====================================
    # 13. 재무비율 계산
    # =====================================
    ratio_data = None
    if financial_data is not None and not financial_data.empty:
        tracker.start_step("재무비율 계산", len(stock_codes))
        
        try:
            from processors.ratio_calculator import calculate_ratios_for_all
            ratio_data = calculate_ratios_for_all(financial_data, market_cap_df)
            if ratio_data is not None and not ratio_data.empty:
                tracker.update(len(ratio_data))
                tracker.finish_step(f"{len(ratio_data):,}건 계산")
            else:
                tracker.finish_step("계산 데이터 없음")
        except Exception as e:
            logger.error(f"재무비율 오류: {e}")
            tracker.finish_step("계산 실패")
    
    # =====================================
    # 14. 엑셀 파일 생성
    # =====================================
    tracker.start_step("엑셀 파일 생성", 1)
    
    try:
        exporter = ExcelExporter(output_dir='outputs')
        
        filepath = exporter.export_all(
            financial_data=financial_data,
            market_data=market_cap_df,  # 시장데이터 통합
            ratio_data=ratio_data,       # 재무비율 추가
            macro_data=macro_data,
            stock_list=stock_list,
            filename=args.output
        )
        
        tracker.update(1)
        tracker.finish_step(f"저장 완료: {os.path.basename(filepath)}")
    except Exception as e:
        logger.error(f"엑셀 저장 오류: {e}")
        tracker.finish_step("저장 실패")
        return
    
    # =====================================
    # 15. 완료 요약
    # =====================================
    tracker.show_summary()
    
    print(f"\n📂 출력 파일: {filepath}")
    print("\n💡 사용 팁:")
    print("  • 엑셀에서 '데이터 > 필터'로 조건 검색")
    print("  • '📚 활용가이드' 시트에서 사용법 확인")
    print("  • '📈 재무비율' 시트에서 60개+ 지표 확인")



def parse_args():
    """명령줄 파싱"""
    parser = argparse.ArgumentParser(
        description='국내 주식 재무데이터 수집 시스템 (Pro-Level)',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--market', type=str, default='KRX',
                       choices=['KRX', 'KOSPI', 'KOSDAQ', 'KONEX'],
                       help='대상 시장 (기본: KRX 전체)')
    
    # 3년치가 기본 (OpenDART 일일 10,000건 제한 고려)
    parser.add_argument('--years', type=int, nargs='+',
                       default=[2022, 2023, 2024],
                       help='수집 연도 (기본: 최근 3년, API 제한 고려)')
    
    parser.add_argument('--select', type=str, default=None,
                       help='수집 항목 선택: financial,indicators,market,macro')
    
    parser.add_argument('--interactive', action='store_true',
                       help='대화형 메뉴 모드')
    
    parser.add_argument('--quick', action='store_true',
                       help='빠른 테스트 (100종목만)')
    
    parser.add_argument('--check-deps', action='store_true',
                       help='의존성 체크만')
    
    parser.add_argument('--skip-dep-check', action='store_true',
                       help='의존성 체크 건너뛰기')
    
    parser.add_argument('--output', type=str, default=None,
                       help='출력 파일명')
    
    return parser.parse_args()


def load_config(path: str = "config/api_keys.yaml") -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def load_config_safe() -> Optional[dict]:
    try:
        return load_config()
    except:
        return None


if __name__ == "__main__":
    main()
