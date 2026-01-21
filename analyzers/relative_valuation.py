"""
섹터별 상대가치 평가 모듈 (v2.0)
- WICS 섹터 분류 기반
- 섹터 내 순위 계산
- 업종 평균 대비 할인/프리미엄
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging

logger = logging.getLogger("kr_stock_collector.relative_valuation")


# WICS 섹터 분류 (KRX 기준)
WICS_SECTORS = {
    '에너지': ['정유', '석유가스', '가스'],
    '소재': ['화학', '철강', '비철금속', '종이', '포장재', '건설자재'],
    '산업재': ['건설', '기계', '항공우주', '방위산업', '전기장비', '조선'],
    '경기소비재': ['자동차', '자동차부품', '섬유의복', '레저', '호텔', '미디어'],
    '필수소비재': ['음식료', '담배', '가정용품', '소매'],
    'IT': ['반도체', 'IT하드웨어', '소프트웨어', 'IT서비스', '통신장비'],
    '통신서비스': ['유선통신', '무선통신', '인터넷'],
    '금융': ['은행', '증권', '보험', '기타금융'],
    '헬스케어': ['제약', '바이오', '의료기기', '헬스케어서비스'],
    '유틸리티': ['전력', '가스', '상수도'],
    '부동산': ['리츠', '부동산개발', '부동산서비스']
}


class RelativeValuator:
    """섹터별 상대가치 평가기"""
    
    # 핵심 밸류에이션 지표
    VALUATION_METRICS = [
        'PER', 'PBR', 'PSR', 'PCR', 'EV/EBITDA',
        'ROE(%)', 'ROA(%)', 'ROIC(%)',
        '영업이익률(%)', '순이익률(%)',
        '부채비율(%)', '매출성장률(%)'
    ]
    
    def __init__(self):
        pass
    
    def calculate_sector_stats(self, df: pd.DataFrame, metric: str) -> pd.DataFrame:
        """섹터별 통계 계산"""
        if 'sector' not in df.columns or metric not in df.columns:
            return pd.DataFrame()
        
        stats = df.groupby('sector')[metric].agg([
            ('count', 'count'),
            ('mean', 'mean'),
            ('median', 'median'),
            ('std', 'std'),
            ('min', 'min'),
            ('max', 'max'),
            ('q25', lambda x: x.quantile(0.25)),
            ('q75', lambda x: x.quantile(0.75))
        ]).reset_index()
        
        return stats
    
    def calculate_sector_rank(self, df: pd.DataFrame, metric: str, ascending: bool = True) -> pd.DataFrame:
        """
        섹터 내 순위 계산
        
        Args:
            df: 종목 데이터
            metric: 순위 계산할 지표
            ascending: True면 낮을수록 좋음 (PER), False면 높을수록 좋음 (ROE)
        """
        if 'sector' not in df.columns or metric not in df.columns:
            return df
        
        result = df.copy()
        
        # 섹터 내 순위 (%)
        result[f'{metric}_섹터순위%'] = result.groupby('sector')[metric].rank(
            pct=True, ascending=ascending, na_option='bottom'
        )
        
        # 섹터 내 절대순위
        result[f'{metric}_섹터순위'] = result.groupby('sector')[metric].rank(
            ascending=ascending, na_option='bottom'
        ).astype(int)
        
        return result
    
    def calculate_sector_premium(self, df: pd.DataFrame, metric: str) -> pd.DataFrame:
        """
        섹터 중앙값 대비 할인/프리미엄 계산
        
        Returns:
            premium > 0: 섹터 평균 대비 비쌈 (또는 높음)
            premium < 0: 섹터 평균 대비 쌈 (또는 낮음)
        """
        if 'sector' not in df.columns or metric not in df.columns:
            return df
        
        result = df.copy()
        
        # 섹터별 중앙값
        sector_median = result.groupby('sector')[metric].transform('median')
        
        # 프리미엄 계산 (%)
        result[f'{metric}_섹터프리미엄%'] = (
            (result[metric] - sector_median) / sector_median * 100
        ).round(2)
        
        return result
    
    def add_all_relative_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """모든 상대가치 지표 추가"""
        result = df.copy()
        
        # PER, PBR, PSR, EV/EBITDA: 낮을수록 좋음
        for metric in ['PER', 'PBR', 'PSR', 'EV/EBITDA']:
            if metric in result.columns:
                result = self.calculate_sector_rank(result, metric, ascending=True)
                result = self.calculate_sector_premium(result, metric)
        
        # ROE, ROA, ROIC, 이익률: 높을수록 좋음
        for metric in ['ROE(%)', 'ROA(%)', 'ROIC(%)', '영업이익률(%)', '순이익률(%)', '매출성장률(%)']:
            if metric in result.columns:
                result = self.calculate_sector_rank(result, metric, ascending=False)
                result = self.calculate_sector_premium(result, metric)
        
        # 부채비율: 낮을수록 좋음
        if '부채비율(%)' in result.columns:
            result = self.calculate_sector_rank(result, '부채비율(%)', ascending=True)
        
        return result
    
    def get_undervalued_in_sector(
        self,
        df: pd.DataFrame,
        sector: str = None,
        per_percentile: float = 0.3,    # 섹터 내 PER 하위 30%
        roe_percentile: float = 0.7     # 섹터 내 ROE 상위 30%
    ) -> pd.DataFrame:
        """섹터 내 저평가 종목 찾기"""
        
        result = self.add_all_relative_metrics(df)
        
        conditions = pd.Series([True] * len(result))
        
        if sector:
            conditions &= (result['sector'] == sector)
        
        if 'PER_섹터순위%' in result.columns:
            conditions &= (result['PER_섹터순위%'] <= per_percentile)
        
        if 'ROE(%)_섹터순위%' in result.columns:
            conditions &= (result['ROE(%)_섹터순위%'] >= roe_percentile)
        
        return result[conditions].sort_values('PER_섹터순위%')
    
    def generate_sector_report(self, df: pd.DataFrame) -> Dict:
        """섹터별 요약 리포트 생성"""
        if 'sector' not in df.columns:
            return {}
        
        report = {}
        
        for sector in df['sector'].dropna().unique():
            sector_df = df[df['sector'] == sector]
            
            report[sector] = {
                '종목수': len(sector_df),
                'PER_중앙값': sector_df['PER'].median() if 'PER' in sector_df.columns else None,
                'PBR_중앙값': sector_df['PBR'].median() if 'PBR' in sector_df.columns else None,
                'ROE_평균': sector_df['ROE(%)'].mean() if 'ROE(%)' in sector_df.columns else None,
                '시총_합계': sector_df['market_cap'].sum() if 'market_cap' in sector_df.columns else None,
            }
        
        return report


def add_relative_valuation(ratio_df: pd.DataFrame, stock_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    재무비율 데이터에 섹터 상대가치 지표 추가
    
    Args:
        ratio_df: 재무비율 데이터
        stock_df: 종목 기본정보 (섹터 포함)
    """
    if ratio_df is None or ratio_df.empty:
        return ratio_df
    
    result = ratio_df.copy()
    
    # 섹터 정보 병합
    if stock_df is not None and 'sector' in stock_df.columns:
        code_col = '종목코드' if '종목코드' in result.columns else 'stock_code'
        if code_col in result.columns:
            sector_map = dict(zip(stock_df['Code'], stock_df['sector']))
            result['sector'] = result[code_col].map(sector_map)
    
    # 상대가치 계산
    valuator = RelativeValuator()
    result = valuator.add_all_relative_metrics(result)
    
    logger.info(f"상대가치 지표 추가 완료: {len(result)}개 종목")
    
    return result


if __name__ == '__main__':
    # 테스트
    test_data = pd.DataFrame({
        '종목코드': ['005930', '000660', '035420', '035720'],
        '기업명': ['삼성전자', 'SK하이닉스', 'NAVER', '카카오'],
        'sector': ['IT', 'IT', 'IT', 'IT'],
        'PER': [12.5, 8.2, 35.0, 42.0],
        'ROE(%)': [15.2, 22.1, 12.0, 8.5]
    })
    
    valuator = RelativeValuator()
    result = valuator.add_all_relative_metrics(test_data)
    print(result[['기업명', 'PER', 'PER_섹터순위%', 'ROE(%)', 'ROE(%)_섹터순위%']])
