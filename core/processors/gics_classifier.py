"""
GICS 섹터 분류 관리자 (v3.0)
- 글로벌 산업 분류 기준
- 4단계 계층 구조
"""

import pandas as pd
from typing import Dict, List, Optional
import logging

logger = logging.getLogger("kr_stock_collector.gics")


# GICS 4단계 계층 구조
GICS_HIERARCHY = {
    # Level 1: Sector (10)
    '10': {
        'name_en': 'Energy',
        'name_kr': '에너지',
        'industry_groups': {
            '1010': {
                'name_en': 'Energy',
                'name_kr': '에너지',
                'industries': {
                    '101010': {'name_en': 'Energy Equipment & Services', 'name_kr': '에너지장비서비스'},
                    '101020': {'name_en': 'Oil, Gas & Consumable Fuels', 'name_kr': '석유가스'},
                }
            }
        }
    },
    '15': {
        'name_en': 'Materials',
        'name_kr': '소재',
        'industry_groups': {
            '1510': {
                'name_en': 'Materials',
                'name_kr': '소재',
                'industries': {
                    '151010': {'name_en': 'Chemicals', 'name_kr': '화학'},
                    '151020': {'name_en': 'Construction Materials', 'name_kr': '건설자재'},
                    '151030': {'name_en': 'Containers & Packaging', 'name_kr': '용기포장'},
                    '151040': {'name_en': 'Metals & Mining', 'name_kr': '금속광업'},
                    '151050': {'name_en': 'Paper & Forest Products', 'name_kr': '제지'},
                }
            }
        }
    },
    '20': {
        'name_en': 'Industrials',
        'name_kr': '산업재',
        'industry_groups': {
            '2010': {'name_en': 'Capital Goods', 'name_kr': '자본재'},
            '2020': {'name_en': 'Commercial & Professional Services', 'name_kr': '상업서비스'},
            '2030': {'name_en': 'Transportation', 'name_kr': '운송'},
        }
    },
    '25': {
        'name_en': 'Consumer Discretionary',
        'name_kr': '경기소비재',
        'industry_groups': {
            '2510': {'name_en': 'Automobiles & Components', 'name_kr': '자동차부품'},
            '2520': {'name_en': 'Consumer Durables & Apparel', 'name_kr': '내구소비재'},
            '2530': {'name_en': 'Consumer Services', 'name_kr': '소비자서비스'},
            '2550': {'name_en': 'Retailing', 'name_kr': '소매'},
        }
    },
    '30': {
        'name_en': 'Consumer Staples',
        'name_kr': '필수소비재',
        'industry_groups': {
            '3010': {'name_en': 'Food & Staples Retailing', 'name_kr': '식품소매'},
            '3020': {'name_en': 'Food, Beverage & Tobacco', 'name_kr': '식품음료'},
            '3030': {'name_en': 'Household & Personal Products', 'name_kr': '가정용품'},
        }
    },
    '35': {
        'name_en': 'Health Care',
        'name_kr': '헬스케어',
        'industry_groups': {
            '3510': {'name_en': 'Health Care Equipment & Services', 'name_kr': '의료장비서비스'},
            '3520': {'name_en': 'Pharmaceuticals, Biotechnology & Life Sciences', 'name_kr': '제약바이오'},
        }
    },
    '40': {
        'name_en': 'Financials',
        'name_kr': '금융',
        'industry_groups': {
            '4010': {'name_en': 'Banks', 'name_kr': '은행'},
            '4020': {'name_en': 'Diversified Financials', 'name_kr': '다각화금융'},
            '4030': {'name_en': 'Insurance', 'name_kr': '보험'},
        }
    },
    '45': {
        'name_en': 'Information Technology',
        'name_kr': 'IT',
        'industry_groups': {
            '4510': {'name_en': 'Software & Services', 'name_kr': '소프트웨어'},
            '4520': {'name_en': 'Technology Hardware & Equipment', 'name_kr': 'IT하드웨어'},
            '4530': {'name_en': 'Semiconductors & Semiconductor Equipment', 'name_kr': '반도체'},
        }
    },
    '50': {
        'name_en': 'Communication Services',
        'name_kr': '커뮤니케이션',
        'industry_groups': {
            '5010': {'name_en': 'Telecommunication Services', 'name_kr': '통신서비스'},
            '5020': {'name_en': 'Media & Entertainment', 'name_kr': '미디어엔터'},
        }
    },
    '55': {
        'name_en': 'Utilities',
        'name_kr': '유틸리티',
        'industry_groups': {
            '5510': {'name_en': 'Utilities', 'name_kr': '유틸리티'},
        }
    },
    '60': {
        'name_en': 'Real Estate',
        'name_kr': '부동산',
        'industry_groups': {
            '6010': {'name_en': 'Real Estate', 'name_kr': '부동산'},
        }
    },
}

# 한국 종목 → GICS 매핑 (주요 종목)
KR_STOCK_GICS_MAP = {
    '005930': {'sector': '45', 'industry': '453010', 'name': '삼성전자'},  # 반도체
    '000660': {'sector': '45', 'industry': '453010', 'name': 'SK하이닉스'},
    '035420': {'sector': '45', 'industry': '451020', 'name': 'NAVER'},  # 인터넷
    '035720': {'sector': '50', 'industry': '502010', 'name': '카카오'},  # 미디어
    '051910': {'sector': '15', 'industry': '151010', 'name': 'LG화학'},  # 화학
    '006400': {'sector': '45', 'industry': '452030', 'name': '삼성SDI'},  # 전자장비
    '005380': {'sector': '25', 'industry': '251020', 'name': '현대자동차'},  # 자동차
    '000270': {'sector': '25', 'industry': '251020', 'name': '기아'},
    '005490': {'sector': '15', 'industry': '151040', 'name': 'POSCO홀딩스'},  # 철강
    '055550': {'sector': '40', 'industry': '401010', 'name': '신한지주'},  # 은행
    '105560': {'sector': '40', 'industry': '401010', 'name': 'KB금융'},
    '207940': {'sector': '35', 'industry': '352010', 'name': '삼성바이오로직스'},  # 바이오
    '068270': {'sector': '35', 'industry': '352020', 'name': '셀트리온'},
}


class GICSClassifier:
    """GICS 분류기"""
    
    def __init__(self):
        self.hierarchy = GICS_HIERARCHY
        self.kr_map = KR_STOCK_GICS_MAP
    
    def get_sector(self, sector_code: str) -> Dict:
        """섹터 정보 조회"""
        if sector_code in self.hierarchy:
            return {
                'code': sector_code,
                **self.hierarchy[sector_code]
            }
        return {}
    
    def get_sector_name(self, sector_code: str, lang: str = 'kr') -> str:
        """섹터명 조회"""
        sector = self.get_sector(sector_code)
        if lang == 'kr':
            return sector.get('name_kr', '')
        return sector.get('name_en', '')
    
    def classify_stock(self, stock_code: str) -> Dict:
        """종목 GICS 분류"""
        if stock_code in self.kr_map:
            mapping = self.kr_map[stock_code]
            sector_code = mapping['sector']
            
            return {
                'stock_code': stock_code,
                'sector_code': sector_code,
                'sector_name': self.get_sector_name(sector_code),
                'industry_code': mapping.get('industry', ''),
            }
        
        return {'stock_code': stock_code, 'sector_code': None}
    
    def get_sector_stocks(self, sector_code: str) -> List[str]:
        """섹터별 종목 조회"""
        return [
            code for code, info in self.kr_map.items()
            if info['sector'] == sector_code
        ]
    
    def get_all_sectors(self) -> pd.DataFrame:
        """전체 섹터 리스트"""
        sectors = []
        for code, info in self.hierarchy.items():
            sectors.append({
                'code': code,
                'name_en': info['name_en'],
                'name_kr': info['name_kr']
            })
        return pd.DataFrame(sectors)
    
    def add_gics_to_df(self, df: pd.DataFrame, code_col: str = '종목코드') -> pd.DataFrame:
        """DataFrame에 GICS 컬럼 추가"""
        result = df.copy()
        
        result['gics_sector'] = result[code_col].apply(
            lambda x: self.kr_map.get(x, {}).get('sector', '')
        )
        result['gics_sector_name'] = result['gics_sector'].apply(
            lambda x: self.get_sector_name(x) if x else ''
        )
        
        return result


def get_sector_statistics(df: pd.DataFrame, sector_col: str = 'gics_sector') -> pd.DataFrame:
    """섹터별 통계"""
    if df is None or df.empty or sector_col not in df.columns:
        return pd.DataFrame()
    
    stats = df.groupby(sector_col).agg({
        'PER': ['count', 'mean', 'median'],
        'PBR': ['mean', 'median'],
        'ROE': ['mean', 'median'] if 'ROE' in df.columns else 'count'
    }).round(2)
    
    return stats


if __name__ == '__main__':
    # 테스트
    classifier = GICSClassifier()
    
    # 섹터 조회
    print("전체 섹터:")
    print(classifier.get_all_sectors())
    
    # 종목 분류
    print("\n삼성전자 GICS:")
    print(classifier.classify_stock('005930'))
    
    # IT 섹터 종목
    print("\nIT 섹터 종목:")
    print(classifier.get_sector_stocks('45'))
