"""
스크리닝 설정 모듈
- 선택적 스크리닝 카테고리 정의
- 대화형 메뉴 지원
"""

from typing import Dict, List, Set
from dataclasses import dataclass, field
import json


@dataclass
class ScreeningConfig:
    """스크리닝 설정 클래스"""
    
    # 재무제표
    balance_sheet: bool = True        # 재무상태표
    income_statement: bool = True     # 손익계산서
    cash_flow: bool = True            # 현금흐름표
    comprehensive_income: bool = False  # 포괄손익계산서
    
    # 투자지표
    profitability: bool = True        # 수익성 지표
    stability: bool = True            # 안정성 지표
    growth: bool = True               # 성장성 지표
    valuation: bool = True            # 밸류에이션
    activity: bool = True             # 활동성 지표
    cashflow_quality: bool = True     # 현금흐름 품질
    
    # 시장 데이터
    price: bool = True                # 주가
    volume: bool = True               # 거래량
    market_cap: bool = True           # 시가총액
    foreign_holding: bool = False     # 외국인 보유
    institutional: bool = False       # 기관 보유
    
    # 한국 경제
    kr_interest_rate: bool = True     # 금리
    kr_inflation: bool = True         # 물가
    kr_exchange_rate: bool = True     # 환율
    kr_trade: bool = True             # 수출입
    kr_money_supply: bool = False     # 통화량
    kr_employment: bool = False       # 고용
    kr_sentiment: bool = False        # 심리지수
    
    # 글로벌 경제
    us_rates: bool = True             # 미국 금리
    volatility: bool = True           # VIX
    commodities: bool = True          # 원자재
    global_fx: bool = False           # 글로벌 환율
    global_equity: bool = False       # 글로벌 주식
    credit_spread: bool = False       # 신용 스프레드
    
    def to_dict(self) -> Dict[str, bool]:
        """설정을 딕셔너리로 변환"""
        return {
            'financial_statements': {
                'balance_sheet': self.balance_sheet,
                'income_statement': self.income_statement,
                'cash_flow': self.cash_flow,
                'comprehensive_income': self.comprehensive_income,
            },
            'investment_indicators': {
                'profitability': self.profitability,
                'stability': self.stability,
                'growth': self.growth,
                'valuation': self.valuation,
                'activity': self.activity,
                'cashflow_quality': self.cashflow_quality,
            },
            'market_data': {
                'price': self.price,
                'volume': self.volume,
                'market_cap': self.market_cap,
                'foreign_holding': self.foreign_holding,
                'institutional': self.institutional,
            },
            'macro_korea': {
                'interest_rate': self.kr_interest_rate,
                'inflation': self.kr_inflation,
                'exchange_rate': self.kr_exchange_rate,
                'trade': self.kr_trade,
                'money_supply': self.kr_money_supply,
                'employment': self.kr_employment,
                'sentiment': self.kr_sentiment,
            },
            'macro_global': {
                'us_rates': self.us_rates,
                'volatility': self.volatility,
                'commodities': self.commodities,
                'global_fx': self.global_fx,
                'global_equity': self.global_equity,
                'credit_spread': self.credit_spread,
            }
        }
    
    def get_enabled_categories(self) -> Set[str]:
        """활성화된 카테고리 반환"""
        enabled = set()
        
        if any([self.balance_sheet, self.income_statement, self.cash_flow]):
            enabled.add('financial')
        if any([self.profitability, self.stability, self.growth, self.valuation]):
            enabled.add('indicators')
        if any([self.price, self.volume, self.market_cap]):
            enabled.add('market')
        if any([self.kr_interest_rate, self.kr_inflation, self.kr_exchange_rate]):
            enabled.add('macro_kr')
        if any([self.us_rates, self.volatility, self.commodities]):
            enabled.add('macro_global')
        
        return enabled
    
    @classmethod
    def from_selection(cls, selections: List[str]) -> 'ScreeningConfig':
        """
        CLI 선택에서 설정 생성
        
        Args:
            selections: ['financial', 'indicators', 'macro_kr'] 등
        """
        config = cls()
        
        # 모두 비활성화
        for field_name in config.__dataclass_fields__:
            setattr(config, field_name, False)
        
        # 선택된 것만 활성화
        selection_set = set(s.lower().strip() for s in selections)
        
        if 'financial' in selection_set or 'all' in selection_set:
            config.balance_sheet = True
            config.income_statement = True
            config.cash_flow = True
        
        if 'indicators' in selection_set or 'all' in selection_set:
            config.profitability = True
            config.stability = True
            config.growth = True
            config.valuation = True
            config.activity = True
            config.cashflow_quality = True
        
        if 'market' in selection_set or 'all' in selection_set:
            config.price = True
            config.volume = True
            config.market_cap = True
        
        if 'macro_kr' in selection_set or 'macro' in selection_set or 'all' in selection_set:
            config.kr_interest_rate = True
            config.kr_inflation = True
            config.kr_exchange_rate = True
            config.kr_trade = True
        
        if 'macro_global' in selection_set or 'macro' in selection_set or 'all' in selection_set:
            config.us_rates = True
            config.volatility = True
            config.commodities = True
        
        return config
    
    @classmethod
    def preset_basic(cls) -> 'ScreeningConfig':
        """기본 프리셋 (재무제표 + 투자지표)"""
        return cls.from_selection(['financial', 'indicators'])
    
    @classmethod
    def preset_market(cls) -> 'ScreeningConfig':
        """시장 프리셋 (주가 + 지표)"""
        return cls.from_selection(['market', 'indicators'])
    
    @classmethod
    def preset_macro(cls) -> 'ScreeningConfig':
        """거시경제 프리셋"""
        return cls.from_selection(['macro_kr', 'macro_global'])
    
    @classmethod
    def preset_full(cls) -> 'ScreeningConfig':
        """전체 프리셋"""
        return cls.from_selection(['all'])


# 스크리닝 옵션 설명
SCREENING_OPTIONS = {
    'financial': {
        'name': '재무제표',
        'description': '재무상태표, 손익계산서, 현금흐름표',
        'items': ['balance_sheet', 'income_statement', 'cash_flow']
    },
    'indicators': {
        'name': '투자지표',
        'description': 'ROE, PER, PBR, 성장률 등 50개+ 지표',
        'items': ['profitability', 'stability', 'growth', 'valuation', 'activity']
    },
    'market': {
        'name': '시장데이터',
        'description': '주가, 거래량, 시가총액',
        'items': ['price', 'volume', 'market_cap']
    },
    'macro_kr': {
        'name': '한국경제',
        'description': '금리, 물가, 환율, 수출입 등 30개+ 지표',
        'items': ['kr_interest_rate', 'kr_inflation', 'kr_exchange_rate', 'kr_trade']
    },
    'macro_global': {
        'name': '글로벌경제',
        'description': '미국금리, VIX, 원자재 등 50개+ 지표',
        'items': ['us_rates', 'volatility', 'commodities']
    }
}


def show_interactive_menu() -> ScreeningConfig:
    """
    대화형 메뉴로 스크리닝 옵션 선택
    
    Returns:
        ScreeningConfig 객체
    """
    print("\n" + "=" * 60)
    print("📊 스크리닝 옵션 선택")
    print("=" * 60)
    
    print("\n🔹 프리셋 선택:")
    print("  1. 기본 (재무제표 + 투자지표)")
    print("  2. 시장 (주가 + 투자지표)")
    print("  3. 거시경제 (한국 + 글로벌)")
    print("  4. 전체 (모든 데이터)")
    print("  5. 커스텀 (직접 선택)")
    
    try:
        choice = input("\n선택 (1-5) [기본: 4]: ").strip() or "4"
        
        if choice == "1":
            return ScreeningConfig.preset_basic()
        elif choice == "2":
            return ScreeningConfig.preset_market()
        elif choice == "3":
            return ScreeningConfig.preset_macro()
        elif choice == "4":
            return ScreeningConfig.preset_full()
        elif choice == "5":
            return _custom_selection()
        else:
            return ScreeningConfig.preset_full()
    except:
        return ScreeningConfig.preset_full()


def _custom_selection() -> ScreeningConfig:
    """커스텀 선택 메뉴"""
    print("\n🔹 수집할 데이터 선택 (콤마로 구분):")
    
    for key, info in SCREENING_OPTIONS.items():
        print(f"  {key}: {info['name']} - {info['description']}")
    
    print("\n예시: financial,indicators,macro_kr")
    
    try:
        selection = input("\n선택: ").strip()
        if not selection:
            return ScreeningConfig.preset_full()
        
        items = [s.strip() for s in selection.split(',')]
        return ScreeningConfig.from_selection(items)
    except:
        return ScreeningConfig.preset_full()


def get_screening_summary(config: ScreeningConfig) -> str:
    """스크리닝 설정 요약 문자열"""
    enabled = config.get_enabled_categories()
    
    parts = []
    if 'financial' in enabled:
        parts.append("📑 재무제표")
    if 'indicators' in enabled:
        parts.append("📊 투자지표")
    if 'market' in enabled:
        parts.append("📈 시장데이터")
    if 'macro_kr' in enabled:
        parts.append("🇰🇷 한국경제")
    if 'macro_global' in enabled:
        parts.append("🌍 글로벌경제")
    
    return " | ".join(parts) if parts else "없음"
