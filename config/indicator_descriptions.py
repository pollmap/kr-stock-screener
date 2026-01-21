"""
지표 설명 사전
- 모든 투자지표, 재무계정, 거시경제 지표 설명
- 초보자용 해석 가이드
- 사용 방법 및 주의사항
"""


# =====================================
# 투자지표 설명 (60개+)
# =====================================
INVESTMENT_INDICATORS = {
    # 수익성 지표
    'roe': {
        'name': 'ROE (자기자본이익률)',
        'formula': '순이익 ÷ 자기자본 × 100',
        'unit': '%',
        'description': '주주가 투자한 자본으로 얼마나 이익을 냈는지 보여줍니다.',
        'interpretation': '높을수록 좋음. 보통 10% 이상이면 양호, 15% 이상이면 우수.',
        'caution': '부채가 많으면 ROE가 높아질 수 있으니 부채비율도 함께 확인하세요.',
        'category': '수익성'
    },
    'roa': {
        'name': 'ROA (총자산이익률)',
        'formula': '순이익 ÷ 총자산 × 100',
        'unit': '%',
        'description': '회사의 모든 자산을 활용해 얼마나 이익을 냈는지 보여줍니다.',
        'interpretation': '높을수록 좋음. 업종에 따라 다르나 5% 이상이면 양호.',
        'caution': '자산 규모가 큰 제조업은 낮고, IT는 높은 경향이 있습니다.',
        'category': '수익성'
    },
    'roic': {
        'name': 'ROIC (투하자본이익률)',
        'formula': 'NOPAT ÷ 투하자본 × 100',
        'unit': '%',
        'description': '실제 영업에 투입된 자본의 수익률입니다. 가장 정확한 수익성 지표.',
        'interpretation': 'WACC(자본비용)보다 높으면 가치 창출, 낮으면 가치 파괴.',
        'caution': '10% 이상이면 양호. 업종 평균과 비교하세요.',
        'category': '수익성'
    },
    'gross_margin': {
        'name': '매출총이익률',
        'formula': '(매출 - 매출원가) ÷ 매출 × 100',
        'unit': '%',
        'description': '제품/서비스의 원가 경쟁력을 보여줍니다.',
        'interpretation': '높을수록 좋음. 동종업계 비교가 중요.',
        'caution': '업종별 차이가 큼 (제조: 20-30%, 소프트웨어: 70-90%).',
        'category': '수익성'
    },
    'operating_margin': {
        'name': '영업이익률',
        'formula': '영업이익 ÷ 매출 × 100',
        'unit': '%',
        'description': '본업에서 얼마나 효율적으로 이익을 내는지 보여줍니다.',
        'interpretation': '높을수록 좋음. 10% 이상이면 양호.',
        'caution': '일회성 비용이 있으면 왜곡될 수 있습니다.',
        'category': '수익성'
    },
    'net_margin': {
        'name': '순이익률',
        'formula': '순이익 ÷ 매출 × 100',
        'unit': '%',
        'description': '최종적으로 남는 이익의 비율입니다.',
        'interpretation': '높을수록 좋음. 5% 이상이면 양호.',
        'caution': '일회성 손익에 영향받으니 영업이익률과 함께 확인하세요.',
        'category': '수익성'
    },
    'ebitda_margin': {
        'name': 'EBITDA 마진',
        'formula': 'EBITDA ÷ 매출 × 100',
        'unit': '%',
        'description': '감가상각 전 영업 수익성입니다. 현금 창출력 지표.',
        'interpretation': '높을수록 좋음. 15% 이상이면 양호.',
        'caution': '설비투자가 많은 기업 비교에 유용.',
        'category': '수익성'
    },
    
    # 안정성 지표
    'debt_ratio': {
        'name': '부채비율',
        'formula': '총부채 ÷ 자기자본 × 100',
        'unit': '%',
        'description': '자기자본 대비 빚의 비율입니다.',
        'interpretation': '낮을수록 안전. 100% 이하 양호, 200% 이상 주의.',
        'caution': '업종마다 다름. 금융업은 높고, IT는 낮은 경향.',
        'category': '안정성'
    },
    'current_ratio': {
        'name': '유동비율',
        'formula': '유동자산 ÷ 유동부채 × 100',
        'unit': '%',
        'description': '1년 내 갚아야 할 빚을 갚을 능력입니다.',
        'interpretation': '150% 이상 안전, 100% 미만 위험.',
        'caution': '재고자산이 많으면 실제 유동성은 낮을 수 있음.',
        'category': '안정성'
    },
    'quick_ratio': {
        'name': '당좌비율',
        'formula': '(유동자산 - 재고) ÷ 유동부채 × 100',
        'unit': '%',
        'description': '재고를 제외한 순수 유동성입니다.',
        'interpretation': '100% 이상 안전.',
        'caution': '유동비율과 함께 확인하세요.',
        'category': '안정성'
    },
    'interest_coverage': {
        'name': '이자보상배율',
        'formula': '영업이익 ÷ 이자비용',
        'unit': '배',
        'description': '이자를 갚을 능력입니다.',
        'interpretation': '5배 이상 안전, 1배 미만 위험(이자도 못 갚음).',
        'caution': '3배 이상이면 양호.',
        'category': '안정성'
    },
    
    # 밸류에이션 지표
    'per': {
        'name': 'PER (주가수익비율)',
        'formula': '주가 ÷ 주당순이익',
        'unit': '배',
        'description': '현재 이익 기준으로 원금 회수까지 걸리는 년수입니다.',
        'interpretation': '낮을수록 저평가. 10배 이하 저평가, 20배 이상 고평가 가능성.',
        'caution': '적자기업은 계산 불가. 성장주는 높아도 괜찮을 수 있음.',
        'category': '밸류에이션'
    },
    'pbr': {
        'name': 'PBR (주가순자산비율)',
        'formula': '주가 ÷ 주당순자산',
        'unit': '배',
        'description': '회사를 청산할 때 돌려받을 수 있는 금액 대비 주가입니다.',
        'interpretation': '1 이하면 청산가치보다 싸게 거래 (저평가 가능성).',
        'caution': '무형자산이 많은 IT기업은 높은 게 정상.',
        'category': '밸류에이션'
    },
    'psr': {
        'name': 'PSR (주가매출비율)',
        'formula': '시가총액 ÷ 매출',
        'unit': '배',
        'description': '매출 대비 주가 수준입니다.',
        'interpretation': '적자기업 평가에 유용. 1 이하면 저평가 가능성.',
        'caution': '이익률이 낮으면 PSR이 낮아도 좋지 않을 수 있음.',
        'category': '밸류에이션'
    },
    'ev_ebitda': {
        'name': 'EV/EBITDA',
        'formula': '기업가치 ÷ EBITDA',
        'unit': '배',
        'description': '인수합병 시 가장 많이 쓰는 밸류에이션 지표입니다.',
        'interpretation': '낮을수록 저평가. 10배 이하면 저평가 가능성.',
        'caution': '성장성이 높으면 높아도 괜찮을 수 있음.',
        'category': '밸류에이션'
    },
    'dividend_yield': {
        'name': '배당수익률',
        'formula': '주당배당금 ÷ 주가 × 100',
        'unit': '%',
        'description': '투자금 대비 받는 배당의 비율입니다.',
        'interpretation': '높을수록 배당 매력. 3% 이상이면 양호.',
        'caution': '배당을 유지할 수 있는지 배당성향도 확인하세요.',
        'category': '밸류에이션'
    },
    'earnings_yield': {
        'name': 'Earnings Yield',
        'formula': '순이익 ÷ 시가총액 × 100',
        'unit': '%',
        'description': 'PER의 역수. 주식 투자 수익률 개념.',
        'interpretation': '국채금리보다 높으면 주식이 매력적.',
        'caution': '금리와 비교해서 판단하세요.',
        'category': '밸류에이션'
    },
    'fcf_yield': {
        'name': 'FCF Yield',
        'formula': '잉여현금흐름 ÷ 시가총액 × 100',
        'unit': '%',
        'description': '현금 기준 투자 수익률입니다.',
        'interpretation': '높을수록 현금 창출력 대비 저평가.',
        'caution': '5% 이상이면 매력적.',
        'category': '밸류에이션'
    },
    
    # 성장성 지표
    'revenue_growth': {
        'name': '매출성장률',
        'formula': '(당기매출 - 전기매출) ÷ 전기매출 × 100',
        'unit': '%',
        'description': '전년 대비 매출이 얼마나 늘었는지 보여줍니다.',
        'interpretation': '양수면 성장, 음수면 역성장.',
        'caution': '일회성 매출 증가인지 지속적인지 확인하세요.',
        'category': '성장성'
    },
    'operating_income_growth': {
        'name': '영업이익성장률',
        'formula': '(당기영업이익 - 전기영업이익) ÷ 전기영업이익 × 100',
        'unit': '%',
        'description': '본업 이익의 성장률입니다.',
        'interpretation': '매출성장률보다 높으면 수익성 개선 중.',
        'caution': '기저효과에 주의하세요.',
        'category': '성장성'
    },
    'sgr': {
        'name': 'SGR (지속가능성장률)',
        'formula': 'ROE × (1 - 배당성향)',
        'unit': '%',
        'description': '외부 자금 없이 자체적으로 성장할 수 있는 속도입니다.',
        'interpretation': '실제 성장률이 SGR보다 높으면 자금 조달 필요.',
        'caution': '장기 성장 계획 수립에 유용.',
        'category': '성장성'
    },
    
    # 현금흐름 품질
    'cfo_to_ni': {
        'name': '영업CF/순이익 비율',
        'formula': '영업활동현금흐름 ÷ 순이익',
        'unit': '배',
        'description': '이익의 질을 나타냅니다. 현금으로 뒷받침되는 이익인지 확인.',
        'interpretation': '1 이상이면 이익의 질 양호. 1 미만이면 외상이 많음.',
        'caution': '지속적으로 1 미만이면 분식회계 의심.',
        'category': '현금흐름품질'
    },
    'altman_z': {
        'name': 'Altman Z-Score',
        'formula': '1.2×A + 1.4×B + 3.3×C + 0.6×D + 1.0×E',
        'unit': '점',
        'description': '부도 위험을 예측하는 점수입니다.',
        'interpretation': '3.0 이상: 안전 | 1.8-3.0: 회색지대 | 1.8 미만: 부도 위험.',
        'caution': '제조업 기준. 금융업에는 적용 불가.',
        'category': '현금흐름품질'
    },
    
    # 활동성 지표
    'asset_turnover': {
        'name': '총자산회전율',
        'formula': '매출 ÷ 총자산',
        'unit': '회',
        'description': '자산을 얼마나 효율적으로 활용하는지 보여줍니다.',
        'interpretation': '높을수록 좋음. 업종별 비교 필요.',
        'caution': '자산경량형(IT) 기업이 높음.',
        'category': '활동성'
    },
    'ccc': {
        'name': 'CCC (현금전환주기)',
        'formula': '매출채권회수기간 + 재고보유기간 - 매입채무지급기간',
        'unit': '일',
        'description': '현금이 묶여있는 기간입니다.',
        'interpretation': '짧을수록 좋음. 음수면 매우 우수 (선결제 사업).',
        'caution': '업종별 차이가 큼.',
        'category': '활동성'
    },
}


# =====================================
# 거시경제 지표 설명
# =====================================
MACRO_INDICATORS = {
    # 한국 금리
    '기준금리': {
        'name': '한국은행 기준금리',
        'description': '한국은행이 정하는 정책금리. 모든 금리의 기준.',
        'interpretation': '인상 시 주식/부동산 하락 압력, 인하 시 상승 압력.',
        'source': 'BOK',
        'category': '금리'
    },
    '국고채_3년': {
        'name': '국고채 3년물 금리',
        'description': '단기 채권 금리. 기준금리 방향 예측에 유용.',
        'interpretation': '기준금리보다 높으면 인상 기대, 낮으면 인하 기대.',
        'source': 'BOK',
        'category': '금리'
    },
    '국고채_10년': {
        'name': '국고채 10년물 금리',
        'description': '장기 금리. 경기 전망과 물가 기대 반영.',
        'interpretation': '3년물과의 차이(스프레드)가 좁아지면 경기 침체 신호.',
        'source': 'BOK',
        'category': '금리'
    },
    
    # 물가
    'CPI': {
        'name': '소비자물가지수',
        'description': '물가 상승률의 대표 지표.',
        'interpretation': '2% 수준이 목표. 높으면 금리 인상 압력.',
        'source': 'BOK',
        'category': '물가'
    },
    
    # 환율
    '원달러환율': {
        'name': '원/달러 환율',
        'description': '1달러당 원화 가격.',
        'interpretation': '상승 시 원화 약세, 수출주 유리. 하락 시 내수주 유리.',
        'source': 'BOK',
        'category': '환율'
    },
    
    # 미국
    'Fed_Funds': {
        'name': '미국 기준금리 (Fed Funds)',
        'description': '미 연준(Fed)이 정하는 정책금리. 글로벌 금융시장 기준.',
        'interpretation': '인상 시 달러 강세, 신흥국 자금 유출. 인하 시 반대.',
        'source': 'FRED',
        'category': '미국금리'
    },
    '미국채_10Y': {
        'name': '미국 국채 10년물',
        'description': '글로벌 무위험 금리의 기준.',
        'interpretation': '상승 시 주식 밸류에이션 하락 압력.',
        'source': 'FRED',
        'category': '미국금리'
    },
    '10Y-2Y_스프레드': {
        'name': '장단기 금리차 (10Y-2Y)',
        'description': '장기-단기 금리 차이. 경기 선행지표.',
        'interpretation': '음수(역전) 시 1-2년 후 경기침체 확률 높음.',
        'source': 'FRED',
        'category': '미국금리'
    },
    
    # 변동성
    'VIX': {
        'name': 'VIX (공포지수)',
        'description': 'S&P500 옵션 변동성. 시장 불안심리 지표.',
        'interpretation': '20 이하: 안정 | 20-30: 불안 | 30 이상: 공포.',
        'source': 'FRED',
        'category': '변동성'
    },
    
    # 원자재
    'WTI_원유': {
        'name': 'WTI 원유 가격',
        'description': '미국 원유 가격. 인플레이션 선행지표.',
        'interpretation': '상승 시 물가 상승 압력, 정유/화학주 유리.',
        'source': 'FRED',
        'category': '원자재'
    },
    '금': {
        'name': '금 가격',
        'description': '안전자산의 대표. 인플레이션 헤지 수단.',
        'interpretation': '불확실성 증가 시 상승. 금리 상승 시 하락 경향.',
        'source': 'FRED',
        'category': '원자재'
    },
}


# =====================================
# 재무제표 계정 설명
# =====================================
FINANCIAL_ACCOUNTS = {
    # 재무상태표 - 자산
    '자산총계': {
        'name': '자산총계',
        'description': '회사가 보유한 모든 자산의 합계.',
        'location': '재무상태표',
        'category': '자산'
    },
    '유동자산': {
        'name': '유동자산',
        'description': '1년 내에 현금화할 수 있는 자산 (현금, 매출채권, 재고 등).',
        'location': '재무상태표',
        'category': '자산'
    },
    '현금및현금성자산': {
        'name': '현금 및 현금성자산',
        'description': '즉시 사용 가능한 현금과 3개월 이내 현금화 가능한 자산.',
        'location': '재무상태표',
        'category': '자산'
    },
    
    # 재무상태표 - 부채
    '부채총계': {
        'name': '부채총계',
        'description': '회사가 갚아야 할 모든 빚의 합계.',
        'location': '재무상태표',
        'category': '부채'
    },
    '유동부채': {
        'name': '유동부채',
        'description': '1년 내에 갚아야 할 빚.',
        'location': '재무상태표',
        'category': '부채'
    },
    
    # 재무상태표 - 자본
    '자본총계': {
        'name': '자본총계(순자산)',
        'description': '자산에서 부채를 뺀 금액. 주주의 몫.',
        'location': '재무상태표',
        'category': '자본'
    },
    '이익잉여금': {
        'name': '이익잉여금',
        'description': '지금까지 벌어서 쌓아둔 이익. 배당 재원.',
        'location': '재무상태표',
        'category': '자본'
    },
    
    # 손익계산서
    '매출액': {
        'name': '매출액',
        'description': '회사가 번 총 수입. 매출총액.',
        'location': '손익계산서',
        'category': '손익'
    },
    '영업이익': {
        'name': '영업이익',
        'description': '본업에서 번 이익. 핵심 수익력 지표.',
        'location': '손익계산서',
        'category': '손익'
    },
    '당기순이익': {
        'name': '당기순이익',
        'description': '모든 수익과 비용을 계산한 후 최종 이익.',
        'location': '손익계산서',
        'category': '손익'
    },
    
    # 현금흐름표
    '영업활동현금흐름': {
        'name': '영업활동 현금흐름 (CFO)',
        'description': '본업에서 실제로 들어온 현금. 양수여야 건전.',
        'location': '현금흐름표',
        'category': '현금흐름'
    },
    '투자활동현금흐름': {
        'name': '투자활동 현금흐름 (CFI)',
        'description': '설비투자, 자산 매입/매각 등. 보통 음수.',
        'location': '현금흐름표',
        'category': '현금흐름'
    },
    '재무활동현금흐름': {
        'name': '재무활동 현금흐름 (CFF)',
        'description': '차입금, 배당금 등 자금 조달/상환.',
        'location': '현금흐름표',
        'category': '현금흐름'
    },
}


def get_indicator_description(indicator_code: str) -> dict:
    """지표 설명 조회"""
    if indicator_code in INVESTMENT_INDICATORS:
        return INVESTMENT_INDICATORS[indicator_code]
    elif indicator_code in MACRO_INDICATORS:
        return MACRO_INDICATORS[indicator_code]
    elif indicator_code in FINANCIAL_ACCOUNTS:
        return FINANCIAL_ACCOUNTS[indicator_code]
    else:
        return {
            'name': indicator_code,
            'description': '설명 없음',
            'interpretation': '',
            'category': ''
        }


def get_all_descriptions() -> dict:
    """모든 설명 합치기"""
    return {
        **INVESTMENT_INDICATORS,
        **MACRO_INDICATORS,
        **FINANCIAL_ACCOUNTS
    }
