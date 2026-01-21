"""
CUFA 주식 스크리너 웹 대시보드 (Streamlit)
- 실시간 스크리닝 필터
- 재무비율 시각화
- DCF 계산기
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
import os

# 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 페이지 설정
st.set_page_config(
    page_title="CUFA Stock Screener",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS 스타일
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(90deg, #1f4e79, #4472c4);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 1rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
    }
    .stDataFrame {
        font-size: 12px;
    }
</style>
""", unsafe_allow_html=True)


def load_sample_data():
    """샘플 데이터 로드 (실제로는 DB에서)"""
    return pd.DataFrame({
        '종목코드': ['005930', '000660', '035420', '035720', '051910'],
        '기업명': ['삼성전자', 'SK하이닉스', 'NAVER', '카카오', 'LG화학'],
        '섹터': ['IT', 'IT', 'IT', 'IT', '소재'],
        '시가총액': [350000000, 80000000, 45000000, 25000000, 30000000],
        'PER': [12.5, 8.2, 35.0, 42.0, 15.0],
        'PBR': [1.3, 1.1, 2.5, 3.2, 1.0],
        'ROE': [15.2, 22.1, 12.0, 8.5, 10.5],
        'ROA': [8.5, 12.0, 8.0, 5.0, 6.0],
        '부채비율': [35, 45, 30, 40, 80],
        '영업이익률': [18.0, 25.0, 15.0, 12.0, 8.0],
        '매출성장률': [5.0, 15.0, 10.0, -5.0, 8.0]
    })


def main():
    # 헤더
    st.markdown('<p class="main-header">📊 CUFA Stock Screener</p>', unsafe_allow_html=True)
    st.caption(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')} | Made by 이찬희 (금은동 8기)")
    
    # 사이드바 - 필터
    with st.sidebar:
        st.header("🎯 스크리닝 필터")
        
        # 시장 선택
        market = st.multiselect(
            "시장",
            ["KOSPI", "KOSDAQ"],
            default=["KOSPI", "KOSDAQ"]
        )
        
        # 섹터 선택
        sectors = st.multiselect(
            "섹터",
            ["IT", "소재", "금융", "헬스케어", "산업재", "경기소비재"],
            default=[]
        )
        
        st.divider()
        st.subheader("📈 밸류에이션")
        
        per_range = st.slider("PER", 0.0, 50.0, (0.0, 20.0))
        pbr_range = st.slider("PBR", 0.0, 5.0, (0.0, 2.0))
        
        st.divider()
        st.subheader("💪 수익성")
        
        min_roe = st.slider("최소 ROE (%)", 0, 30, 10)
        min_roa = st.slider("최소 ROA (%)", 0, 20, 5)
        
        st.divider()
        st.subheader("🛡️ 안정성")
        
        max_debt = st.slider("최대 부채비율 (%)", 0, 200, 100)
        
        # 전략 프리셋
        st.divider()
        strategy = st.selectbox(
            "📚 전략 프리셋",
            ["직접 설정", "그레이엄 (가치)", "버핏 (퀄리티)", "린치 (성장)"]
        )
    
    # 데이터 로드
    df = load_sample_data()
    
    # 필터 적용
    filtered = df.copy()
    
    if sectors:
        filtered = filtered[filtered['섹터'].isin(sectors)]
    
    filtered = filtered[
        (filtered['PER'] >= per_range[0]) & (filtered['PER'] <= per_range[1]) &
        (filtered['PBR'] >= pbr_range[0]) & (filtered['PBR'] <= pbr_range[1]) &
        (filtered['ROE'] >= min_roe) &
        (filtered['ROA'] >= min_roa) &
        (filtered['부채비율'] <= max_debt)
    ]
    
    # 메인 컨텐츠
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📋 전체 종목", f"{len(df)}개")
    with col2:
        st.metric("✅ 통과 종목", f"{len(filtered)}개")
    with col3:
        st.metric("📈 평균 PER", f"{filtered['PER'].mean():.1f}" if len(filtered) > 0 else "-")
    with col4:
        st.metric("💰 평균 ROE", f"{filtered['ROE'].mean():.1f}%" if len(filtered) > 0 else "-")
    
    st.divider()
    
    # 탭
    tab1, tab2, tab3 = st.tabs(["📋 종목 리스트", "📊 차트 분석", "💰 DCF 계산기"])
    
    with tab1:
        st.subheader("🏆 스크리닝 결과")
        
        if len(filtered) > 0:
            # 정렬 옵션
            sort_col = st.selectbox("정렬 기준", ["ROE", "PER", "PBR", "시가총액"])
            ascending = st.checkbox("오름차순", value=(sort_col == "PER"))
            
            display_df = filtered.sort_values(sort_col, ascending=ascending)
            
            # 시총 포맷팅
            display_df['시가총액'] = display_df['시가총액'].apply(
                lambda x: f"{x/100000000:.0f}억"
            )
            
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True
            )
            
            # 다운로드
            csv = filtered.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                "📥 CSV 다운로드",
                csv,
                "screened_stocks.csv",
                "text/csv"
            )
        else:
            st.warning("조건에 맞는 종목이 없습니다.")
    
    with tab2:
        st.subheader("📊 밸류에이션 분포")
        
        if len(filtered) > 0:
            col1, col2 = st.columns(2)
            
            with col1:
                # PER vs ROE 산점도
                fig = px.scatter(
                    filtered,
                    x='PER', y='ROE',
                    size='시가총액',
                    color='섹터',
                    hover_name='기업명',
                    title='PER vs ROE',
                    template='plotly_white'
                )
                fig.add_hline(y=15, line_dash="dash", line_color="green", annotation_text="ROE 15%")
                fig.add_vline(x=15, line_dash="dash", line_color="red", annotation_text="PER 15")
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # 섹터별 PER 분포
                fig = px.box(
                    df,
                    x='섹터', y='PER',
                    title='섹터별 PER 분포',
                    template='plotly_white'
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("데이터를 선택해주세요.")
    
    with tab3:
        st.subheader("💰 DCF 내재가치 계산기")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**📝 입력값**")
            fcf = st.number_input("현재 FCF (억원)", value=1000, step=100)
            growth1 = st.slider("1~5년 성장률 (%)", 0, 30, 10) / 100
            growth2 = st.slider("6~10년 성장률 (%)", 0, 20, 5) / 100
            wacc = st.slider("WACC (%)", 5, 15, 10) / 100
            terminal_g = st.slider("영구성장률 (%)", 0, 5, 2) / 100
            net_debt = st.number_input("순부채 (억원)", value=0, step=100)
            shares = st.number_input("발행주식수 (만주)", value=10000, step=1000) * 10000
        
        with col2:
            st.markdown("**📊 계산 결과**")
            
            if st.button("🧮 계산하기", type="primary"):
                try:
                    from analyzers import DCFCalculator
                    calc = DCFCalculator(wacc=wacc, terminal_growth=terminal_g)
                    result = calc.calculate_fair_value(
                        fcf, net_debt, shares,
                        growth_phase1=growth1, growth_phase2=growth2
                    )
                    
                    fair_value = result.get('fair_value', 0)
                    ev = result.get('enterprise_value', 0)
                    
                    st.success(f"**적정주가: {fair_value:,.0f}원**")
                    st.info(f"기업가치(EV): {ev:,.0f}억원")
                    
                    # 민감도 분석
                    sensitivity = calc.sensitivity_analysis(fcf, net_debt, shares)
                    st.markdown("**민감도 분석 (WACC × 영구성장률)**")
                    st.dataframe(sensitivity.style.format("{:,.0f}"))
                    
                except Exception as e:
                    st.error(f"계산 오류: {e}")
    
    # 푸터
    st.divider()
    st.caption("© 2026 CUFA 충북대학교 가치투자학회 | 이찬희 (금은동 8기)")


if __name__ == "__main__":
    main()
