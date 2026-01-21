"""
SQLAlchemy 데이터베이스 모델 (v2.0)
- 종목, 재무제표, 주가, 거시경제 테이블
- 증분 업데이트 지원
"""

from sqlalchemy import (
    create_engine, Column, Integer, Float, String, Text, Date, DateTime,
    ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime
import os

Base = declarative_base()


class Stock(Base):
    """종목 마스터"""
    __tablename__ = 'stocks'
    
    code = Column(String(10), primary_key=True)  # 종목코드 (6자리)
    name = Column(String(100), nullable=False)   # 기업명
    market = Column(String(20))                  # KOSPI/KOSDAQ
    sector = Column(String(50))                  # 섹터 (WICS)
    industry = Column(String(100))               # 산업
    corp_code = Column(String(20))               # DART 기업코드
    listed_date = Column(Date)                   # 상장일
    is_active = Column(Integer, default=1)       # 활성 여부
    updated_at = Column(DateTime, default=datetime.now)
    
    # Relationships
    financials = relationship('Financial', back_populates='stock')
    prices = relationship('Price', back_populates='stock')
    ratios = relationship('Ratio', back_populates='stock')


class Financial(Base):
    """재무제표 (BS/IS/CF)"""
    __tablename__ = 'financials'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(10), ForeignKey('stocks.code'), nullable=False)
    fiscal_year = Column(String(10), nullable=False)  # 2024
    fiscal_quarter = Column(String(5))                # 1Q, 2Q, 3Q, 4Q, FY
    report_type = Column(String(20))                  # 사업보고서/분기보고서
    fs_type = Column(String(10))                      # CFS/OFS
    
    # 손익계산서
    revenue = Column(Float)                # 매출액
    cost_of_sales = Column(Float)          # 매출원가
    gross_profit = Column(Float)           # 매출총이익
    operating_income = Column(Float)       # 영업이익
    ebit = Column(Float)                   # 세전이익
    net_income = Column(Float)             # 당기순이익
    
    # 재무상태표
    total_assets = Column(Float)           # 자산총계
    total_liabilities = Column(Float)      # 부채총계
    total_equity = Column(Float)           # 자본총계
    current_assets = Column(Float)         # 유동자산
    current_liabilities = Column(Float)    # 유동부채
    cash = Column(Float)                   # 현금성자산
    total_debt = Column(Float)             # 총차입금
    
    # 현금흐름표
    ocf = Column(Float)                    # 영업현금흐름
    icf = Column(Float)                    # 투자현금흐름
    fcf = Column(Float)                    # 잉여현금흐름
    capex = Column(Float)                  # CAPEX
    
    collected_at = Column(DateTime, default=datetime.now)
    
    # Unique constraint
    __table_args__ = (
        UniqueConstraint('stock_code', 'fiscal_year', 'fiscal_quarter', 'fs_type'),
        Index('idx_financial_stock', 'stock_code'),
    )
    
    stock = relationship('Stock', back_populates='financials')


class Price(Base):
    """일별 주가"""
    __tablename__ = 'prices'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(10), ForeignKey('stocks.code'), nullable=False)
    date = Column(Date, nullable=False)
    
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    market_cap = Column(Float)             # 시가총액
    shares = Column(Integer)               # 상장주식수
    
    __table_args__ = (
        UniqueConstraint('stock_code', 'date'),
        Index('idx_price_stock_date', 'stock_code', 'date'),
    )
    
    stock = relationship('Stock', back_populates='prices')


class Ratio(Base):
    """계산된 재무비율"""
    __tablename__ = 'ratios'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(10), ForeignKey('stocks.code'), nullable=False)
    calc_date = Column(Date, nullable=False)  # 계산일
    
    # 수익성
    roe = Column(Float)
    roa = Column(Float)
    roic = Column(Float)
    gross_margin = Column(Float)
    operating_margin = Column(Float)
    net_margin = Column(Float)
    ebitda_margin = Column(Float)
    
    # 안정성
    debt_ratio = Column(Float)
    equity_ratio = Column(Float)
    current_ratio = Column(Float)
    interest_coverage = Column(Float)
    
    # 성장성
    revenue_growth = Column(Float)
    op_income_growth = Column(Float)
    net_income_growth = Column(Float)
    
    # 밸류에이션
    per = Column(Float)
    pbr = Column(Float)
    psr = Column(Float)
    pcr = Column(Float)
    ev_ebitda = Column(Float)
    
    # 현금흐름
    fcf_yield = Column(Float)
    ocf_to_ni = Column(Float)
    
    # 부도위험
    altman_z = Column(Float)
    z_grade = Column(String(10))
    
    __table_args__ = (
        UniqueConstraint('stock_code', 'calc_date'),
    )
    
    stock = relationship('Stock', back_populates='ratios')


class MacroIndicator(Base):
    """거시경제 지표"""
    __tablename__ = 'macro_indicators'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(20), nullable=False)   # BOK/FRED
    category = Column(String(50))                  # 금리/물가 등
    indicator = Column(String(100), nullable=False)
    date = Column(Date, nullable=False)
    value = Column(Float)
    
    collected_at = Column(DateTime, default=datetime.now)
    
    __table_args__ = (
        UniqueConstraint('source', 'indicator', 'date'),
        Index('idx_macro_source_date', 'source', 'date'),
    )


class BacktestResult(Base):
    """백테스팅 결과"""
    __tablename__ = 'backtest_results'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_name = Column(String(100), nullable=False)
    start_date = Column(Date)
    end_date = Column(Date)
    
    total_return = Column(Float)           # 총수익률
    annual_return = Column(Float)          # 연환산수익률
    max_drawdown = Column(Float)           # 최대낙폭
    sharpe_ratio = Column(Float)           # 샤프비율
    win_rate = Column(Float)               # 승률
    
    params = Column(Text)                  # JSON 파라미터
    created_at = Column(DateTime, default=datetime.now)


# 데이터베이스 초기화 함수
def init_db(db_path: str = 'database/screener.db'):
    """데이터베이스 및 테이블 생성"""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    engine = create_engine(f'sqlite:///{db_path}', echo=False)
    Base.metadata.create_all(engine)
    return engine


def get_session(engine):
    """세션 생성"""
    Session = sessionmaker(bind=engine)
    return Session()


if __name__ == '__main__':
    engine = init_db()
    print("✓ 데이터베이스 초기화 완료: database/screener.db")
