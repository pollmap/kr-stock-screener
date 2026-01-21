-- CUFA Stock Screener v3.0 Database Schema
-- PostgreSQL + TimescaleDB

-- TimescaleDB 확장 활성화
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- =====================================================
-- 1. 종목 마스터 (GICS 분류)
-- =====================================================
CREATE TABLE IF NOT EXISTS stocks (
    code VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    name_en VARCHAR(100),
    market VARCHAR(20),                  -- KOSPI/KOSDAQ
    gics_sector VARCHAR(50),             -- GICS 섹터
    gics_industry_group VARCHAR(100),    -- GICS 산업그룹
    gics_industry VARCHAR(100),          -- GICS 산업
    gics_sub_industry VARCHAR(100),      -- GICS 하위산업
    corp_code VARCHAR(20),               -- DART 기업코드
    listed_at DATE,                      -- 상장일
    delisted_at DATE,                    -- 상장폐지일 (Survivorship Bias 방지)
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stocks_market ON stocks(market);
CREATE INDEX idx_stocks_sector ON stocks(gics_sector);
CREATE INDEX idx_stocks_active ON stocks(is_active);

-- =====================================================
-- 2. 기업 이벤트 (Corporate Actions)
-- =====================================================
CREATE TABLE IF NOT EXISTS corporate_actions (
    id SERIAL PRIMARY KEY,
    stock_code VARCHAR(10) REFERENCES stocks(code),
    action_date DATE NOT NULL,
    action_type VARCHAR(30) NOT NULL,    -- split, dividend, rights, merger
    ratio DECIMAL(10,4),                 -- 분할비율 (1:10 = 0.1)
    dividend_amount DECIMAL(10,2),       -- 배당금
    description TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_corporate_actions_stock ON corporate_actions(stock_code);
CREATE INDEX idx_corporate_actions_date ON corporate_actions(action_date);

-- =====================================================
-- 3. 재무제표 (Point-in-Time)
-- =====================================================
CREATE TABLE IF NOT EXISTS financials (
    id SERIAL PRIMARY KEY,
    stock_code VARCHAR(10) REFERENCES stocks(code),
    fiscal_year VARCHAR(10) NOT NULL,
    fiscal_quarter VARCHAR(5),           -- 1Q, 2Q, 3Q, 4Q, FY
    report_type VARCHAR(30),             -- 사업보고서/분기보고서
    fs_type VARCHAR(10),                 -- CFS/OFS (연결/별도)
    announced_at DATE,                   -- 공표일 (Point-in-Time 핵심)
    
    -- 손익계산서
    revenue DECIMAL(20,2),               -- 매출액
    cost_of_sales DECIMAL(20,2),         -- 매출원가
    gross_profit DECIMAL(20,2),          -- 매출총이익
    operating_income DECIMAL(20,2),      -- 영업이익
    ebit DECIMAL(20,2),                  -- 세전이익
    net_income DECIMAL(20,2),            -- 당기순이익
    ebitda DECIMAL(20,2),                -- EBITDA
    
    -- 재무상태표
    total_assets DECIMAL(20,2),          -- 자산총계
    total_liabilities DECIMAL(20,2),     -- 부채총계
    total_equity DECIMAL(20,2),          -- 자본총계
    current_assets DECIMAL(20,2),        -- 유동자산
    current_liabilities DECIMAL(20,2),   -- 유동부채
    cash DECIMAL(20,2),                  -- 현금성자산
    total_debt DECIMAL(20,2),            -- 총차입금
    retained_earnings DECIMAL(20,2),     -- 이익잉여금
    
    -- 현금흐름표
    ocf DECIMAL(20,2),                   -- 영업현금흐름
    icf DECIMAL(20,2),                   -- 투자현금흐름
    fcf DECIMAL(20,2),                   -- 잉여현금흐름
    capex DECIMAL(20,2),                 -- CAPEX
    
    collected_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(stock_code, fiscal_year, fiscal_quarter, fs_type)
);

CREATE INDEX idx_financials_stock ON financials(stock_code);
CREATE INDEX idx_financials_year ON financials(fiscal_year);
CREATE INDEX idx_financials_announced ON financials(announced_at);

-- =====================================================
-- 4. 일별 주가 (TimescaleDB Hypertable)
-- =====================================================
CREATE TABLE IF NOT EXISTS prices (
    time TIMESTAMPTZ NOT NULL,
    stock_code VARCHAR(10) NOT NULL,
    open DECIMAL(12,2),
    high DECIMAL(12,2),
    low DECIMAL(12,2),
    close DECIMAL(12,2),
    volume BIGINT,
    market_cap DECIMAL(20,2),
    shares BIGINT,
    adj_close DECIMAL(12,2),             -- 수정주가
    adj_factor DECIMAL(10,6) DEFAULT 1,  -- 수정계수
    
    PRIMARY KEY (time, stock_code)
);

-- TimescaleDB Hypertable 변환
SELECT create_hypertable('prices', 'time', if_not_exists => TRUE);

CREATE INDEX idx_prices_stock ON prices(stock_code, time DESC);

-- =====================================================
-- 5. 계산된 재무비율
-- =====================================================
CREATE TABLE IF NOT EXISTS ratios (
    id SERIAL PRIMARY KEY,
    stock_code VARCHAR(10) REFERENCES stocks(code),
    calc_date DATE NOT NULL,
    data_date DATE,                      -- 기준 재무데이터 날짜
    
    -- 수익성
    roe DECIMAL(8,2),
    roa DECIMAL(8,2),
    roic DECIMAL(8,2),
    gross_margin DECIMAL(8,2),
    operating_margin DECIMAL(8,2),
    net_margin DECIMAL(8,2),
    ebitda_margin DECIMAL(8,2),
    
    -- 안정성
    debt_ratio DECIMAL(8,2),
    equity_ratio DECIMAL(8,2),
    current_ratio DECIMAL(8,2),
    quick_ratio DECIMAL(8,2),
    interest_coverage DECIMAL(8,2),
    
    -- 성장성 (YoY)
    revenue_growth DECIMAL(8,2),
    op_income_growth DECIMAL(8,2),
    net_income_growth DECIMAL(8,2),
    
    -- 밸류에이션
    per DECIMAL(8,2),
    pbr DECIMAL(8,2),
    psr DECIMAL(8,2),
    pcr DECIMAL(8,2),
    ev_ebitda DECIMAL(8,2),
    
    -- 현금흐름
    fcf_yield DECIMAL(8,2),
    ocf_to_ni DECIMAL(8,2),
    
    -- 부도위험
    altman_z DECIMAL(8,4),
    z_grade VARCHAR(10),
    
    -- RIM 밸류에이션
    rim_value DECIMAL(20,2),
    rim_upside DECIMAL(8,2),
    
    created_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(stock_code, calc_date)
);

CREATE INDEX idx_ratios_stock ON ratios(stock_code);
CREATE INDEX idx_ratios_date ON ratios(calc_date);

-- =====================================================
-- 6. 컨센서스 (애널리스트 추정)
-- =====================================================
CREATE TABLE IF NOT EXISTS consensus (
    id SERIAL PRIMARY KEY,
    stock_code VARCHAR(10) REFERENCES stocks(code),
    target_year VARCHAR(10),
    collected_at DATE NOT NULL,
    
    eps_estimate DECIMAL(12,2),          -- 추정 EPS
    revenue_estimate DECIMAL(20,2),      -- 추정 매출
    target_price DECIMAL(12,2),          -- 목표주가
    target_price_avg DECIMAL(12,2),      -- 목표주가 평균
    target_price_high DECIMAL(12,2),     -- 목표주가 최고
    target_price_low DECIMAL(12,2),      -- 목표주가 최저
    analyst_count INT,                   -- 애널리스트 수
    recommendation VARCHAR(20),          -- buy/hold/sell
    
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_consensus_stock ON consensus(stock_code);

-- =====================================================
-- 7. 거시경제 지표
-- =====================================================
CREATE TABLE IF NOT EXISTS macro_indicators (
    id SERIAL PRIMARY KEY,
    source VARCHAR(20) NOT NULL,         -- BOK/FRED
    category VARCHAR(50),
    indicator VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    value DECIMAL(20,4),
    
    collected_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(source, indicator, date)
);

CREATE INDEX idx_macro_source ON macro_indicators(source);
CREATE INDEX idx_macro_date ON macro_indicators(date);

-- =====================================================
-- 8. 백테스팅 결과
-- =====================================================
CREATE TABLE IF NOT EXISTS backtest_results (
    id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(100) NOT NULL,
    start_date DATE,
    end_date DATE,
    
    total_return DECIMAL(8,2),
    annual_return DECIMAL(8,2),
    max_drawdown DECIMAL(8,2),
    sharpe_ratio DECIMAL(8,4),
    win_rate DECIMAL(8,2),
    
    params JSONB,                        -- 전략 파라미터
    holdings JSONB,                      -- 보유 종목 기록
    
    created_at TIMESTAMP DEFAULT NOW()
);

-- =====================================================
-- 9. GICS 섹터 분류 마스터
-- =====================================================
CREATE TABLE IF NOT EXISTS gics_sectors (
    code VARCHAR(10) PRIMARY KEY,
    level INT,                           -- 1=Sector, 2=IndustryGroup, 3=Industry, 4=SubIndustry
    parent_code VARCHAR(10),
    name_en VARCHAR(100),
    name_kr VARCHAR(100)
);

-- 초기 GICS 데이터 삽입
INSERT INTO gics_sectors (code, level, name_en, name_kr) VALUES
('10', 1, 'Energy', '에너지'),
('15', 1, 'Materials', '소재'),
('20', 1, 'Industrials', '산업재'),
('25', 1, 'Consumer Discretionary', '경기소비재'),
('30', 1, 'Consumer Staples', '필수소비재'),
('35', 1, 'Health Care', '헬스케어'),
('40', 1, 'Financials', '금융'),
('45', 1, 'Information Technology', 'IT'),
('50', 1, 'Communication Services', '커뮤니케이션'),
('55', 1, 'Utilities', '유틸리티'),
('60', 1, 'Real Estate', '부동산')
ON CONFLICT (code) DO NOTHING;

-- =====================================================
-- 업데이트 트리거
-- =====================================================
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER stocks_updated_at
    BEFORE UPDATE ON stocks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- 완료 메시지
DO $$
BEGIN
    RAISE NOTICE 'CUFA Stock Screener v3.0 Schema 초기화 완료';
END $$;
