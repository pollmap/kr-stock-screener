"""
데이터베이스 저장소 (Repository Pattern)
- CRUD 작업 추상화
- 증분 업데이트 지원
"""

from sqlalchemy.orm import Session
from sqlalchemy import and_, func
from datetime import datetime, date
from typing import List, Optional, Dict
import pandas as pd
import logging

from .models import Stock, Financial, Price, Ratio, MacroIndicator, init_db, get_session

logger = logging.getLogger("kr_stock_collector.repository")


class StockRepository:
    """종목 저장소"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def upsert(self, code: str, name: str, market: str = None, 
               sector: str = None, industry: str = None, corp_code: str = None) -> Stock:
        """종목 추가/업데이트"""
        stock = self.session.query(Stock).filter_by(code=code).first()
        
        if stock:
            stock.name = name
            if market: stock.market = market
            if sector: stock.sector = sector
            if industry: stock.industry = industry
            if corp_code: stock.corp_code = corp_code
            stock.updated_at = datetime.now()
        else:
            stock = Stock(
                code=code, name=name, market=market,
                sector=sector, industry=industry, corp_code=corp_code
            )
            self.session.add(stock)
        
        return stock
    
    def bulk_upsert(self, df: pd.DataFrame) -> int:
        """대량 종목 추가/업데이트"""
        count = 0
        for _, row in df.iterrows():
            self.upsert(
                code=str(row.get('Code', '')).zfill(6),
                name=row.get('Name', ''),
                market=row.get('Market', ''),
                sector=row.get('Sector', ''),
                industry=row.get('Industry', '')
            )
            count += 1
        self.session.commit()
        return count
    
    def get_all_codes(self) -> List[str]:
        """모든 종목코드 조회"""
        return [s.code for s in self.session.query(Stock.code).filter_by(is_active=1).all()]
    
    def get_by_code(self, code: str) -> Optional[Stock]:
        """종목 조회"""
        return self.session.query(Stock).filter_by(code=code).first()


class FinancialRepository:
    """재무제표 저장소"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def exists(self, stock_code: str, fiscal_year: str, fiscal_quarter: str = 'FY') -> bool:
        """존재 여부 확인 (증분 업데이트용)"""
        result = self.session.query(Financial).filter(
            and_(
                Financial.stock_code == stock_code,
                Financial.fiscal_year == fiscal_year,
                Financial.fiscal_quarter == fiscal_quarter
            )
        ).first()
        return result is not None
    
    def upsert(self, data: Dict) -> Financial:
        """재무제표 추가/업데이트"""
        existing = self.session.query(Financial).filter(
            and_(
                Financial.stock_code == data['stock_code'],
                Financial.fiscal_year == data['fiscal_year'],
                Financial.fiscal_quarter == data.get('fiscal_quarter', 'FY')
            )
        ).first()
        
        if existing:
            for key, value in data.items():
                if hasattr(existing, key):
                    setattr(existing, key, value)
            existing.collected_at = datetime.now()
            return existing
        else:
            financial = Financial(**data)
            self.session.add(financial)
            return financial
    
    def get_latest(self, stock_code: str) -> Optional[Financial]:
        """최신 재무제표 조회"""
        return self.session.query(Financial).filter_by(
            stock_code=stock_code
        ).order_by(Financial.fiscal_year.desc()).first()
    
    def get_ttm(self, stock_code: str) -> Dict[str, float]:
        """TTM (Trailing 12 Months) 계산"""
        # 최근 4개 분기 데이터 조회
        quarters = self.session.query(Financial).filter(
            and_(
                Financial.stock_code == stock_code,
                Financial.fiscal_quarter.in_(['1Q', '2Q', '3Q', '4Q'])
            )
        ).order_by(Financial.fiscal_year.desc(), Financial.fiscal_quarter.desc()).limit(4).all()
        
        if len(quarters) < 4:
            return {}
        
        ttm = {
            'revenue': sum(q.revenue or 0 for q in quarters),
            'operating_income': sum(q.operating_income or 0 for q in quarters),
            'net_income': sum(q.net_income or 0 for q in quarters),
            'ocf': sum(q.ocf or 0 for q in quarters),
            'fcf': sum(q.fcf or 0 for q in quarters),
        }
        return ttm


class PriceRepository:
    """주가 저장소"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def get_last_date(self, stock_code: str) -> Optional[date]:
        """마지막 수집일 조회 (증분 업데이트용)"""
        result = self.session.query(func.max(Price.date)).filter_by(
            stock_code=stock_code
        ).scalar()
        return result
    
    def bulk_insert(self, stock_code: str, df: pd.DataFrame) -> int:
        """대량 주가 삽입"""
        count = 0
        for _, row in df.iterrows():
            price = Price(
                stock_code=stock_code,
                date=row.get('Date') or row.name,
                open=row.get('Open'),
                high=row.get('High'),
                low=row.get('Low'),
                close=row.get('Close'),
                volume=row.get('Volume'),
                market_cap=row.get('MarketCap')
            )
            self.session.add(price)
            count += 1
        self.session.commit()
        return count
    
    def get_latest(self, stock_code: str) -> Optional[Price]:
        """최신 주가 조회"""
        return self.session.query(Price).filter_by(
            stock_code=stock_code
        ).order_by(Price.date.desc()).first()


class MacroRepository:
    """거시경제 저장소"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def exists(self, source: str, indicator: str, ind_date: date) -> bool:
        """존재 여부 확인"""
        return self.session.query(MacroIndicator).filter(
            and_(
                MacroIndicator.source == source,
                MacroIndicator.indicator == indicator,
                MacroIndicator.date == ind_date
            )
        ).first() is not None
    
    def upsert(self, source: str, category: str, indicator: str, 
               ind_date: date, value: float) -> MacroIndicator:
        """거시지표 추가/업데이트"""
        existing = self.session.query(MacroIndicator).filter(
            and_(
                MacroIndicator.source == source,
                MacroIndicator.indicator == indicator,
                MacroIndicator.date == ind_date
            )
        ).first()
        
        if existing:
            existing.value = value
            existing.collected_at = datetime.now()
            return existing
        else:
            macro = MacroIndicator(
                source=source, category=category, indicator=indicator,
                date=ind_date, value=value
            )
            self.session.add(macro)
            return macro
    
    def get_latest_all(self) -> pd.DataFrame:
        """모든 지표의 최신값 조회"""
        subq = self.session.query(
            MacroIndicator.source,
            MacroIndicator.indicator,
            func.max(MacroIndicator.date).label('max_date')
        ).group_by(MacroIndicator.source, MacroIndicator.indicator).subquery()
        
        results = self.session.query(MacroIndicator).join(
            subq,
            and_(
                MacroIndicator.source == subq.c.source,
                MacroIndicator.indicator == subq.c.indicator,
                MacroIndicator.date == subq.c.max_date
            )
        ).all()
        
        if results:
            return pd.DataFrame([{
                'source': r.source,
                'category': r.category,
                'indicator': r.indicator,
                'date': r.date,
                'value': r.value
            } for r in results])
        return pd.DataFrame()


class DatabaseManager:
    """데이터베이스 매니저 (Facade)"""
    
    def __init__(self, db_path: str = 'database/screener.db'):
        self.engine = init_db(db_path)
        self.session = get_session(self.engine)
        
        self.stocks = StockRepository(self.session)
        self.financials = FinancialRepository(self.session)
        self.prices = PriceRepository(self.session)
        self.macro = MacroRepository(self.session)
    
    def commit(self):
        """커밋"""
        self.session.commit()
    
    def rollback(self):
        """롤백"""
        self.session.rollback()
    
    def close(self):
        """세션 종료"""
        self.session.close()


if __name__ == '__main__':
    # 테스트
    db = DatabaseManager()
    print("✓ DatabaseManager 초기화 완료")
    db.close()
