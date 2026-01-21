"""
통합 로깅 시스템
- 파일 및 콘솔 동시 출력
- 레벨별 포매팅
- 일별 로그 파일 생성
"""

import logging
import os
from datetime import datetime
from typing import Optional


def setup_logger(
    name: str = "kr_stock_collector",
    log_dir: str = "logs",
    level: int = logging.INFO
) -> logging.Logger:
    """
    로거 설정 및 반환
    
    Args:
        name: 로거 이름
        log_dir: 로그 파일 저장 디렉토리
        level: 로깅 레벨
    
    Returns:
        설정된 Logger 객체
    """
    # 로그 디렉토리 생성
    os.makedirs(log_dir, exist_ok=True)
    
    # 로거 생성
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 이미 핸들러가 있으면 추가하지 않음
    if logger.handlers:
        return logger
    
    # 포맷 설정
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 파일 핸들러 (일별 로그)
    today = datetime.now().strftime('%Y%m%d')
    log_file = os.path.join(log_dir, f'collector_{today}.log')
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    기존 로거 반환 (없으면 생성)
    
    Args:
        name: 로거 이름 (None이면 루트 로거)
    
    Returns:
        Logger 객체
    """
    if name:
        return logging.getLogger(f"kr_stock_collector.{name}")
    return logging.getLogger("kr_stock_collector")
