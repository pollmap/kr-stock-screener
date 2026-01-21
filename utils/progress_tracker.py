"""
진행상황 표시 모듈
- 상세 진행률 표시
- 예상 소요 시간 계산
- 단계별 상태 표시
"""

import time
import sys
from typing import Optional, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import deque
import threading


@dataclass
class TaskInfo:
    """작업 정보"""
    name: str
    total: int
    current: int = 0
    start_time: float = field(default_factory=time.time)
    status: str = "대기"
    
    @property
    def progress(self) -> float:
        """진행률 (0~1)"""
        return self.current / self.total if self.total > 0 else 0
    
    @property
    def elapsed(self) -> float:
        """경과 시간 (초)"""
        return time.time() - self.start_time
    
    @property
    def eta(self) -> Optional[float]:
        """예상 남은 시간 (초)"""
        if self.current == 0:
            return None
        rate = self.current / self.elapsed
        remaining = self.total - self.current
        return remaining / rate if rate > 0 else None


class ProgressTracker:
    """
    진행상황 추적 및 표시 클래스
    
    시각적 진행률 바와 ETA 표시
    """
    
    def __init__(self, total_steps: int = 7, show_eta: bool = True):
        """
        Args:
            total_steps: 전체 단계 수
            show_eta: 예상 시간 표시 여부
        """
        self.total_steps = total_steps
        self.current_step = 0
        self.show_eta = show_eta
        
        self.start_time = time.time()
        self.step_times: deque = deque(maxlen=10)  # 최근 단계별 소요 시간
        
        self.current_task: Optional[TaskInfo] = None
        self.completed_tasks: list = []
        
        self._lock = threading.Lock()
    
    def _format_time(self, seconds: Optional[float]) -> str:
        """시간 포맷팅 (HH:MM:SS)"""
        if seconds is None or seconds < 0:
            return "--:--"
        
        hours, remainder = divmod(int(seconds), 3600)
        minutes, secs = divmod(remainder, 60)
        
        if hours > 0:
            return f"{hours}:{minutes:02d}:{secs:02d}"
        else:
            return f"{minutes:02d}:{secs:02d}"
    
    def _progress_bar(self, progress: float, width: int = 30) -> str:
        """진행률 바 생성"""
        filled = int(width * progress)
        bar = "█" * filled + "░" * (width - filled)
        pct = progress * 100
        return f"[{bar}] {pct:5.1f}%"
    
    def start_step(self, step_name: str, total_items: int = 1) -> None:
        """새 단계 시작"""
        with self._lock:
            self.current_step += 1
            self.current_task = TaskInfo(
                name=step_name,
                total=total_items,
                status="진행중"
            )
            
            # 헤더 출력
            step_info = f"[{self.current_step}/{self.total_steps}]"
            print(f"\n{'='*60}")
            print(f"📍 {step_info} {step_name}")
            print(f"{'='*60}")
            
            # 예상 시간 계산
            if self.show_eta and self.step_times:
                avg_time = sum(self.step_times) / len(self.step_times)
                remaining_steps = self.total_steps - self.current_step + 1
                eta = avg_time * remaining_steps * (total_items / 100)  # 보정
                print(f"⏱️  예상 소요: {self._format_time(eta)}")
    
    def update(self, current: int = None, status: str = None) -> None:
        """진행 상황 업데이트"""
        if self.current_task is None:
            return
        
        with self._lock:
            if current is not None:
                self.current_task.current = current
            if status is not None:
                self.current_task.status = status
            
            task = self.current_task
            
            # 진행률 바
            bar = self._progress_bar(task.progress)
            
            # ETA
            eta_str = self._format_time(task.eta) if self.show_eta else ""
            
            # 상태 메시지
            status_str = f" | {task.status}" if task.status else ""
            
            # 출력 (같은 줄에서 업데이트)
            line = f"\r  {bar} ({task.current:,}/{task.total:,})"
            if eta_str:
                line += f" | 남은시간: {eta_str}"
            line += status_str
            
            sys.stdout.write(f"{line:<80}")
            sys.stdout.flush()
    
    def finish_step(self, message: str = None) -> None:
        """단계 완료"""
        if self.current_task is None:
            return
        
        with self._lock:
            elapsed = self.current_task.elapsed
            self.step_times.append(elapsed)
            
            self.completed_tasks.append({
                'name': self.current_task.name,
                'elapsed': elapsed,
                'items': self.current_task.total
            })
            
            # 완료 메시지
            print()  # 새 줄
            if message:
                print(f"  ✓ {message} ({self._format_time(elapsed)})")
            else:
                print(f"  ✓ 완료 ({self._format_time(elapsed)})")
            
            self.current_task = None
    
    def skip_step(self, step_name: str, reason: str = "건너뜀") -> None:
        """단계 건너뛰기"""
        with self._lock:
            self.current_step += 1
            print(f"\n⏭️  [{self.current_step}/{self.total_steps}] {step_name} - {reason}")
    
    def show_summary(self) -> None:
        """최종 요약 표시"""
        total_elapsed = time.time() - self.start_time
        
        print("\n" + "=" * 60)
        print("📊 수집 완료 요약")
        print("=" * 60)
        
        for task in self.completed_tasks:
            print(f"  • {task['name']}: {task['items']:,}건 ({self._format_time(task['elapsed'])})")
        
        print("-" * 60)
        print(f"  ⏱️  총 소요 시간: {self._format_time(total_elapsed)}")
        print("=" * 60)
    
    def estimate_total_time(
        self,
        stock_count: int,
        years: int = 5,
        include_dart: bool = True,
        include_macro: bool = True
    ) -> float:
        """
        전체 예상 소요 시간 계산
        
        기준:
        - 종목 리스트: ~3초
        - 재무제표: 종목당 0.5초 x 연도
        - 투자지표: ~5초
        - 주가: ~5초
        - 거시경제: ~30초
        """
        estimate = 5  # 기본
        
        if include_dart:
            estimate += stock_count * years * 0.1  # 다중API 기준
        
        estimate += 10  # 투자지표 + 주가
        
        if include_macro:
            estimate += 60  # 한국 + 글로벌
        
        estimate += 10  # 엑셀 저장
        
        return estimate
    
    def show_initial_estimate(
        self,
        stock_count: int,
        years: int,
        screening_summary: str
    ) -> None:
        """초기 예상 시간 표시"""
        eta = self.estimate_total_time(stock_count, years)
        
        print("\n" + "=" * 60)
        print("🏦 국내 주식 재무데이터 수집 시스템")
        print("=" * 60)
        print(f"📌 스크리닝: {screening_summary}")
        print(f"📊 대상 종목: {stock_count:,}개")
        print(f"📅 수집 연도: {years}년치")
        print(f"⏱️  예상 소요 시간: {self._format_time(eta)}")
        print("=" * 60)


def create_progress_callback(tracker: ProgressTracker, total: int) -> Callable:
    """진행률 콜백 함수 생성"""
    def callback(current: int, status: str = None):
        tracker.update(current, status)
    return callback
