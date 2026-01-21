"""
자동 의존성 체크 및 설치 모듈
- 패키지 버전 체크 및 자동 설치
- API 연결 상태 테스트
- 시스템 환경 진단
"""

import subprocess
import sys
import importlib
import os
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger("kr_stock_collector.setup")


# 필수 패키지 목록 (패키지명, 임포트명, 최소 버전)
REQUIRED_PACKAGES = [
    ("pandas", "pandas", "2.0.0"),
    ("numpy", "numpy", "1.24.0"),
    ("openpyxl", "openpyxl", "3.1.0"),
    ("requests", "requests", "2.31.0"),
    ("pyyaml", "yaml", "6.0"),
    ("finance-datareader", "FinanceDataReader", "0.9.50"),
    ("pykrx", "pykrx", "1.0.45"),
    ("tqdm", "tqdm", "4.65.0"),
    ("colorama", "colorama", "0.4.6"),
]

# 선택적 패키지 (없어도 동작하지만 권장)
OPTIONAL_PACKAGES = [
    ("opendartreader", "OpenDartReader", "0.2.0"),
    ("fredapi", "fredapi", "0.5.0"),
]


class SetupChecker:
    """
    시스템 설정 및 의존성 체크 클래스
    """
    
    def __init__(self, auto_install: bool = True):
        """
        Args:
            auto_install: 미설치 패키지 자동 설치 여부
        """
        self.auto_install = auto_install
        self.check_results: Dict[str, bool] = {}
        self.missing_packages: List[str] = []
        self.outdated_packages: List[Tuple[str, str, str]] = []  # (name, current, required)
    
    def _get_package_version(self, import_name: str) -> Optional[str]:
        """패키지 버전 조회"""
        try:
            module = importlib.import_module(import_name)
            return getattr(module, '__version__', 'unknown')
        except ImportError:
            return None
    
    def _compare_versions(self, current: str, required: str) -> bool:
        """버전 비교 (current >= required 이면 True)"""
        try:
            current_parts = [int(x) for x in current.split('.')[:3]]
            required_parts = [int(x) for x in required.split('.')[:3]]
            
            for c, r in zip(current_parts, required_parts):
                if c > r:
                    return True
                elif c < r:
                    return False
            return True
        except:
            return True  # 비교 실패 시 통과
    
    def _install_package(self, package_name: str) -> bool:
        """pip로 패키지 설치"""
        try:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", package_name, "--quiet"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            return True
        except subprocess.CalledProcessError:
            return False
    
    def check_packages(self, packages: List[Tuple[str, str, str]] = None) -> Dict[str, dict]:
        """
        패키지 설치 상태 체크
        
        Args:
            packages: [(pip_name, import_name, min_version), ...]
        
        Returns:
            {package_name: {'installed': bool, 'version': str, 'required': str, 'ok': bool}}
        """
        if packages is None:
            packages = REQUIRED_PACKAGES
        
        results = {}
        
        for pip_name, import_name, min_version in packages:
            current_version = self._get_package_version(import_name)
            
            if current_version is None:
                results[pip_name] = {
                    'installed': False,
                    'version': None,
                    'required': min_version,
                    'ok': False
                }
                self.missing_packages.append(pip_name)
            else:
                version_ok = self._compare_versions(current_version, min_version)
                results[pip_name] = {
                    'installed': True,
                    'version': current_version,
                    'required': min_version,
                    'ok': version_ok
                }
                if not version_ok:
                    self.outdated_packages.append((pip_name, current_version, min_version))
        
        return results
    
    def install_missing(self) -> Dict[str, bool]:
        """
        미설치 패키지 자동 설치
        
        Returns:
            {package_name: success}
        """
        results = {}
        
        for package in self.missing_packages:
            print(f"  📦 Installing {package}...", end=" ", flush=True)
            success = self._install_package(package)
            results[package] = success
            print("✓" if success else "✗")
        
        for package, current, required in self.outdated_packages:
            print(f"  📦 Upgrading {package} ({current} → {required})...", end=" ", flush=True)
            success = self._install_package(f"{package}>={required}")
            results[package] = success
            print("✓" if success else "✗")
        
        return results
    
    def check_api_connectivity(self, config: dict = None) -> Dict[str, dict]:
        """
        API 연결 상태 테스트
        
        Returns:
            {api_name: {'connected': bool, 'message': str, 'response_time': float}}
        """
        import requests
        import time
        
        results = {}
        
        # OpenDART
        try:
            start = time.time()
            resp = requests.get(
                "https://opendart.fss.or.kr/api/corpCode.xml",
                params={'crtfc_key': config.get('opendart', {}).get('api_key', 'test')},
                timeout=10
            )
            elapsed = time.time() - start
            results['OpenDART'] = {
                'connected': resp.status_code == 200,
                'message': 'OK' if resp.status_code == 200 else f'HTTP {resp.status_code}',
                'response_time': elapsed
            }
        except Exception as e:
            results['OpenDART'] = {'connected': False, 'message': str(e), 'response_time': 0}
        
        # 한국은행
        try:
            start = time.time()
            resp = requests.get(
                "https://ecos.bok.or.kr/api/StatisticSearch/"
                f"{config.get('bok', {}).get('api_key', 'test')}/json/kr/1/1/722Y001/M/202301/202301/0101000",
                timeout=10
            )
            elapsed = time.time() - start
            results['한국은행'] = {
                'connected': resp.status_code == 200,
                'message': 'OK' if resp.status_code == 200 else f'HTTP {resp.status_code}',
                'response_time': elapsed
            }
        except Exception as e:
            results['한국은행'] = {'connected': False, 'message': str(e), 'response_time': 0}
        
        # FRED
        try:
            start = time.time()
            resp = requests.get(
                "https://api.stlouisfed.org/fred/series",
                params={
                    'series_id': 'FEDFUNDS',
                    'api_key': config.get('fred', {}).get('api_key', 'test'),
                    'file_type': 'json'
                },
                timeout=10
            )
            elapsed = time.time() - start
            results['FRED'] = {
                'connected': resp.status_code == 200,
                'message': 'OK' if resp.status_code == 200 else f'HTTP {resp.status_code}',
                'response_time': elapsed
            }
        except Exception as e:
            results['FRED'] = {'connected': False, 'message': str(e), 'response_time': 0}
        
        # KRX (pykrx)
        try:
            start = time.time()
            resp = requests.get("http://data.krx.co.kr/", timeout=10)
            elapsed = time.time() - start
            results['KRX'] = {
                'connected': resp.status_code == 200,
                'message': 'OK' if resp.status_code == 200 else f'HTTP {resp.status_code}',
                'response_time': elapsed
            }
        except Exception as e:
            results['KRX'] = {'connected': False, 'message': str(e), 'response_time': 0}
        
        return results
    
    def check_directories(self, base_dir: str = ".") -> Dict[str, bool]:
        """
        필요 디렉토리 존재 여부 체크 및 생성
        """
        required_dirs = ['outputs', 'logs', 'cache', 'config']
        results = {}
        
        for dir_name in required_dirs:
            dir_path = os.path.join(base_dir, dir_name)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path, exist_ok=True)
                results[dir_name] = 'created'
            else:
                results[dir_name] = 'exists'
        
        return results
    
    def run_full_check(self, config: dict = None) -> dict:
        """
        전체 시스템 체크 실행
        """
        print("\n" + "=" * 60)
        print("🔍 시스템 환경 체크 시작")
        print("=" * 60)
        
        # 1. Python 버전
        py_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        py_ok = sys.version_info >= (3, 10)
        print(f"\n📌 Python 버전: {py_version} {'✓' if py_ok else '⚠️ (3.10+ 권장)'}")
        
        # 2. 필수 패키지
        print("\n📦 필수 패키지 체크...")
        pkg_results = self.check_packages(REQUIRED_PACKAGES)
        
        for name, info in pkg_results.items():
            if info['installed']:
                status = '✓' if info['ok'] else f"⚠️ ({info['version']} < {info['required']})"
                print(f"  {name}: {info['version']} {status}")
            else:
                print(f"  {name}: ❌ 미설치")
        
        # 3. 자동 설치
        if self.auto_install and (self.missing_packages or self.outdated_packages):
            print("\n📥 미설치/구버전 패키지 설치 중...")
            install_results = self.install_missing()
        else:
            install_results = {}
        
        # 4. 선택적 패키지
        print("\n📦 선택적 패키지 체크...")
        opt_results = self.check_packages(OPTIONAL_PACKAGES)
        for name, info in opt_results.items():
            if info['installed']:
                print(f"  {name}: {info['version']} ✓")
            else:
                print(f"  {name}: ⚠️ 미설치 (선택사항)")
        
        # 5. 디렉토리
        print("\n📁 디렉토리 체크...")
        dir_results = self.check_directories()
        for name, status in dir_results.items():
            print(f"  {name}/: {status}")
        
        # 6. API 연결 (config 있을 때만)
        api_results = {}
        if config:
            print("\n🌐 API 연결 테스트...")
            api_results = self.check_api_connectivity(config)
            for name, info in api_results.items():
                if info['connected']:
                    print(f"  {name}: ✓ ({info['response_time']:.2f}s)")
                else:
                    print(f"  {name}: ❌ ({info['message']})")
        
        print("\n" + "=" * 60)
        all_ok = all(r['ok'] for r in pkg_results.values())
        print(f"{'✅ 모든 체크 완료!' if all_ok else '⚠️ 일부 항목 확인 필요'}")
        print("=" * 60 + "\n")
        
        return {
            'python_version': py_version,
            'python_ok': py_ok,
            'packages': pkg_results,
            'optional_packages': opt_results,
            'install_results': install_results,
            'directories': dir_results,
            'api_connectivity': api_results,
            'all_ok': all_ok
        }


def ensure_dependencies(auto_install: bool = True, config: dict = None) -> bool:
    """
    의존성 체크 및 설치 (간편 함수)
    
    Args:
        auto_install: 자동 설치 여부
        config: API 키 설정 (연결 테스트용)
    
    Returns:
        모든 체크 통과 여부
    """
    checker = SetupChecker(auto_install=auto_install)
    result = checker.run_full_check(config)
    return result['all_ok']


if __name__ == "__main__":
    # 직접 실행 시 체크만
    ensure_dependencies(auto_install=True)
