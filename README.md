# 🏦 KR Stock Screener - 국내 주식 재무데이터 수집 시스템

> CUFA 가치투자 동아리를 위한 전문가급 종목 스크리닝 도구

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 📋 목차
- [소개](#-소개)
- [주요 기능](#-주요-기능)
- [빠른 시작](#-빠른-시작)
- [사용 방법](#-사용-방법)
- [엑셀 결과물 활용](#-엑셀-결과물-활용)
- [API 키 발급](#-api-키-발급)
- [문제 해결](#-문제-해결)

---

## 🎯 소개

이 프로그램은 **국내 상장 전 종목(KOSPI/KOSDAQ/KONEX)**의 재무데이터를 자동으로 수집하여 **분석하기 쉬운 엑셀 파일**로 출력합니다.

### 이런 분들께 추천합니다
- 📊 가치투자를 위해 저평가 종목을 찾고 싶은 분
- 📈 재무지표 기반으로 종목을 스크리닝하고 싶은 분
- 🔍 2,500개 전 종목을 한눈에 비교하고 싶은 분

---

## ✨ 주요 기능

| 기능 | 설명 |
|------|------|
| 📑 **재무제표** | 재무상태표, 손익계산서, 현금흐름표 (3년치) |
| 📊 **투자지표 60개+** | ROE, PER, PBR, ROIC, Altman Z-Score 등 |
| 🇰🇷 **한국경제 30개+** | 기준금리, CPI, 환율, 수출입, 실업률 등 |
| 🌍 **글로벌경제 50개+** | 미국금리, VIX, 원자재, 달러인덱스 등 |
| 📝 **초보자 가이드** | 엑셀에 지표 설명 및 해석 방법 포함 |
| ⏱️ **진행상황 표시** | 실시간 진행률과 예상 소요 시간 |

---

## 🚀 빠른 시작

### 1단계: 저장소 클론
```bash
git clone https://github.com/YOUR_USERNAME/kr-stock-screener.git
cd kr-stock-screener/kr_stock_collector
```

### 2단계: 의존성 설치
```bash
pip install -r requirements.txt
```

### 3단계: API 키 설정
`config/api_keys.yaml` 파일을 열어 본인의 API 키 입력:
```yaml
opendart:
  api_key: "YOUR_OPENDART_API_KEY"
bok:
  api_key: "YOUR_BOK_API_KEY"
fred:
  api_key: "YOUR_FRED_API_KEY"
```

### 4단계: 실행
```bash
# 빠른 테스트 (10종목만)
python main.py --quick

# 전체 수집
python main.py
```

---

## 📖 사용 방법

### 기본 명령어

```bash
# 전체 수집 (KOSPI + KOSDAQ, 3년)
python main.py

# 특정 시장만
python main.py --market KOSPI

# 선택적 수집 (재무제표 + 투자지표만)
python main.py --select financial,indicators

# 거시경제만
python main.py --select macro

# 대화형 메뉴
python main.py --interactive

# 의존성 체크
python main.py --check-deps
```

### 선택 가능한 항목 (`--select`)

| 항목 | 설명 |
|------|------|
| `financial` | 재무제표 (재무상태표, 손익계산서) |
| `indicators` | 투자지표 60개+ (ROE, PER, PBR 등) |
| `market` | 시장데이터 (주가, 거래량, 시가총액) |
| `macro_kr` | 한국경제 (금리, 물가, 환율) |
| `macro_global` | 글로벌경제 (미국금리, VIX, 원자재) |
| `macro` | 거시경제 전체 |
| `all` | 모든 데이터 (기본값) |

---

## 📊 엑셀 결과물 활용

### 파일 위치
`outputs/stock_screener_YYYYMMDD_HHMMSS.xlsx`

### 시트 구성

| 시트 | 용도 |
|------|------|
| 📚 사용가이드 | 초보자용 완전 가이드 |
| 📊 Summary | 수집 요약 정보 |
| 📋 종목리스트 | 전 종목 (시총, 주식수 포함) |
| 📈 투자지표 | 60개+ 지표 (주석 포함) |
| 📑 재무제표 | 3년치 재무데이터 |
| 🌍 거시경제 | 80개+ 경제지표 |
| 📖 지표설명 | 모든 지표 해석법 |

### 🔍 종목 스크리닝 방법

1. **엑셀 열기** → `📈 투자지표` 시트 선택
2. **필터 활성화** (이미 적용되어 있음)
3. **조건 설정** 예시:
   - PER < 10 (저평가)
   - ROE > 15% (고수익)
   - 부채비율 < 100% (안전)

### 💡 추천 스크리닝 전략

| 전략 | 조건 |
|------|------|
| **저평가주** | PER < 10, PBR < 1 |
| **우량주** | ROE > 15%, 부채비율 < 50% |
| **배당주** | 배당수익률 > 3%, 배당성향 < 60% |
| **성장주** | 매출성장률 > 20%, 영업이익성장률 > 20% |
| **안전주** | Altman Z > 3, 이자보상배율 > 5 |

### 📝 지표 해석 팁

헤더에 **마우스를 올리면** 해당 지표의 설명이 표시됩니다:
- 📖 설명: 지표의 의미
- 💡 해석: 좋고 나쁨의 기준
- ⚠️ 주의: 함정에 빠지지 않는 법

---

## 🔑 API 키 발급

### OpenDART (무료)
1. https://opendart.fss.or.kr 접속
2. 회원가입 → 인증키 신청
3. 일일 10,000건 제한

### 한국은행 (무료)
1. https://ecos.bok.or.kr 접속
2. 개발자센터 → API 신청

### FRED (무료)
1. https://fred.stlouisfed.org 접속
2. My Account → API Keys

---

## ❓ 문제 해결

### Q: "API 키를 찾을 수 없습니다" 오류
→ `config/api_keys.yaml` 파일에 키가 정확히 입력되었는지 확인

### Q: "종목 리스트 조회 실패" 오류
→ 인터넷 연결 확인, 주말/공휴일에는 데이터 없을 수 있음

### Q: OpenDART 수집 중 멈춤
→ 일일 10,000건 제한 초과. 다음 날 다시 시도하거나 `--market KOSPI`로 범위 축소

### Q: 특정 종목 데이터 없음
→ 해당 종목이 재무제표를 공시하지 않았거나, 상장폐지된 경우

---

## 📁 프로젝트 구조

```
kr_stock_collector/
├── main.py              # 메인 실행 파일
├── requirements.txt     # 의존성
├── config/
│   ├── api_keys.yaml    # API 키 (⚠️ 비공개)
│   └── settings.yaml    # 설정
├── collectors/          # 데이터 수집
├── processors/          # 데이터 처리
├── exporters/           # 엑셀 출력
├── utils/               # 유틸리티
├── outputs/             # 결과 파일
├── logs/                # 로그
└── cache/               # 캐시
```

---

## 📜 라이선스

MIT License - 자유롭게 사용, 수정, 배포 가능

---

## 👥 기여자

- CUFA 가치투자 동아리

---

**Made with ❤️ for Korean Value Investors**
