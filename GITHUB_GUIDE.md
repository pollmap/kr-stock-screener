# 🚀 GitHub 업로드 가이드

## 📋 준비물
- GitHub 계정 (없으면 https://github.com 에서 가입)
- Git 설치 (없으면 https://git-scm.com 에서 다운로드)

---

## 🔧 Step 1: GitHub 저장소 생성

1. https://github.com 로그인
2. 우측 상단 `+` → `New repository` 클릭
3. 설정:
   - Repository name: `kr-stock-screener`
   - Description: `국내 주식 재무데이터 수집 시스템`
   - Public 선택 (또는 Private)
   - ❌ "Add a README file" 체크 해제
4. `Create repository` 클릭

---

## 💻 Step 2: 로컬에서 Git 설정

터미널(PowerShell)에서:

```powershell
# 프로젝트 폴더로 이동
cd C:\Users\user1\.gemini\antigravity\scratch\stock-screener\kr_stock_collector

# Git 초기화
git init

# 사용자 설정 (처음 한 번만)
git config user.name "YOUR_NAME"
git config user.email "YOUR_EMAIL@example.com"

# 모든 파일 추가
git add .

# 첫 커밋
git commit -m "Initial commit: KR Stock Screener v1.0"

# 원격 저장소 연결 (YOUR_USERNAME을 본인 GitHub 아이디로 변경)
git remote add origin https://github.com/YOUR_USERNAME/kr-stock-screener.git

# 푸시
git branch -M main
git push -u origin main
```

---

## ⚠️ 중요: API 키 보호

API 키가 GitHub에 올라가면 안 됩니다!
`.gitignore` 파일이 이미 `config/api_keys.yaml`을 제외하도록 설정되어 있습니다.

### 확인 방법:
```powershell
cat .gitignore
```

`api_keys.yaml`이 포함되어 있는지 확인하세요.

---

## 📝 Step 3: API 키 템플릿 생성

다른 사람이 사용할 수 있도록 템플릿 파일 생성:

```powershell
# 템플릿 파일 생성
Copy-Item config/api_keys.yaml config/api_keys.example.yaml
```

`config/api_keys.example.yaml` 내용을 다음으로 수정:
```yaml
# API 키 설정 파일
# 이 파일을 api_keys.yaml로 복사한 후 본인의 키를 입력하세요

opendart:
  api_key: "YOUR_OPENDART_API_KEY_HERE"  # https://opendart.fss.or.kr
  
bok:
  api_key: "YOUR_BOK_API_KEY_HERE"  # https://ecos.bok.or.kr

fred:
  api_key: "YOUR_FRED_API_KEY_HERE"  # https://fred.stlouisfed.org
```

```powershell
# 템플릿 추가 및 푸시
git add config/api_keys.example.yaml
git commit -m "Add API keys template"
git push
```

---

## ✅ 완료!

이제 GitHub에서 저장소를 확인하세요:
`https://github.com/YOUR_USERNAME/kr-stock-screener`

---

## 📢 동아리 친구들에게 공유하기

친구들에게 보낼 메시지:

```
🏦 국내 주식 스크리너를 만들었어!

GitHub: https://github.com/YOUR_USERNAME/kr-stock-screener

사용법:
1. 저장소 클론: git clone https://github.com/YOUR_USERNAME/kr-stock-screener.git
2. cd kr-stock-screener/kr_stock_collector
3. pip install -r requirements.txt
4. config/api_keys.example.yaml을 api_keys.yaml로 복사하고 API 키 입력
5. python main.py --quick (테스트)
6. outputs 폴더의 엑셀 파일 열기!

엑셀 파일에 사용법 가이드가 다 있어 👍
```

---

## 🔄 업데이트 방법

코드를 수정한 후:

```powershell
git add .
git commit -m "Update: 변경 내용 설명"
git push
```
