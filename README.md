<<<<<<< HEAD
# 상계백병원 인수증 & 엑셀 데이터 비교 시스템

## 개요
PDF 인수증과 엑셀 데이터를 비교하여 불일치 항목을 찾고 관리하는 시스템입니다.

## 주요 기능
- PDF OCR 처리 및 텍스트 추출
- 엑셀 데이터와 PDF 데이터 비교
- 불일치 항목 관리 및 완료 처리
- 부서별 통계 및 분석
- S3 클라우드 스토리지 연동

## Streamlit Cloud 배포 방법

### 1. 사전 준비
- GitHub 계정
- Streamlit Cloud 계정
- AWS S3 버킷 및 액세스 키

### 2. 배포 단계

1. **GitHub 저장소 생성 및 코드 업로드**
   ```bash
   git init
   git add .
   git commit -m "Initial commit"
   git branch -M main
   git remote add origin [your-github-repo-url]
   git push -u origin main
   ```

2. **Streamlit Cloud 배포**
   - https://share.streamlit.io 접속
   - "New app" 클릭
   - GitHub 저장소 선택
   - Main file path: `streamlit_app.py`
   - Python version: 3.10 권장

3. **환경 변수 설정**
   Streamlit Cloud 대시보드의 "Secrets" 섹션에서 다음 설정:
   ```toml
   [aws]
   AWS_ACCESS_KEY_ID = "your-access-key"
   AWS_SECRET_ACCESS_KEY = "your-secret-key"
   AWS_REGION = "ap-northeast-2"
   S3_BUCKET = "your-bucket-name"
   ```

### 3. 필요한 파일들
- `streamlit_app.py`: 메인 애플리케이션
- `requirements.txt`: Python 의존성
- `packages.txt`: 시스템 의존성
- `pdf3_module.py`: PDF 처리 모듈
- `data_analyzer.py`: 데이터 분석 모듈

### 4. 문제 해결
- 로그 확인: Streamlit Cloud 대시보드에서 "View logs"
- 의존성 문제: requirements.txt와 packages.txt 확인
- 환경 변수: Secrets 설정 확인

## 로컬 개발 환경 설정

1. **의존성 설치**
   ```bash
   pip install -r requirements.txt
   ```

2. **환경 변수 설정**
   `.streamlit/secrets.toml` 파일 생성 후 AWS 설정 추가

3. **실행**
   ```bash
   streamlit run streamlit_app.py
   ```

## 사용 방법
1. 엑셀 파일과 PDF 파일 업로드
2. 작업 기간 설정
3. 각 탭에서 데이터 확인 및 처리
4. 완료 항목 관리

## 기술 스택
- Streamlit: 웹 애플리케이션 프레임워크
- pandas: 데이터 처리
- PyMuPDF: PDF 처리
- Pillow: 이미지 처리
- boto3: AWS S3 연동
- OpenCV: 이미지 전처리

## 주요 기능

1. **불일치 리스트 출력**: 
   - 청구량과 수령량이 불일치하는 항목을 리스트업
   - 부서별, 품목별 불일치 통계 차트 제공
   - CSV 다운로드 기능

2. **부서명 필터링**:
   - 부서명으로 불일치 항목 필터링
   - 선택된 부서의 품목별 차이 차트 제공

3. **PDF 관리**:
   - PDF에서 부서 정보 자동 추출
   - 특정 부서 관련 페이지 PDF 추출 및 저장
   - 추출된 PDF 미리보기 및 표시 기능

## 설치 방법

### 필수 요구사항
- Python 3.8 이상
- Poppler (PDF 변환에 필요)

### Windows에서 Poppler 설치
1. [Poppler for Windows](https://github.com/oschwartz10612/poppler-windows/releases/) 최신 버전 다운로드
2. 압축 해제 후 `C:\poppler-xx.xx.x\Library\bin` 경로 생성
3. `pdf3_module.py` 파일의 `POPPLER_PATH` 변수 경로 확인 및 필요시 수정

### 라이브러리 설치
```bash
pip install -r requirements.txt
```

선택 항목 완료 처리한 내역이 pdf 업로드, 엑셀 업로드 할때마다 초기화되는 문제 발생함
=======
# PDF OCR & 엑셀 데이터 비교 시스템

PDF OCR 처리 결과와 엑셀 데이터를 비교하는 웹 기반 대시보드 애플리케이션입니다.

## 주요 기능

1. **불일치 리스트 출력**: 
   - 청구량과 수령량이 불일치하는 항목을 리스트업
   - 부서별, 품목별 불일치 통계 차트 제공
   - CSV 다운로드 기능

2. **부서명 필터링**:
   - 부서명으로 불일치 항목 필터링
   - 선택된 부서의 품목별 차이 차트 제공

3. **PDF 관리**:
   - PDF에서 부서 정보 자동 추출
   - 특정 부서 관련 페이지 PDF 추출 및 저장
   - 추출된 PDF 미리보기 및 표시 기능

## 설치 방법

### 필수 요구사항
- Python 3.8 이상
- Poppler (PDF 변환에 필요)

### Windows에서 Poppler 설치
1. [Poppler for Windows](https://github.com/oschwartz10612/poppler-windows/releases/) 최신 버전 다운로드
2. 압축 해제 후 `C:\poppler-xx.xx.x\Library\bin` 경로 생성
3. `pdf3_module.py` 파일의 `POPPLER_PATH` 변수 경로 확인 및 필요시 수정

### 라이브러리 설치
```bash
pip install -r requirements.txt
```

## 사용 방법

1. **애플리케이션 실행**:
```bash
streamlit run streamlit_app.py
```

2. **파일 업로드**:
   - 사이드바에서 Excel 파일(.xlsx, .xls) 업로드
   - PDF 파일(.pdf) 업로드
   - "처리 시작" 버튼 클릭

3. **결과 확인**:
   - 불일치 리스트: 청구량/수령량 불일치 항목 확인
   - 부서별 필터링: 부서별 데이터 필터링 및 PDF 추출
   - PDF 결과: OCR 처리 결과 및 추출된 PDF 미리보기

## 프로젝트 구성

- `streamlit_app.py`: 메인 애플리케이션 파일
- `pdf3_module.py`: PDF OCR 처리 관련 기능 (pdf3.py 기반)
- `data_analyzer.py`: 엑셀 데이터 분석 및 비교 기능
- `requirements.txt`: 필요한 라이브러리 목록

## 주의사항

- OCR 처리는 인터넷 연결이 필요합니다 (네이버 클로바 OCR API 사용)
- 대용량 PDF 파일 처리 시 상당한 시간이 소요될 수 있습니다
- 엑셀 파일은 날짜, 부서명, 품목, 청구량, 수령량 컬럼을 포함해야 합니다 

선택 항목 완료 처리한 내역이 pdf 업로드, 엑셀 업로드 할때마다 초기화되는 문제 발생함
>>>>>>> 55ef71880d52a75d2d8f039bd7da6241528941f4
완료 품목끼리 다시 로그 json을 만들고, 기록하고, 불일치 데이터 불러올때마다 데이터 매칭해서 완료 처리된 품목은 빼고 데이터 테이블에 송출하려고 했는데 갑자기 s3에 새로 업로드된 pdf와 엑셀 자료를 못읽어오는 문제 발생함. 디버그 중에 작업 중단됨. backup 파일 생성함. 4월 30일 6시 50분