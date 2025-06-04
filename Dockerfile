# 빌드 스테이지
FROM python:3.12.2-alpine3.19.1 AS builder

WORKDIR /app

# 빌드 의존성 설치
RUN apk add --no-cache \
    gcc \
    musl-dev \
    python3-dev \
    libffi-dev \
    openssl-dev \
    cargo

# Python 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir wheel && \
    pip install --no-cache-dir -r requirements.txt

# 최종 스테이지
FROM python:3.12.2-alpine3.19.1

WORKDIR /app

# 런타임 의존성 설치
RUN apk add --no-cache \
    libstdc++ \
    poppler-utils \
    ttf-dejavu \
    fontconfig

# 빌더에서 설치된 패키지 복사
COPY --from=builder /usr/local/lib/python3.12/site-packages/ /usr/local/lib/python3.12/site-packages/

# 보안 설정
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_NO_CACHE_DIR=1

# Streamlit secrets 디렉토리 생성
RUN mkdir -p /app/.streamlit

# 애플리케이션 파일 복사
COPY . .

# 포트 설정
EXPOSE 8501

# 실행 명령
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]