# ============================
# RSS CRAWLER V8 - Dockerfile
# ============================

FROM python:3.11-slim

# Cài system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
 && rm -rf /var/lib/apt/lists/*

# Set thư mục làm việc
WORKDIR /app

# Copy requirements trước để cache layer
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ source
COPY . .

# Tạo user không phải root cho bảo mật
RUN useradd -m crawler
USER crawler

# Chạy crawler
CMD ["python", "crawler.py"]
