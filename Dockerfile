# --- Tahap 1: Builder ---
# Di sini kita install semua dependensi, termasuk yang untuk kompilasi
FROM python:3.12.3-alpine AS builder

# Install build dependencies yang dibutuhkan
RUN apk add --no-cache \
    build-base \
    gcc \
    g++ \
    musl-dev \
    python3-dev \
    libffi-dev \
    openssl-dev

WORKDIR /app

# Copy dan install requirements terlebih dahulu untuk caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Tahap 2: Final ---
# Image akhir yang bersih dan kecil
FROM python:3.12.3-alpine

WORKDIR /app

RUN apk add --no-cache libmagic ffmpeg

# Buat user non-root untuk keamanan
RUN addgroup -S app && adduser -S -G app app

# Copy hanya library yang sudah terinstall dari tahap builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy kode aplikasi Anda
COPY . .

# BUAT FILE ENTRYPOINT BISA DIEKSEKUSI
RUN chmod +x /app/entrypoint.sh

# Berikan kepemilikan kepada user 'app'
RUN chown -R app:app /app

# Ganti ke user 'app'
USER app

# Jalankan entrypoint script
ENTRYPOINT ["/app/entrypoint.sh"]

# Perintah default jika tidak ada yang ditentukan
CMD ["api"]