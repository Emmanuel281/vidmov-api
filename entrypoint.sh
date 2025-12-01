#!/bin/sh

# Keluar segera jika ada perintah yang gagal
set -e

# Cek argumen pertama yang diberikan
case "$1" in
    api)
        echo "Starting API server..."
        # Ganti main:app dengan nama file dan instance FastAPI Anda
        exec python main.py
        ;;
    rabbit_worker)
        echo "Starting RabbitMQ worker..."
        # Hapus argumen pertama ('worker') dan jalankan sisanya
        shift
        # Jalankan consumer sebagai modul dengan sisa argumennya
        exec python -m baseapp.services.consumer "$@"
        ;;
    redis_worker)
        echo "Starting Redis worker..."
        # Hapus argumen pertama ('worker') dan jalankan sisanya
        shift
        # Jalankan consumer sebagai modul dengan sisa argumennya
        exec python -m baseapp.services.redis_manager "$@"
        ;;
    migrate)
        echo "Running Database Migrations..."
        # 1. Jalankan Alembic untuk membuat tabel (Upgrade schema)
        alembic upgrade head
        
        # 2. (Opsional) Jalankan script seeding data awal jika Anda membuatnya
        # echo "Seeding initial data..."
        # python seed.py
        
        echo "Migration completed."
        ;;
    *)
        # Jalankan perintah apa pun yang diberikan
        exec "$@"
        ;;
esac