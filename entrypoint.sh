#!/bin/sh

# Keluar segera jika ada perintah yang gagal
set -e

trap 'echo "Error occurred, exiting..."; exit 1' ERR

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
    video_worker)
        echo "Starting Video worker..."
        # Hapus argumen pertama ('worker') dan jalankan sisanya
        shift
        # Jalankan consumer sebagai modul dengan sisa argumennya
        exec python -m baseapp.services._redis_worker.video_worker "$@"
        ;;
    postgresql_migrate)
        echo "Running PostgreSQL Migrations..."
        # 1. Jalankan Alembic untuk membuat tabel (Upgrade schema)
        exec alembic upgrade head
        
        # 2. (Opsional) Jalankan script seeding data awal jika Anda membuatnya
        # echo "Seeding initial data..."
        # python seed.py
        
        echo "Migration completed."
        ;;
    mongodb_migrate)
        echo "Running MongoDB Migrations..."
        # 1. Jalankan Alembic untuk membuat tabel (Upgrade schema)
        exec python manage.py upgrade head
        
        # 2. (Opsional) Jalankan script seeding data awal jika Anda membuatnya
        # echo "Seeding initial data..."
        # python seed.py
        
        echo "Migration completed."
        ;;
    init_storage)
        echo "Initializing Object Storage..."
        # Jalankan script python khusus Minio
        exec python -m baseapp.services.database.create_bucket
        ;;
    *)
        # Jalankan perintah apa pun yang diberikan
        exec "$@"
        ;;
esac