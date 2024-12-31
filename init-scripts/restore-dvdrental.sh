#!/bin/bash
set -e

# Đợi PostgreSQL khởi động hoàn toàn (tối đa 60 giây)
for i in {1..30}; do
    if pg_isready -U airflow; then
        break
    fi
    echo "Waiting for PostgreSQL to start... ($i/30)"
    sleep 2
done

# Kiểm tra xem database đã tồn tại chưa
if psql -U airflow -lqt | cut -d \| -f 1 | grep -qw dvd_rental; then
    echo "Database dvd_rental already exists"
else
    echo "Creating database dvd_rental..."
    createdb -U airflow dvd_rental
fi

echo "Starting restore process..."
pg_restore -U airflow -d dvd_rental /data/dvd_rental.tar
echo "Restore completed!"