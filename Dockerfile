FROM apache/airflow:2.7.3-python3.10

# Chuyển sang user airflow (không dùng root để đảm bảo bảo mật)
USER airflow

# Copy file requirements vào container
COPY requirements.txt .

# Cài đặt các thư viện
RUN pip install --no-cache-dir -r requirements.txt

# Cài đặt sẵn DuckDB extension (cài cả httpfs và aws cho chắc chắn)
RUN python -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL httpfs;'); con.execute('INSTALL aws;');"
