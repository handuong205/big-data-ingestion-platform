"""
========================================================================================
DỰ ÁN: BDT301 — Big Data Assignment
MODULE: serving.py (JDBC writer dùng chung cho Job 2a và Job 2b)
MỤC ĐÍCH:
  - Gom logic ghi PostgreSQL về một chỗ để Job 2a (TV3) và Job 2b (TV2) dùng giống nhau.
  - Dùng truncate=true + mode=overwrite: xoá sạch dữ liệu cũ nhưng GIỮ NGUYÊN
    DDL và index do postgres/init.sql tạo. Nếu dùng mode=overwrite mặc định,
    Spark JDBC sẽ DROP TABLE rồi CREATE lại, làm mất index và kiểu cột.
  - Bảng phải tồn tại trước (chạy postgres/init.sql). Nếu thiếu, báo lỗi rõ ràng.
========================================================================================
"""

import os
import sys


def _jdbc_settings():
    db_host = os.getenv("POSTGRES_HOST")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB")
    db_user = os.getenv("POSTGRES_USER")
    db_pass = os.getenv("POSTGRES_PASSWORD")

    missing = [
        name
        for name, value in (
            ("POSTGRES_HOST", db_host),
            ("POSTGRES_DB", db_name),
            ("POSTGRES_USER", db_user),
            ("POSTGRES_PASSWORD", db_pass),
        )
        if not value
    ]
    if missing:
        raise ValueError(
            "Thiếu các biến môi trường bắt buộc của PostgreSQL: " + ", ".join(missing)
        )

    return f"jdbc:postgresql://{db_host}:{db_port}/{db_name}", db_user, db_pass


def write_to_postgres(df, table_name):
    """
    Ghi DataFrame vào PostgreSQL theo kiểu idempotent.

    Yêu cầu: bảng `table_name` đã được tạo bởi postgres/init.sql với cột khớp
    tên và kiểu dữ liệu của DataFrame.
    """
    jdbc_url, db_user, db_pass = _jdbc_settings()

    try:
        print(f"[*] Đang ghi bảng '{table_name}' vào PostgreSQL...")
        (
            df.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
            .option("user", db_user)
            .option("password", db_pass)
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )
        print(f"  [OK] Đã ghi bảng '{table_name}'.")
    except Exception as exc:
        raise RuntimeError(
            f"Không ghi được bảng '{table_name}' vào PostgreSQL. "
            f"Kiểm tra bảng đã được tạo bởi postgres/init.sql chưa "
            f"(nếu volume postgres đã tồn tại, chạy tay file init.sql), "
            f"và kiểu cột của DataFrame có khớp DDL không. Chi tiết: {exc}"
        ) from exc


def write_metrics_to_postgres(spark, metrics):
    """
    Ghi danh sách metric (metric_name, metric_value, detail) vào bảng dq_report.

    Ghi một lần duy nhất để tránh truncate nhiều lần.
    """
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("metric_name", StringType(), False),
            StructField("metric_value", DoubleType(), True),
            StructField("detail", StringType(), True),
        ]
    )
    rows = [
        (name, float(value) if value is not None else None, detail)
        for name, value, detail in metrics
    ]
    write_to_postgres(spark.createDataFrame(rows, schema), "dq_report")
