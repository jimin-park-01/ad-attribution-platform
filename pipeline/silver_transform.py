"""
Bronze raw zone (3 zone) -> processed_events Iceberg (Silver layer).

Bronze layer는 event-type 별 plain parquet 3 zone으로 쌓여 있다
(impressions / clicks / conversions). Silver layer는 event_id 기준으로
join해서 한 행으로 조립하고, Iceberg MERGE INTO로 Late Event를 반영한다.

  bronze/impressions/ + bronze/clicks/ + bronze/conversions/
    -> processed_events (Iceberg, Silver)

- impression이 base. click / conversion은 left-join으로 합류한다.
- conversion이 늦게 도착(=bronze/conversions/에 새 파일이 추가)하면
  다음 MERGE 실행 시 conversion=1로 WHEN MATCHED THEN UPDATE가 발화된다.

로컬 실행 예:
    docker compose exec spark \\
      /usr/local/spark/bin/spark-submit \\
        /home/jovyan/pipeline/silver_transform.py \\
        --catalog-mode local \\
        --catalog-name local \\
        --warehouse /home/jovyan/warehouse

S3 + Glue Catalog 실행 예:
    docker compose exec spark \\
      /usr/local/spark/bin/spark-submit \\
        /home/jovyan/pipeline/silver_transform.py \\
        --catalog-mode glue \\
        --catalog-name glue_catalog \\
        --bucket my-s3-bucket \\
        --prefix ad-attribution \\
        --warehouse s3://my-s3-bucket/warehouse
"""

import argparse

# Silver는 MERGE INTO를 사용하므로
# Iceberg Extension이 포함된 SparkSession 생성
# local(개발) / glue(운영) 환경 모두 지원

def build_spark(app_name, catalog_mode, catalog_name, warehouse):
    """Iceberg 카탈로그를 포함한 SparkSession을 생성한다."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    )

    if catalog_mode == "glue":
        # Glue Catalog: 메타데이터는 AWS Glue, 데이터는 S3FileIO
        builder = (
            builder
            .config(
                f"spark.sql.catalog.{catalog_name}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
            )
            .config(
                f"spark.sql.catalog.{catalog_name}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
        )
    else:
        # 로컬 Hadoop 카탈로그: warehouse 경로에 메타데이터 + 데이터 함께 저장
        builder = (
            builder
            .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
        )

    return builder.getOrCreate()


def ensure_processed_table(spark, target_table, catalog_name, database):
    """
    processed_events Iceberg 테이블이 없으면 생성한다.

    format-version=2: Row-level delete 지원 (MERGE INTO에 필요)
    copy-on-write: 단순 배치 워크로드에서 read 성능 유리
    event_date 파티션: Silver 재실행 시 날짜별로 처리 범위 한정 가능
    """

    # Silver 결과 저장용 Iceberg 테이블 생성
    # 최초 실행 시에만 CREATE TABLE 수행

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{database}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            event_id            STRING,
            event_date          DATE,
            uid                 STRING,
            campaign            INT,
            click               INT,
            conversion          INT,
            conversion_delay_sec BIGINT,
            cost                DOUBLE,
            updated_at          TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES (
            'format-version'              = '2',
            'write.update.mode'           = 'copy-on-write',
            'write.merge.mode'            = 'copy-on-write',
            'write.delete.mode'           = 'copy-on-write',
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )

# Bronze Raw Zone 읽기

def read_zone(spark, path, raw_format):
    """Bronze raw zone(plain parquet)을 DataFrame으로 읽는다."""
    return spark.read.format(raw_format).load(path)

# Bronze 3개 Zone을 통합하여
# Silver 표준 이벤트 테이블 생성

def transform_raw(impression_df, click_df, conversion_df):
    """
    3 zone을 event_id로 join해 Silver row를 조립한다.

    각 zone은 Structured Streaming 재처리 등으로 같은 event_id가 중복 적재될 수
    있으므로, event_id 기준 최신 ingest_ts 행만 source-of-truth로 쓴다.
    """
    from pyspark.sql.functions import (
        coalesce,
        col,
        current_timestamp,
        lit,
        row_number,
        to_date,
    )
    from pyspark.sql.window import Window


    # 동일 event_id 중 가장 최근 수집본만 유지
    # Bronze 재처리나 스트리밍 재시작 시 발생할 수 있는 중복 제거

    def latest_per_event(df):
        # event_timestamp가 아닌 ingest_ts로 최신을 선택하는 이유:
        # Kafka 재처리나 스트리밍 재시작으로 동일 event_id가 중복 수신될 때
        # event_timestamp는 원본 이벤트 시각이라 중복 레코드 간 동일하다.
        # ingest_ts(수집 시각)가 가장 큰 것이 가장 최근에 수집된 정확한 사본이다.
        win = Window.partitionBy("event_id").orderBy(col("ingest_ts").desc_nulls_last())
        return df.withColumn("_rn", row_number().over(win)).filter(col("_rn") == 1).drop("_rn")


    # Silver Base Table
    # impression 기준으로 나머지 이벤트 결합

    imp = latest_per_event(impression_df).select(
        col("event_id"),
        col("event_timestamp").alias("imp_event_ts"),
        col("uid"),
        col("campaign").cast("int").alias("campaign"),
        col("cost").cast("double").alias("cost"),
    )


    # 클릭 발생 여부만 저장
    # 클릭이 존재하면 click_flag = 1

    clk = latest_per_event(click_df).select(
        col("event_id"),
        lit(1).alias("click_flag"),
    )

    # 전환 발생 여부 및 전환 지연 시간 저장
    # Late Event 반영 대상

    cnv = latest_per_event(conversion_df).select(
        col("event_id"),
        lit(1).alias("conversion_flag"),
        col("conversion_delay_sec").cast("bigint").alias("conversion_delay_sec"),
    )

    # impression을 base로, click / conversion을 left-join
    # conversion이 아직 도착하지 않은 event_id는 conversion=0으로 INSERT됨


    joined = imp.join(clk, "event_id", "left").join(cnv, "event_id", "left")

    # Silver 표준 스키마 생성
    # Gold 집계의 입력 데이터

    return joined.select(
        col("event_id"),
        to_date(col("imp_event_ts")).alias("event_date"),
        col("uid"),
        col("campaign"),
        coalesce(col("click_flag"), lit(0)).cast("int").alias("click"),
        coalesce(col("conversion_flag"), lit(0)).cast("int").alias("conversion"),
        col("conversion_delay_sec"),
        col("cost"),
        current_timestamp().alias("updated_at"),
    )

# 전체 테이블 재생성
# 운영보다는 초기 적재용

def full_refresh(spark, transformed_df, target_table):
    """Silver 전체를 재생성한다. 운영보다는 초기 적재나 테스트에 사용."""
    transformed_df.writeTo(target_table).overwritePartitions()


# Late Event 반영
# 기존 event_id는 UPDATE
# 신규 event_id는 INSERT

def merge_recent(spark, transformed_df, target_table, merge_window_days):
    """
    최근 N일 데이터만 MERGE INTO한다.

    Iceberg MERGE는 전체 테이블 스캔 비용이 높다.
    실무적으로 Late Event는 최대 7일 이내 도착하므로 그 범위만 처리해도 정합성이 확보된다.
    광고 전환 지연이 보통 72시간 이내이나, 주말 포함 여유를 더해 기본값을 7일로 설정했다.

    Late Event 처리 핵심:
      WHEN MATCHED: conversion이 늦게 도착하면 기존 event_id 행을 UPDATE
      WHEN NOT MATCHED: 신규 event_id는 INSERT
    """

    # Late Event가 발생할 수 있는 최근 N일만 MERGE
    # 전체 테이블 스캔 비용 감소

    filtered = transformed_df.filter(
        f"event_date >= current_date() - INTERVAL {merge_window_days} DAYS"
    )
    filtered.createOrReplaceTempView("source_processed_events")

    spark.sql(
        f"""
        MERGE INTO {target_table} t
        USING source_processed_events s
        ON t.event_id = s.event_id
        WHEN MATCHED THEN
          UPDATE SET
            t.event_date             = s.event_date,
            t.uid                    = s.uid,
            t.campaign               = s.campaign,
            t.click                  = s.click,
            t.conversion             = s.conversion,
            t.conversion_delay_sec   = s.conversion_delay_sec,
            t.cost                   = s.cost,
            t.updated_at             = s.updated_at
        WHEN NOT MATCHED THEN
          INSERT (
            event_id, event_date, uid, campaign,
            click, conversion, conversion_delay_sec,
            cost, updated_at
          )
          VALUES (
            s.event_id, s.event_date, s.uid, s.campaign,
            s.click, s.conversion, s.conversion_delay_sec,
            s.cost, s.updated_at
          )
        """
    )

# Bronze Zone 위치 결정
def resolve_paths(args):
    """
    --bucket / --warehouse / --prefix 조합에서 Bronze 3 zone 경로를 반환한다.
    """
    if args.bucket:
        base = f"s3a://{args.bucket}/{args.prefix}/bronze"
    else:
        base = f"{args.warehouse}/bronze"

    return (
        f"{base}/impressions",
        f"{base}/clicks",
        f"{base}/conversions",
    )

# 전체 실행 흐름 제어
# Spark 생성 → Bronze 읽기 → Join → Silver 생성 → MERGE
def main():
    parser = argparse.ArgumentParser(
        description="Bronze 3 zone -> processed_events Iceberg (Silver, event_id join + MERGE)"
    )

    # 카탈로그 설정
    parser.add_argument(
        "--catalog-mode",
        choices=["local", "glue"],
        default="local",
        help="Iceberg 카탈로그 종류 (기본: local)",
    )
    parser.add_argument("--catalog-name", default="local")
    parser.add_argument(
        "--warehouse",
        default="/home/jovyan/warehouse",
        help="Iceberg warehouse 경로 (로컬 기본: /home/jovyan/warehouse)",
    )
    parser.add_argument("--database", default="ad_lakehouse")
    parser.add_argument("--table", default="processed_events")

    # Bronze 입력 경로: bucket(S3) 또는 warehouse(로컬) 중 하나
    parser.add_argument(
        "--bucket",
        default=None,
        help="S3 버킷명. 지정 시 bronze 경로를 s3a://bucket/prefix/bronze/ 로 자동 구성.",
    )
    parser.add_argument(
        "--prefix",
        default="ad-attribution",
        help="S3 경로 prefix (기본: ad-attribution)",
    )

    parser.add_argument(
        "--raw-format",
        choices=["parquet", "json"],
        default="parquet",
    )
    parser.add_argument(
        "--mode",
        choices=["merge", "full-refresh"],
        default="merge",
        help="merge: 최근 N일 MERGE INTO (기본) / full-refresh: 전량 덮어쓰기",
    )
    parser.add_argument(
        "--merge-window-days",
        type=int,
        default=7,
        help="MERGE 대상 최근 N일 (기본: 7)",
    )

    args = parser.parse_args()

    # Glue 모드에서 warehouse는 S3 경로여야 함
    warehouse = args.warehouse
    if args.catalog_mode == "glue" and args.bucket and args.warehouse == "/home/jovyan/warehouse":
        warehouse = f"s3a://{args.bucket}/warehouse"

    spark = build_spark(
        "SilverTransform",
        args.catalog_mode,
        args.catalog_name,
        warehouse,
    )

    # Silver 결과 저장 대상 Iceberg 테이블
    target_table = f"{args.catalog_name}.{args.database}.{args.table}"
    ensure_processed_table(spark, target_table, args.catalog_name, args.database)

    impression_path, click_path, conversion_path = resolve_paths(args)

    impression_df = read_zone(spark, impression_path, args.raw_format)
    click_df = read_zone(spark, click_path, args.raw_format)
    conversion_df = read_zone(spark, conversion_path, args.raw_format)

    processed_df = transform_raw(impression_df, click_df, conversion_df)

    if args.mode == "full-refresh":
        full_refresh(spark, processed_df, target_table)
    else:
        merge_recent(spark, processed_df, target_table, args.merge_window_days)

    print(f"target table:      {target_table}")
    print(f"impression source: {impression_path}")
    print(f"click source:      {click_path}")
    print(f"conversion source: {conversion_path}")
    print(f"mode:              {args.mode}")


if __name__ == "__main__":
    main()
