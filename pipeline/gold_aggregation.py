"""
processed_events (Silver) -> campaign_daily_summary Iceberg (Gold layer).

Silver의 processed_events는 Iceberg MERGE INTO로 Late Conversion을 반영하기 때문에
동일 campaign+event_date 조합의 KPI가 시간에 따라 변한다.
Gold는 이 보정된 수치를 campaign × event_date 단위로 집계해 BI 대시보드용으로 제공한다.

Late Event 서사를 수치로 증명하기 위해 기본 CTR/CVR/CPA 외에
late_conversion_count, late_conversion_ratio, conversion delay 분포를 함께 집계한다.

로컬 실행 예:
    docker compose exec spark \\
      /usr/local/spark/bin/spark-submit \\
        /home/jovyan/pipeline/gold_aggregation.py \\
        --catalog-mode local \\
        --catalog-name local \\
        --warehouse /home/jovyan/warehouse

S3 + Glue Catalog 실행 예:
    docker compose exec spark \\
      /usr/local/spark/bin/spark-submit \\
        /home/jovyan/pipeline/gold_aggregation.py \\
        --catalog-mode glue \\
        --catalog-name glue_catalog \\
        --warehouse s3://my-s3-bucket/warehouse
"""
# ============================================================
# Gold Layer Aggregation Flow
# ============================================================
#
# [1] Spark Session 생성
#     - Iceberg Catalog 연결
#     - Local 또는 Glue Catalog 사용
#
# [2] Gold 테이블 존재 확인
#     - campaign_daily_summary 생성
#     - 없으면 Iceberg Table 생성
#
# [3] Silver(processed_events) 조회
#     - 최근 N일 데이터 조회 (merge_window_days)
#     - campaign × event_date 단위 집계
#
# [4] KPI 계산
#     - impressions
#     - clicks
#     - conversions
#     - CTR
#     - CVR
#     - CPA
#
# [5] Late Event 분석 지표 계산
#     - late_conversion_count
#     - late_conversion_ratio
#     - avg_conversion_delay_sec
#     - p50_conversion_delay_sec
#     - p95_conversion_delay_sec
#
# [6] Gold MERGE INTO 수행
#     - 기존 campaign + summary_date 존재
#         → UPDATE
#     - 신규 데이터
#         → INSERT
#
# [7] 데이터 품질 검증
#     - CTR 0~100
#     - CVR 0~100
#     - 이상 데이터 존재 여부 확인
#
# [8] Snapshot 정리 (선택)
#     - 오래된 Iceberg Snapshot 삭제
#     - 메타데이터 및 스토리지 관리
#
# [9] Gold Layer 완료
#     - Athena 조회 가능
#     - Superset Dashboard 반영
#
# ============================================================


import argparse
from datetime import datetime, timedelta, timezone

# Spark가 Iceberg 테이블을 읽고
# MERGE INTO를 수행할 수 있도록 설정

# spark 실행 환경 생성 
def build_spark(app_name, catalog_mode, catalog_name, warehouse):
    """Iceberg 카탈로그를 포함한 SparkSession을 생성한다."""
    from pyspark.sql import SparkSession

    # Iceberg 기능 사용 가능 
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    )

    # 카탈로그 연결 
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


# gold 테이블 생성
def ensure_summary_table(spark, target_table, catalog_name, database):
    """
    campaign_daily_summary Iceberg 테이블이 없으면 생성한다.

    Late Event 특화 컬럼:
    - late_conversion_count: conversion_delay_sec > 3600 (1시간) 인 전환 수
    - late_conversion_ratio: late 전환이 전체 전환에서 차지하는 비율 (%)
    - avg/p50/p95_conversion_delay_sec: 지연 시간 분포 — CVR 왜곡의 심각도를 정량화
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{database}")

    # 마케팅 KPI뿐 아니라
    # Late Event 영향 분석을 위한 컬럼 포함
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            summary_date              DATE,
            campaign                  INT,
            impressions               BIGINT,
            clicks                    BIGINT,
            conversions               BIGINT,
            total_cost                DOUBLE,
            ctr                       DOUBLE,
            cvr                       DOUBLE,
            cpa                       DOUBLE,
            late_conversion_count     BIGINT,
            late_conversion_ratio     DOUBLE,
            avg_conversion_delay_sec  DOUBLE,
            p50_conversion_delay_sec  DOUBLE,
            p95_conversion_delay_sec  DOUBLE,
            updated_at                TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (summary_date)
        TBLPROPERTIES (
            'format-version'    = '2',
            'write.update.mode' = 'copy-on-write',
            'write.merge.mode'  = 'copy-on-write',
            'write.delete.mode' = 'copy-on-write'
        )
        """
    )

# Gold는 단순 집계 테이블이 아니다.
#
# Silver에서 Late Conversion이 반영되면
# 과거 event_date KPI도 변경될 수 있다.
#
# 따라서 INSERT 전용 구조가 아니라
# MERGE 기반 재집계를 수행하여
# KPI를 최신 상태로 유지한다.

def merge_summary(spark, processed_table, summary_table, merge_window_days, late_threshold_sec):
    """
    Silver processed_events -> Gold campaign_daily_summary MERGE INTO.

    Silver에서 Late Conversion이 MERGE되면 Gold도 재실행으로 KPI를 재집계할 수 있다.
    WHEN MATCHED: 같은 campaign+summary_date 조합이 이미 있으면 UPDATE
    WHEN NOT MATCHED: 신규 조합은 INSERT

    late_threshold_sec: late conversion 판단 기준 (기본 3600초 = 1시간)
    """
    # Gold KPI가 마지막으로 계산된 시각
    # 데이터 신선도(Freshness) 확인용
    # Gold KPI가 마지막으로 계산된 시각
    # 데이터 신선도(Freshness) 확인용
    spark.sql(
        f"""
        MERGE INTO {summary_table} t
        USING (
          SELECT
            event_date                                                            AS summary_date,
            campaign,
            COUNT(*)                                                              AS impressions,
            SUM(click)                                                            AS clicks,
            SUM(conversion)                                                       AS conversions,
            SUM(cost)                                                             AS total_cost,
            SUM(
              CASE WHEN conversion = 1 AND conversion_delay_sec > {late_threshold_sec}
                   THEN 1 ELSE 0 END
            )                                                                     AS late_conversion_count,
            AVG(
              CASE WHEN conversion = 1 THEN CAST(conversion_delay_sec AS DOUBLE) END
            )                                                                     AS avg_conversion_delay_sec,
            -- percentile_approx: 정확한 백분위(전체 정렬 필요)는 대용량에서 비용이 높다.
            -- approx는 오차를 허용하는 대신 O(n) 처리로 p50/p95를 근사한다.
            percentile_approx(
              CASE WHEN conversion = 1 THEN conversion_delay_sec END, 0.5
            )                                                                     AS p50_conversion_delay_sec,
            percentile_approx(
              CASE WHEN conversion = 1 THEN conversion_delay_sec END, 0.95
            )                                                                     AS p95_conversion_delay_sec,
            current_timestamp()                                                   AS updated_at 
          FROM {processed_table}
          WHERE event_date >= current_date() - INTERVAL {merge_window_days} DAYS
          GROUP BY event_date, campaign
        ) s
        ON t.summary_date = s.summary_date AND t.campaign = s.campaign
        WHEN MATCHED THEN
          UPDATE SET
            t.impressions              = s.impressions,
            t.clicks                   = s.clicks,
            t.conversions              = s.conversions,
            t.total_cost               = s.total_cost,
            t.ctr                      = CASE WHEN s.impressions > 0
                                              THEN s.clicks * 100.0 / s.impressions
                                              ELSE 0 END,
            t.cvr                      = CASE WHEN s.clicks > 0
                                              THEN s.conversions * 100.0 / s.clicks
                                              ELSE 0 END,
            t.cpa                      = CASE WHEN s.conversions > 0
                                              THEN s.total_cost / s.conversions
                                              ELSE NULL END,
            t.late_conversion_count    = s.late_conversion_count,
            t.late_conversion_ratio    = CASE WHEN s.conversions > 0
                                              THEN s.late_conversion_count * 100.0 / s.conversions
                                              ELSE 0 END,
            t.avg_conversion_delay_sec = s.avg_conversion_delay_sec,
            t.p50_conversion_delay_sec = s.p50_conversion_delay_sec,
            t.p95_conversion_delay_sec = s.p95_conversion_delay_sec,
            t.updated_at               = s.updated_at
        WHEN NOT MATCHED THEN
          INSERT (
            summary_date,
            campaign,
            impressions,
            clicks,
            conversions,
            total_cost,
            ctr,
            cvr,
            cpa,
            late_conversion_count,
            late_conversion_ratio,
            avg_conversion_delay_sec,
            p50_conversion_delay_sec,
            p95_conversion_delay_sec,
            updated_at
          )
          VALUES (
            s.summary_date,
            s.campaign,
            s.impressions,
            s.clicks,
            s.conversions,
            s.total_cost,
            CASE WHEN s.impressions > 0 THEN s.clicks * 100.0 / s.impressions ELSE 0 END,
            CASE WHEN s.clicks > 0 THEN s.conversions * 100.0 / s.clicks ELSE 0 END,
            CASE WHEN s.conversions > 0 THEN s.total_cost / s.conversions ELSE NULL END,
            s.late_conversion_count,
            CASE WHEN s.conversions > 0
                 THEN s.late_conversion_count * 100.0 / s.conversions
                 ELSE 0 END,
            s.avg_conversion_delay_sec,
            s.p50_conversion_delay_sec,
            s.p95_conversion_delay_sec,
            s.updated_at
          )
        """
    )

# KPI 계산 오류 탐지
def assert_quality(spark, summary_table):
    """
    기본 데이터 품질 체크. 위반 건이 있으면 예외를 발생시킨다.

    CTR/CVR는 비율(%)이므로 0~100 범위를 벗어나면 계산 오류를 의심해야 한다.
    """
    violations = spark.sql(
        f"""
        SELECT COUNT(*) AS cnt
        FROM {summary_table}
        WHERE impressions <= 0
           OR ctr < 0 OR ctr > 100
           OR cvr < 0 OR cvr > 100
        """
    ).collect()[0]["cnt"]

    if violations > 0:
        raise ValueError(
            f"데이터 품질 위반 {violations}건: impressions <= 0 또는 CTR/CVR 범위 초과"
        )

# Iceberg 운영 관리
# Snapshot 누적 방지

def expire_snapshots(spark, summary_table, retain_last_n_days, retain_last=10):
    """
    오래된 Iceberg 스냅샷을 삭제해 스토리지 비용을 절감한다.

    Iceberg는 MERGE/INSERT마다 스냅샷을 남기므로 주기적으로 정리하지 않으면
    메타데이터 파일과 고아 데이터 파일이 누적된다.

    older_than은 Python에서 절대 시각으로 변환 후 TIMESTAMP 리터럴로 전달한다.
    Spark SQL은 '30 days ago' 같은 자연어 표현을 지원하지 않기 때문이다.
    """
    older_than = (
        datetime.now(timezone.utc) - timedelta(days=retain_last_n_days)
    ).strftime("%Y-%m-%d %H:%M:%S")

    spark.sql(
        f"""
        CALL system.expire_snapshots(
          table       => '{summary_table}',
          older_than  => TIMESTAMP '{older_than}',
          retain_last => {retain_last}
        )
        """
    ).show(truncate=False)


def main():
    parser = argparse.ArgumentParser(
        description="processed_events (Silver) -> campaign_daily_summary Iceberg (Gold)"
    )

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
    parser.add_argument("--processed-table", default="processed_events")
    parser.add_argument("--summary-table", default="campaign_daily_summary")
    parser.add_argument(
        "--merge-window-days",
        type=int,
        default=7,
        help="MERGE 대상 최근 N일 (기본: 7)",
    )
    # 최근 N일만 재집계

    # Late Event는 과거 데이터를 수정할 수 있으므로
    # 전체 테이블 Full Scan 대신
    # 최근 N일 범위만 MERGE 대상으로 사용

    # 성능 최적화 목적
    
    parser.add_argument(
        "--late-threshold-sec",
        type=int,
        default=3600,

        # Late Conversion 판단 기준
        #
        # 광고 도메인에서는 전환이 즉시 발생하지 않을 수 있다.
        #
        # 본 프로젝트는 데모 목적으로
        # 1시간 이상 지연된 전환을 Late Event로 정의하였다.
        #
        # 실제 운영 환경에서는
        # 24시간 또는 7일 등
        # 비즈니스 정책에 따라 조정 가능하다.


        help="Late Conversion 판단 기준 초 (기본: 3600 = 1시간)",
    )
    parser.add_argument(
        "--expire-snapshots-days",
        type=int,
        default=None,
        help="이 일수보다 오래된 Iceberg 스냅샷 삭제 (기본: 삭제 안 함)",
    )
    parser.add_argument(
        "--skip-quality-check",
        action="store_true",
        help="데이터 품질 체크 건너뜀",
    )

    args = parser.parse_args()

    warehouse = args.warehouse

    spark = build_spark(
        "GoldAggregation",
        args.catalog_mode,
        args.catalog_name,
        warehouse,
    )

    processed_table = f"{args.catalog_name}.{args.database}.{args.processed_table}"
    summary_table = f"{args.catalog_name}.{args.database}.{args.summary_table}"

    ensure_summary_table(spark, summary_table, args.catalog_name, args.database)
    merge_summary(
        spark,
        processed_table,
        summary_table,
        args.merge_window_days,
        args.late_threshold_sec,
    )

    if not args.skip_quality_check:
        assert_quality(spark, summary_table)

    if args.expire_snapshots_days is not None:
        expire_snapshots(spark, summary_table, args.expire_snapshots_days)

    print(f"processed source: {processed_table}")
    print(f"summary target:   {summary_table}")
    print(f"merge window:     {args.merge_window_days} days")
    print(f"late threshold:   {args.late_threshold_sec} sec")


if __name__ == "__main__":
    main()
