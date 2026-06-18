# 전체 처리 흐름
#
# Kafka Topic
#
# ad-impressions
# ad-clicks
# ad-conversions
#
#   ↓
# Kafka Stream 수신
#
#   ↓
# JSON → Spark DataFrame 변환
#
#   ↓
# event-type 별 스키마 적용
#
#   ↓
# 이벤트 발생 시각(event_timestamp) 생성
#
#   ↓
# 수집 시각(ingest_ts) 생성
#
#   ↓
# raw_date / raw_hour 파티션 생성
#
#   ↓
# Append Only Parquet 저장
#
#   ↓
# Bronze Raw Zone
#
# impressions/
# clicks/
# conversions/
#
#   ↓
# Silver Layer 입력 데이터 생성
#
# 설계 목적
#
# - Kafka 이벤트 영구 보존
# - 원본 데이터 재처리 가능
# - Bronze는 최소 변환만 수행
# - Silver에서 event_id 기준 JOIN 가능
# - Append Only 구조 유지

"""
Spark Structured Streaming: Kafka -> Bronze raw zone (event-type 별 분리).

Bronze layer는 Iceberg를 쓰지 않는다. Kafka 메시지를 event-type 별로
append-only plain parquet로 적재하는 것이 전부다.

  --event-type impression  ->  ad-impressions  ->  {prefix}/bronze/impressions/
  --event-type click       ->  ad-clicks       ->  {prefix}/bronze/clicks/
  --event-type conversion  ->  ad-conversions  ->  {prefix}/bronze/conversions/

각 zone은 raw_date / raw_hour (ingest 시각) 기준으로 파티셔닝된다.
Silver layer (silver_transform.py) 가 event_id 기준으로 3 zone을 조립한다.

로컬 실행 예:
    docker compose exec spark \\
      /usr/local/spark/bin/spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \\
        /home/jovyan/pipeline/bronze_streaming.py \\
        --event-type impression \\
        --bootstrap-servers kafka:29092 \\
        --warehouse /home/jovyan/warehouse

S3 실행 예:
    docker compose exec spark \\
      /usr/local/spark/bin/spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \\
        /home/jovyan/pipeline/bronze_streaming.py \\
        --event-type impression \\
        --bootstrap-servers kafka:29092 \\
        --bucket my-s3-bucket \\
        --prefix ad-attribution
"""

import argparse

# event-type -> kafka 토픽 매핑 ( CLI 입력 단순화 )
EVENT_TYPE_DEFAULT_TOPIC = {
    "impression": "ad-impressions",
    "click": "ad-clicks",
    "conversion": "ad-conversions",
}

# Kafka JSON -> Spark Schema ( Spark는 JSON 구조를 알아야 함 )
def build_event_schema(event_type):
    """event-type별 Kafka payload JSON 스키마를 반환한다.

    impression / click / conversion은 각자 다른 비즈니스 의미를 가지므로 스키마가 다르다.
    - impression: cost 포함 (CPC 모델에서 노출 예약 시점에 비용이 확정된다)
    - click: impression_timestamp 포함 (어떤 노출에서 파생된 클릭인지 Silver JOIN 시 사용)
    - conversion: impression_timestamp + conversion_delay_sec 포함 (Late Event 지연 측정)
    단일 스키마에 nullable 필드로 합치면 event_type별 null 처리 로직이 복잡해진다.
    """
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    common = [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("timestamp", LongType()),
        StructField("event_time", StringType()),
        StructField("uid", StringType()),
        StructField("campaign", IntegerType()),
    ]

    if event_type == "impression":
        return StructType(common + [StructField("cost", DoubleType())])

    if event_type == "click":
        # click에는 cost 없고 impression_timestamp 추가
        return StructType(
            [common[0], common[1], common[2], common[3]]
            + [StructField("impression_timestamp", LongType())]
            + [common[4], common[5]]
        )

    if event_type == "conversion":
        # conversion에는 impression_timestamp + conversion_delay_sec 추가
        return StructType(
            [common[0], common[1], common[2], common[3]]
            + [StructField("impression_timestamp", LongType())]
            + [common[4], common[5]]
            + [StructField("conversion_delay_sec", LongType())]
        )

    raise ValueError(f"지원하지 않는 event-type: {event_type}")

# Bronze 전용 SparkSession 생성
# Bronze는 Append Only Parquet 저장만 수행하므로 Iceberg 설정이 필요 없다.
def build_spark(app_name):
    """Bronze 전용 SparkSession — Iceberg 설정 없음."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

# 저장 경로 계산
#
# 동일 코드로
# - Local Warehouse
# - AWS S3
#
# 두 환경을 모두 지원하기 위해 경로를 동적으로 생성한다.
def resolve_paths(args):
    """
    --bucket / --warehouse / --prefix 를 조합해서
    raw zone 경로와 checkpoint 경로를 반환한다.

    bucket 지정 시: s3a://bucket/prefix/bronze/{event_type}/
    warehouse 지정 시: warehouse/bronze/{event_type}/
    """
    if args.bucket:
        base = f"s3a://{args.bucket}/{args.prefix}/bronze"
        raw_path = f"{base}/{args.event_type}s"
        checkpoint_path = f"s3a://{args.bucket}/checkpoints/bronze/{args.event_type}s"
    else:
        base = f"{args.warehouse}/bronze"
        raw_path = f"{base}/{args.event_type}s"
        checkpoint_path = f"{args.warehouse}/checkpoints/bronze/{args.event_type}s"

    return raw_path, checkpoint_path

# 스트리밍 시작
# parser - CLI 옵션 처리
def main():
    parser = argparse.ArgumentParser(
        description="Kafka -> Bronze raw zone (event-type 별 append-only parquet)"
    )
    parser.add_argument(
        "--event-type",
        choices=list(EVENT_TYPE_DEFAULT_TOPIC.keys()),
        required=True,
        help="처리할 이벤트 타입. schema와 default topic을 결정한다.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="kafka:29092",
        help="Kafka 브로커 주소 (기본: kafka:29092)",
    )
    parser.add_argument(
        "--topic",
        default=None,
        help="Kafka 토픽 (생략 시 event-type 기본값 자동 선택)",
    )
    parser.add_argument(
        "--starting-offsets",
        default="earliest",
        help="시작 오프셋 (기본: earliest)",
    )
    # 출력 경로: bucket(S3) 또는 warehouse(로컬) 중 하나 지정
    path_group = parser.add_mutually_exclusive_group(required=True)
    path_group.add_argument(
        "--bucket",
        help="S3 버킷명 (s3a://bucket/prefix/bronze/{event_type}/ 로 적재)",
    )
    path_group.add_argument(
        "--warehouse",
        help="로컬/MinIO warehouse 경로 (warehouse/bronze/{event_type}/ 로 적재)",
    )
    parser.add_argument(
        "--prefix",
        default="ad-attribution",
        help="S3 경로 prefix (기본: ad-attribution, --bucket 사용 시만 적용)",
    )
    parser.add_argument(
        "--output-format",
        choices=["parquet", "json"],
        default="parquet",
    )
    # Kafka 패키지는 spark-submit --packages 로 전달하는 것이 표준이지만,
    # 컨테이너 내부에서 SparkSession 생성 시 동적으로 추가할 수도 있다.
    parser.add_argument(
        "--kafka-packages",
        default="",
        help="Spark Kafka connector coordinates. 빈 문자열이면 --packages 로 전달된 것을 사용.",
    )
    args = parser.parse_args()

    topic = args.topic or EVENT_TYPE_DEFAULT_TOPIC[args.event_type]
    raw_path, checkpoint_path = resolve_paths(args)

    from pyspark.sql.functions import (
        col,
        current_timestamp,
        from_json,
        from_unixtime,
        hour,
        to_date,
        to_timestamp,
    )

    spark = build_spark(f"BronzeStreaming[{args.event_type}]")
    if args.kafka_packages:
        spark.conf.set("spark.jars.packages", args.kafka_packages)

    # 스키마 생성 - Kafka JSON 해석 준비
    schema = build_event_schema(args.event_type)

    # Kafka Consumer 생성
    #
    # Producer가 Kafka 토픽에 적재한 이벤트를
    # Spark Structured Streaming이 실시간으로 수신한다.
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )


    # Kafka JSON(String) -> Spark DataFrame 변환
    #
    # event_timestamp
    #   실제 이벤트 발생 시각
    #   Silver Join 및 비즈니스 분석 기준
    #
    # ingest_ts
    #   Bronze 수집 시각
    #   Late Event 및 재수집 분석 기준
    #
    # 발생 시각과 수집 시각을 분리 저장하여
    # 이벤트 지연을 추적할 수 있다.

    parsed = (
        kafka_df.select(
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), schema).alias("data"),
        )
        .select("kafka_partition", "kafka_offset", "kafka_timestamp", "data.*")
        # event_timestamp: 이벤트 발생 시각 (Silver JOIN 기준)
        # ingest_ts: Spark 처리 시각 — Silver dedup에서 동일 event_id 중 가장 최근 수집본 선택 시 사용
        # 두 시각을 모두 저장하는 이유: Bronze는 수집 기준, Silver는 이벤트 발생 기준으로 각각 필요하다.
        .withColumn("event_timestamp", to_timestamp(from_unixtime(col("timestamp"))))
        .withColumn("ingest_ts", current_timestamp())
        # raw_date / raw_hour는 ingest 시각 기준으로 파티셔닝.
        # event_time 기준이면 Late Event가 과거 파티션에 들어가
        # Silver가 재처리할 파티션 범위를 산정하기 어렵다.
        .withColumn("raw_date", to_date(col("ingest_ts")))
        .withColumn("raw_hour", hour(col("ingest_ts")))
    )


    # Bronze 적재
    #
    # outputMode("append")
    #   Bronze 원칙인 Append Only 유지
    #
    # checkpointLocation
    #   장애 발생 시 Kafka Offset 복구
    #
    # partitionBy(raw_date, raw_hour)
    #   수집 시각 기준 파티셔닝
    #
    # Bronze는 원본 보존 계층이므로
    # UPDATE / DELETE 없이 Parquet Append만 수행한다.

    query = (
        parsed.writeStream.format(args.output_format)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", raw_path)
        .partitionBy("raw_date", "raw_hour")
        .start()
    )

    print(f"event-type:  {args.event_type}")
    print(f"topic:       {topic}")
    print(f"raw path:    {raw_path}")
    print(f"checkpoint:  {checkpoint_path}")
    query.awaitTermination() # Streaming Job 종료 전까지 계속 Kafka 수신


if __name__ == "__main__":
    main()
