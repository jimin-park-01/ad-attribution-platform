"""
Criteo Attribution Dataset -> 실습용 CSV 변환기.

다운로드:
    https://ailab.criteo.com/criteo-attribution-modeling-bidding-dataset/

사용법:
    # 권장: 100만건 샘플링 (gzip 입력도 바로 가능)
    python scripts/prepare_criteo_data.py --input ./data/criteo_attribution_dataset.tsv.gz

    # 더 작은 로컬 테스트
    python scripts/prepare_criteo_data.py --input ./data/criteo_attribution_dataset.tsv.gz --sample 100000

    # cat1~cat9 유지
    python scripts/prepare_criteo_data.py --input ./data/criteo_attribution_dataset.tsv.gz --keep-cats
"""

import argparse
import csv
import gzip
import os
import random
from datetime import datetime

# 상수 정의
DEFAULT_SAMPLE_SIZE = 1_000_000  # 100 만건 추출 
DEFAULT_BASE_DATETIME = "2026-05-15T00:00:00" # 기준 시각 
RELATIVE_TIMESTAMP_THRESHOLD = 1_000_000_000 # 상대시간 판별 기준 
SCAN_PROGRESS_EVERY = 1_000_000 # 로그 출력 주기


# 유연한 입력 지원
def open_input(path):
    """일반 TSV와 TSV.GZ를 모두 지원한다.

    Criteo 원본 배포 형식은 .tsv.gz이지만, 압축 해제 후 직접 사용하거나
    로컬 테스트를 위해 작은 TSV를 별도 준비하는 경우도 있어 두 형식을 모두 수용한다.
    """
    if path.endswith(".gz"):
        return gzip.open(path, "rt", newline="")
    return open(path, "r", newline="")


# TSV 헤더 추출 -> 컬럼명 가져오기
def parse_header(header_line):
    return header_line.rstrip("\r\n").split("\t")


# TSV 한 줄 -> dict 변환 -> 이후 코드에서 쓰기 편하게 하기 위함
def parse_tsv_line(line, fieldnames):
    values = line.rstrip("\r\n").split("\t")
    if len(values) < len(fieldnames):
        values.extend([""] * (len(fieldnames) - len(values)))
    elif len(values) > len(fieldnames):
        values = values[: len(fieldnames)]
    return dict(zip(fieldnames, values))


# 문지열 -> 정수 -> 빈값 null 이상값 같은 데이터 품질 문제 방어  
def safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# 전체 파일 스캔 -> 100만건 샘플 추출 -> 전체 데이터를 메모리에 적재하지 않고 파일을 한 번만 스캔하면서 대표 샘플을 생성하기 위해 사용 
def scan_and_sample_lines(input_path, sample_size, seed=42):
    """
    TSV를 한 번만 스캔하면서 reservoir sampling 수행.

    Criteo 원본은 압축 해제 시 수십 GB에 달해 전체를 메모리에 올리는 것이 불가능 -> 메모리 터짐 
    Reservoir sampling은 파일을 1회 스캔하면서 각 행을 동일한 확률로 샘플 버킷에 교체하므로, 
    메모리는 sample_size에만 비례하고 파일 크기에 무관하다.
    """
    rng = random.Random(seed)
    sampled_lines = []
    total_rows = 0
    min_ts = None
    max_ts = None

    with open_input(input_path) as handle:
        header_line = handle.readline()
        if not header_line:
            raise ValueError("입력 파일이 비어 있습니다.")
        fieldnames = parse_header(header_line)

        for raw_line in handle:
            line = raw_line.rstrip("\r\n")
            if not line:
                continue

            total_rows += 1

            ts_field = line.split("\t", 1)[0]
            ts = safe_int(ts_field, default=None)
            if ts is not None:
                min_ts = ts if min_ts is None else min(min_ts, ts)
                max_ts = ts if max_ts is None else max(max_ts, ts)

            if len(sampled_lines) < sample_size:
                sampled_lines.append(line)
            else:
                pick = rng.randint(0, total_rows - 1)
                if pick < sample_size:
                    sampled_lines[pick] = line

            if total_rows % SCAN_PROGRESS_EVERY == 0:
                print(f"  스캔 중... {total_rows:,}건")

    return fieldnames, sampled_lines, total_rows, min_ts, max_ts


# timestamp기 싱대시간인지 절대시간인지 판별
def choose_base_timestamp(min_ts, max_ts, base_datetime, preserve_relative_timestamps):
    # RELATIVE_TIMESTAMP_THRESHOLD(1_000_000_000)는 Unix 시각 기준 약 2001년 9월 이전을 뜻한다.
    # Criteo 원본의 timestamp가 이 값보다 작으면 절대 시각이 아닌 데이터셋 수집 기준 상대값이므로
    # base_datetime을 기준으로 절대 시각으로 변환한다.
    if preserve_relative_timestamps:
        return None, False

    if max_ts is None:
        return None, False

    if max_ts < RELATIVE_TIMESTAMP_THRESHOLD:
        base_ts = int(datetime.fromisoformat(base_datetime).timestamp())
        return base_ts, True

    return None, False


# 정렬용 
def extract_timestamp(line):
    return safe_int(line.split("\t", 1)[0], 0)


# 원본 criteo -> 프로젝트 표준 스키마 변환 -> 원본의 많은 컬럼들에서 필요한 광고 이벤트 데이터만 가져와 표준 스키마를 만듦
# 원본 데이터 의존성을 줄이고 파이프라인 표준 스키마를 만든다
def convert_row(row, keep_cats=False, base_timestamp=None):
    """
    Criteo 원본 컬럼 -> 실습용 포맷 변환.

    Criteo 원본은 이벤트 시각을 데이터셋 수집 시점 기준의 상대값(초)으로 저장한다.
    파이프라인에서 Bronze/Silver/Gold가 모두 절대 Unix 시각을 기대하므로
    base_timestamp를 더해 절대 시각으로 복원한다.
    """
    ts = safe_int(row.get("timestamp"), 0)
    conversion_ts = safe_int(row.get("conversion_timestamp"), 0)

    if base_timestamp is not None:
        ts += base_timestamp
        if conversion_ts > 0:
            conversion_ts += base_timestamp

    converted = {
        "timestamp": str(ts),
        "uid": row.get("uid", ""),
        "campaign": row.get("campaign", ""),
        "click": str(safe_int(row.get("click"), 0)),
        "conversion": str(safe_int(row.get("conversion"), 0)),
        "conversion_timestamp": str(conversion_ts) if conversion_ts > 0 else "",
        "cost": row.get("cost", "0.0") or "0.0",
    }

    if keep_cats:
        for idx in range(1, 10):
            key = f"cat{idx}"
            converted[key] = row.get(key, "")

    return converted


# 통계 초기화 -> 지표 계산용 
def init_stats():
    return {
        "total": 0,
        "clicks": 0,
        "conversions": 0,
        "total_cost": 0.0,
        "delay_sum": 0,
        "delay_count": 0,
        "campaigns": set(),
        "min_ts": None,
        "max_ts": None,
    }


# 통계 누적 -> 데이터 품질 확인 
def update_stats(stats, row):
    stats["total"] += 1
    click = safe_int(row.get("click"), 0)
    conversion = safe_int(row.get("conversion"), 0)
    ts = safe_int(row.get("timestamp"), 0)
    conversion_ts = safe_int(row.get("conversion_timestamp"), 0)

    stats["clicks"] += click
    stats["conversions"] += conversion
    stats["total_cost"] += float(row.get("cost", 0) or 0)
    stats["campaigns"].add(row.get("campaign", ""))

    if stats["min_ts"] is None or ts < stats["min_ts"]:
        stats["min_ts"] = ts
    if stats["max_ts"] is None or ts > stats["max_ts"]:
        stats["max_ts"] = ts

    if conversion and conversion_ts > ts:
        stats["delay_sum"] += (conversion_ts - ts)
        stats["delay_count"] += 1


def format_period(min_ts, max_ts):
    if min_ts is None or max_ts is None:
        return "N/A"
    start = datetime.fromtimestamp(min_ts)
    end = datetime.fromtimestamp(max_ts)
    return f"{start.strftime('%Y-%m-%d %H:%M')} ~ {end.strftime('%Y-%m-%d %H:%M')}"


# 반환 결과 검증 
def print_stats(stats, label):
    total = stats["total"]
    clicks = stats["clicks"]
    conversions = stats["conversions"]
    total_cost = stats["total_cost"]
    avg_delay_hr = (
        stats["delay_sum"] / stats["delay_count"] / 3600 if stats["delay_count"] else 0
    )

    print(f"  [{label}]")
    print(f"  총 이벤트:     {total:>10,}건")
    if total:
        print(f"  클릭:          {clicks:>10,}건  (CTR {clicks / total * 100:.2f}%)")
    else:
        print(f"  클릭:          {clicks:>10,}건")
    if clicks:
        print(
            f"  전환:          {conversions:>10,}건  "
            f"(CVR {conversions / clicks * 100:.2f}% of clicks)"
        )
    else:
        print(f"  전환:          {conversions:>10,}건")
    print(f"  총 비용:       ${total_cost:>10,.2f}")
    if conversions:
        print(f"  평균 CPA:      ${total_cost / conversions:>10,.2f}")
    print(f"  평균 전환지연:  {avg_delay_hr:.1f}시간")
    print(f"  캠페인 수:     {len(stats['campaigns'])}")
    print(f"  기간:          {format_period(stats['min_ts'], stats['max_ts'])}")


# 최종 결과 생성
# ad_events.csv -> 메인 데이터 ( 초기 적재 )
# ad_events_sample.csv -> 1000건 ( 빠른 테스트용 )
# ad_events_batch_01.csv -> 초기 적재 이후 유입되는 Incremental Load 시뮬레이션용 (Airflow, MERGE INTO, Late Event 검증에 사용 )

def write_outputs(sampled_lines, fieldnames, output_dir, keep_cats=False, base_timestamp=None):
    output_fields = [
        "timestamp",
        "uid",
        "campaign",
        "click",
        "conversion",
        "conversion_timestamp",
        "cost",
    ]

    if keep_cats:
        output_fields.extend([f"cat{i}" for i in range(1, 10)])

    initial_path = os.path.join(output_dir, "ad_events_initial.csv")
    sample_path = os.path.join(output_dir, "ad_events_sample.csv")

    batch_specs = [
        ("ad_events_batch_01.csv", 20_000),
        ("ad_events_batch_02.csv", 30_000),
        ("ad_events_batch_03.csv", 40_000),
        ("ad_events_batch_04.csv", 50_000),
        ("ad_events_batch_05.csv", 60_000),
    ]

    sample_limit = min(1000, len(sampled_lines))
    initial_count = 800_000

    stats = init_stats()

    batch_ranges = []

    start = initial_count

    for filename, count in batch_specs:
        end = start + count
        batch_ranges.append((filename, start, end))
        start = end

    with (
        open(initial_path, "w", newline="") as initial_file,
        open(sample_path, "w", newline="") as sample_file,
    ):

        initial_writer = csv.DictWriter(
            initial_file,
            fieldnames=output_fields,
        )

        sample_writer = csv.DictWriter(
            sample_file,
            fieldnames=output_fields,
        )

        initial_writer.writeheader()
        sample_writer.writeheader()

        batch_files = {}
        batch_writers = {}

        for filename, _ in batch_specs:
            path = os.path.join(output_dir, filename)

            handle = open(path, "w", newline="")

            batch_files[filename] = handle

            writer = csv.DictWriter(
                handle,
                fieldnames=output_fields,
            )

            writer.writeheader()

            batch_writers[filename] = writer

        try:

            for idx, line in enumerate(sampled_lines):

                row = parse_tsv_line(line, fieldnames)

                converted = convert_row(
                    row,
                    keep_cats=keep_cats,
                    base_timestamp=base_timestamp,
                )

                if idx < initial_count:
                    initial_writer.writerow(converted)

                if idx < sample_limit:
                    sample_writer.writerow(converted)

                for filename, start, end in batch_ranges:
                    if start <= idx < end:
                        batch_writers[filename].writerow(converted)
                        break

                update_stats(stats, converted)

        finally:
            for handle in batch_files.values():
                handle.close()

    return {
        "initial_path": initial_path,
        "sample_path": sample_path,
        "batch_specs": batch_specs,
        "sample_limit": sample_limit,
        "stats": stats,
    }

# 전체 오케스트레이션
def main():
    parser = argparse.ArgumentParser(
        description="Criteo Attribution Dataset -> 실습용 CSV 변환"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Criteo TSV 파일 경로 (.tsv 또는 .tsv.gz)",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help=f"샘플링할 행 수 (기본: {DEFAULT_SAMPLE_SIZE:,})",
    )
    parser.add_argument(
        "--output",
        default="./data",
        help="출력 디렉토리 (기본: ./data)",
    )
    parser.add_argument(
        "--keep-cats",
        action="store_true",
        help="cat1~cat9 컬럼 유지 (기본: 제거)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="랜덤 시드 (기본: 42)",
    )
    parser.add_argument(
        "--base-datetime",
        default=DEFAULT_BASE_DATETIME,
        help=f"상대 timestamp를 rebasing할 기준 시각 (기본: {DEFAULT_BASE_DATETIME})",
    )
    parser.add_argument(
        "--preserve-relative-timestamps",
        action="store_true",
        help="Criteo 원본의 상대 timestamp를 그대로 유지",
    )
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    print("=" * 60)
    print("  Criteo Attribution Dataset 변환기")
    print("=" * 60)
    print(f"  입력: {args.input}")
    print(f"  샘플: {args.sample:,}건")
    print(f"  cat1~9: {'유지' if args.keep_cats else '제거'}")
    print()
    print("  TSV 스캔 + reservoir sampling 중...")

    fieldnames, sampled_lines, total_rows, min_ts, max_ts = scan_and_sample_lines(
        args.input,
        args.sample,
        seed=args.seed,
    )
    print(f"  원본 스캔 완료: {total_rows:,}건")
    print(f"  샘플링 완료: {len(sampled_lines):,}건")
    print(f"  원본 컬럼: {fieldnames}")

    base_timestamp, rebased = choose_base_timestamp(
        min_ts,
        max_ts,
        args.base_datetime,
        args.preserve_relative_timestamps,
    )
    if rebased:
        print(f"  timestamp 모드: relative -> {args.base_datetime} 기준 absolute 변환")
    elif args.preserve_relative_timestamps:
        print("  timestamp 모드: relative 유지")
    else:
        print("  timestamp 모드: absolute 유지")

    print("  샘플 정렬 중...")
    sampled_lines.sort(key=extract_timestamp)

    print("  CSV 출력 중...")
    result = write_outputs(
        sampled_lines,
        fieldnames,
        args.output,
        keep_cats=args.keep_cats,
        base_timestamp=base_timestamp,
    )

    print()
    print("-" * 60)
    print_stats(result["stats"], "전체")
    print()
    print("-" * 60)

    print(
        f"  {result['initial_path']:<40} "
        f"({os.path.getsize(result['initial_path']) / 1024 / 1024:.1f} MB)"
    )

    print(
        f"  {result['sample_path']:<40} "
        f"({result['sample_limit']:,}건)"
    )

    for filename, count in result["batch_specs"]:
        path = os.path.join(args.output, filename)

        print(
            f"  {path:<40} "
            f"(batch, {count:,}건)"
        )

    print()
    print("  다음 단계:")
    print("    초기 적재")
    print("    python scripts/kafka_producer.py --create-topics --csv ./data/ad_events_initial.csv --max-events 0")
    print("    python scripts/kafka_producer.py --csv ./data/ad_events_initial.csv")
    print()
    print("    증분 적재")
    print("    python scripts/kafka_producer.py --csv ./data/ad_events_batch_01.csv")
    print("    python scripts/kafka_producer.py --csv ./data/ad_events_batch_02.csv")
    print("    python scripts/kafka_producer.py --csv ./data/ad_events_batch_03.csv")
    print("    python scripts/kafka_producer.py --csv ./data/ad_events_batch_04.csv")
    print("    python scripts/kafka_producer.py --csv ./data/ad_events_batch_05.csv")

    print("=" * 60)


if __name__ == "__main__":
    main()

# 전체 처리 흐름
#
# Criteo TSV
#   ↓
# Reservoir Sampling
#   ↓
# Timestamp Rebase
#   ↓
# 표준 스키마 변환
#   ↓
# ad_events.csv 생성
#   ↓
# Kafka Producer 입력 데이터 준비