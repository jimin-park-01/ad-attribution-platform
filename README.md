# Ad Attribution Lakehouse Platform

> Apache Iceberg를 활용하여 Late Event를 처리하는 광고 Attribution Lakehouse 프로젝트

Kafka, Spark Structured Streaming, Apache Iceberg, Airflow를 활용하여 광고 이벤트를 수집·처리·집계하고 대시보드까지 연결하는 End-to-End 데이터 엔지니어링 프로젝트입니다.

---

## 프로젝트 개요

광고 데이터는 일반적으로 다음과 같은 흐름을 가집니다.

Impression → Click → Conversion

하지만 실제 환경에서는 Conversion 이벤트가 Click 이후 수 분~수 시간 뒤에 도착할 수 있으며, 이러한 **Late Event(지연 도착 이벤트)** 를 적절히 처리하지 못하면 CTR, CVR, CPA 등의 KPI가 왜곡될 수 있습니다.

본 프로젝트는 Apache Iceberg의 MERGE 기능을 활용하여 Late Event를 반영할 수 있는 광고 데이터 Lakehouse를 구축하는 것을 목표로 하였습니다.

---

## 문제 정의

### Late Event 문제

광고 이벤트는 발생 순서와 수집 순서가 항상 일치하지 않습니다.

예를 들어 사용자가 광고를 클릭한 뒤 일정 시간이 지나 구매를 완료하면 Conversion 이벤트는 늦게 수집될 수 있습니다.

이 경우 단순 Append 방식의 데이터 파이프라인은 이미 집계된 결과를 수정하기 어렵기 때문에 KPI 정합성 문제가 발생합니다.

본 프로젝트는 Iceberg 기반 Silver 레이어에서 MERGE INTO를 수행하여 지연 도착 이벤트를 반영할 수 있도록 설계하였습니다.

---

## Architecture

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Bronze (Raw Parquet)
  ↓
Silver (Apache Iceberg)
  ↓
Gold (Campaign KPI Aggregation)
  ↓
Athena
  ↓
Superset
```

---

## Tech Stack

| Category         | Technology                 |
| ---------------- | -------------------------- |
| Data Ingestion   | Kafka                      |
| Data Processing  | Spark Structured Streaming |
| Storage          | Amazon S3                  |
| Table Format     | Apache Iceberg             |
| Metadata Catalog | AWS Glue Catalog           |
| Query Engine     | Amazon Athena              |
| Orchestration    | Apache Airflow             |
| Visualization    | Apache Superset            |
| Infrastructure   | Docker Compose             |

---

## Data Flow

### Bronze Layer

원본 광고 이벤트를 저장하는 Raw Zone

목적

* 원본 데이터 보존
* 장애 복구
* 재처리(Backfill)

저장 포맷

* Parquet

---

### Silver Layer

광고 이벤트 정제 및 통합 레이어

주요 처리

* 타입 변환
* 중복 제거
* 이벤트 정합성 확보
* Late Event 반영
* MERGE 기반 Upsert

저장 포맷

* Apache Iceberg

---

### Gold Layer

비즈니스 KPI 제공 레이어

주요 지표

* Impressions
* Clicks
* Conversions
* CTR
* CVR
* CPA

---

## 데이터셋

본 프로젝트는 [Criteo Attribution Modeling Dataset](https://ailab.criteo.com/criteo-attribution-modeling-bidding-dataset/)을 사용합니다.

| 항목 | 내용 |
| ---- | ---- |
| 규모 | 16.5M impressions / 700여 캠페인 / 30일 |
| 라이선스 | CC BY-NC-SA 4.0 |
| 출처 | Criteo AI Lab |

### 주요 데이터 특성

**CTR이 일반 디스플레이 광고보다 높게 나타나는 이유**

이 데이터셋은 **리타게팅(Retargeting) 광고** 전용 데이터입니다.
리타게팅은 자사 웹사이트를 이미 방문한 구매 의도 보유 사용자를 대상으로 하며,
Criteo의 클릭 예측 기반 입찰 시스템이 클릭 가능성이 높은 노출만 낙찰받는 구조이므로
일반 디스플레이 광고(CTR 0.1~3%) 대비 현저히 높은 클릭률이 나타납니다.

**비용(cost) 필드는 실제 금액이 아닙니다**

Criteo는 영업 비밀 보호를 위해 실제 광고 단가를 정규화된 상대값으로 변환하여 공개합니다.
공식 문서 원문: *"price paid by Criteo (not real price, transformed version)"*
따라서 대시보드의 CPA는 실제 광고비 단위가 아닌 **정규화 비용 지수**를 나타냅니다.

### 데이터 준비

`scripts/prepare_criteo_data.py`를 사용하여 원본 TSV에서 실습용 CSV로 변환합니다.

```bash
python scripts/prepare_criteo_data.py \
  --input ./data/criteo_attribution_dataset.tsv.gz \
  --sample 1000000
```

- Reservoir Sampling으로 전체 파일을 1회 스캔하여 메모리 효율적으로 추출
- 초기 적재용 800K건 + 증분 적재용 5개 배치 파일 생성

---

## 주요 구현 내용

### 실시간 이벤트 수집

Kafka Producer를 이용하여 광고 이벤트를 스트리밍 데이터처럼 재생

### Apache Iceberg 기반 데이터 관리

* MERGE INTO 기반 Upsert
* Snapshot 관리
* Metadata 기반 운영성 확보

### Late Event 처리

event_id 기준으로 Silver 레이어에서 MERGE 수행

이를 통해 지연 도착 이벤트가 발생하더라도 KPI 정합성을 유지할 수 있도록 설계

### Airflow 자동화

정기적으로

* Silver Merge
* Gold Aggregation
* Maintenance

작업을 자동 수행하도록 구성

### Superset 시각화

Gold 테이블 기반 KPI 대시보드 구성

---

## Dashboard

### Campaign KPI Dashboard

> 추후 이미지 추가 예정

### Airflow DAG

> 추후 이미지 추가 예정

---

## Repository Structure

```text
.
├── conf/               # Spark 설정
├── data/               # 원본 데이터셋 (gitignore)
├── pipeline/           # Spark Streaming 파이프라인
├── scripts/            # 데이터 생성 및 Kafka Producer
├── spark/              # SparkSession 설정
├── Dockerfile
├── requirements.txt
├── README.md
└── CLAUDE.md
```

---

## 프로젝트 결과

* Kafka → Spark → Iceberg → Airflow → Superset End-to-End 파이프라인 구축
* Bronze / Silver / Gold 메달리온 아키텍처 구현
* Apache Iceberg 기반 Late Event 처리
* Airflow 기반 데이터 파이프라인 자동화
* Athena 및 Superset을 활용한 KPI 분석 환경 구축

---

## 향후 확장 계획

* Trino Query Engine 연동
* Data Quality Validation
* Grafana Monitoring
* Slack Alert
* AWS EMR 기반 Spark 실행 환경 전환
* AWS MSK 기반 Kafka 운영 환경 전환
* Kubernetes 기반 배포 환경 구성

---

## Documentation

상세 설계 및 의사결정 과정은 docs 디렉토리에 정리할 예정입니다.

* Architecture
* Design Decisions
* Troubleshooting
* Roadmap

---

## AI Assisted Development

본 프로젝트는 Claude Code와 ChatGPT를 활용하여 설계, 구현, 리팩토링 및 문서화를 진행하였습니다.

자세한 내용은 CLAUDE.md 문서를 참고해주세요.
