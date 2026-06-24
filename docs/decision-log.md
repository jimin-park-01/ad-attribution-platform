# Design Decisions

프로젝트 진행 중 설계 의사결정과 그 이유를 기록한다.

---

# Decision 001 - Apache Iceberg Adoption

## Context

광고 Attribution 데이터는 Late Event가 발생할 수 있다.

Conversion 이벤트가 Impression 이후 수 시간~수일 뒤에 도착할 수 있으며, 이미 집계된 데이터를 수정해야 하는 상황이 발생한다.

## Decision

Silver 및 Gold Layer에 Apache Iceberg를 사용한다.

## Alternatives

### Plain Parquet

* 단순 저장
* UPDATE 불가능
* 재집계 필요

### Apache Iceberg (Selected)

* MERGE INTO 지원
* Snapshot 관리
* Time Travel 지원
* Late Event 반영 가능

## Reason

Late Event를 효율적으로 반영하기 위해 Iceberg를 선택하였다.

---

# Decision 002 - Bronze Uses Parquet

## Context

Bronze Layer의 목적은 원본 이벤트 보존이다.

## Decision

Bronze Layer는 Iceberg 대신 Plain Parquet를 사용한다.

## Alternatives

### Bronze = Iceberg

* Snapshot 관리 가능
* 운영 복잡도 증가

### Bronze = Parquet (Selected)

* Append Only 구조
* 단순한 저장 방식
* 재처리 용이

## Reason

Bronze는 원본 보존 계층이므로 복잡한 테이블 관리 기능이 필요하지 않다.

---

# Decision 003 - Event-Type Based Kafka Topics

## Context

광고 이벤트는 Impression, Click, Conversion 세 종류로 구성된다.

각 이벤트는 서로 다른 스키마를 가진다.

## Decision

Kafka 토픽을 이벤트 타입별로 분리한다.

### Topics

* ad-impressions
* ad-clicks
* ad-conversions

## Alternatives

### Single Topic

모든 이벤트를 하나의 토픽에 저장

### Multiple Topics (Selected)

이벤트별 토픽 분리

## Reason

* 이벤트별 스키마 분리
* Null 컬럼 최소화
* Bronze 저장 단순화
* Silver Join 구조 명확화

---

# Decision 004 - Ingest-Time Partitioning

## Context

Late Event는 실제 발생 시각보다 늦게 수집될 수 있다.

## Decision

Bronze Layer는 event_timestamp가 아닌 ingest_ts 기준으로 파티셔닝한다.

### Partition Columns

* raw_date
* raw_hour

## Alternatives

### Event Time Partitioning

이벤트 발생 시각 기준

### Ingest Time Partitioning (Selected)

수집 시각 기준

## Reason

* Late Event 관리 용이
* 최근 수집 데이터 확인 가능
* 재처리 범위 단순화

---

# Decision 005 - Medallion Architecture

## Context

데이터 처리 단계별 책임을 분리할 필요가 있다.

## Decision

Bronze → Silver → Gold 구조를 사용한다.

## Responsibilities

### Bronze

원본 데이터 보존

### Silver

정제 및 통합

### Gold

비즈니스 KPI 제공

## Reason

* 계층별 역할 분리
* 재처리 용이
* 운영 및 유지보수 용이
* 데이터 품질 관리 용이

---

# Decision 006 - Silver MERGE Strategy

## Context

광고 Attribution 데이터는 Late Event가 발생할 수 있다.

Conversion 이벤트가 Impression 이후 지연되어 도착할 경우,
이미 처리된 데이터의 상태가 변경될 수 있다.

전체 데이터를 매번 재처리하는 방식은 비용이 크며,
변경된 데이터만 효율적으로 반영할 필요가 있다.

## Decision

Silver Layer는 Iceberg MERGE INTO 기반 Incremental 처리 방식을 사용한다.

## Alternatives

### Full Refresh

* 전체 데이터 재처리
* 구현 단순
* 처리 비용 증가

### MERGE INTO (Selected)

* 변경 데이터만 반영
* Late Event 처리 가능
* 과거 데이터 수정 가능

## Reason

Late Event를 효율적으로 반영하고
전체 재처리 비용을 줄이기 위해 MERGE INTO 방식을 선택하였다.

---

# Decision 007 - Late Event Handling Policy

## Context

광고 Conversion은 Impression 이후 일정 시간이 지난 뒤 발생할 수 있다.

Late Event의 영향을 분석하기 위해서는
지연 전환의 기준을 정의할 필요가 있다.

## Decision

Conversion Delay가 1시간 이상인 경우를 Late Event로 정의한다.

## Alternatives

### No Threshold

모든 Conversion을 동일하게 처리

### Fixed Threshold (Selected)

1시간 이상 지연된 Conversion을 Late Event로 분류

## Reason

Late Event의 영향을 정량적으로 분석하기 위한 기준이 필요하였다.

본 프로젝트에서는 데모 환경을 고려하여
1시간 기준을 적용하였다.

실제 운영 환경에서는 비즈니스 요구사항에 따라
24시간 또는 7일 등으로 조정 가능하다.

---

# Decision 008 - Gold Data Mart Design

## Context

마케팅 KPI 분석 및 대시보드 제공을 위해
집계 단위를 정의할 필요가 있다.

## Decision

Gold Layer는 Campaign × Summary Date 단위로 집계한다.

## Grain

### 1 Row

* 1 Campaign
* 1 Summary Date

## Alternatives

### Event Level

* 상세 분석 가능
* 조회 비용 증가
* 대시보드 성능 저하

### Campaign × Date (Selected)

* KPI 조회 최적화
* Athena 비용 절감
* 대시보드 응답 속도 향상

## Reason

마케팅 KPI 분석과 BI 대시보드 제공에 적합한 집계 단위로 판단하였다.

---

# Decision 009 - Separation of Processing and Maintenance Jobs

## Context

데이터 처리 작업과 Iceberg 유지보수 작업은
목적과 실행 주기가 다르다.

유지보수 작업 실패가 데이터 처리 실패로 이어지지 않도록
분리할 필요가 있다.

## Decision

데이터 처리와 Iceberg 유지보수 작업을 별도의 Spark Job으로 분리한다.

## Components

### Data Processing

* silver_transform.py
* gold_aggregation.py

### Maintenance

* iceberg_maintenance.py

## Reason

* 관심사 분리
* 유지보수 작업 독립 실행
* 장애 발생 시 개별 재실행 가능
* 운영 관리 단순화

---

# Decision 010 - Airflow Orchestration

## Context

Silver 및 Gold 파이프라인은 정기적으로 실행되어야 하며,
작업 간 의존성 관리가 필요하다.

## Decision

Apache Airflow를 사용하여 데이터 파이프라인을 오케스트레이션한다.

## Alternatives

### Cron

* 단순 스케줄링
* 의존성 관리 제한

### Apache Airflow (Selected)

* DAG 기반 의존성 관리
* 재실행 지원
* 실행 이력 관리
* 운영 가시성 확보

## Reason

파이프라인 자동화와 운영 가시성을 확보하기 위해 Airflow를 선택하였다.


# Future Decisions

향후 아래 항목에 대한 의사결정을 추가 기록한다.

* Athena vs Trino
* Data Quality Validation
* Monitoring & Alerting
* Cost Optimization Strategy

