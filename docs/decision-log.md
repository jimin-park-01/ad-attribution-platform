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

# Future Decisions

향후 아래 항목에 대한 의사결정을 추가 기록한다.

* Silver MERGE 전략
* Late Event 처리 정책
* Airflow Orchestration
* Athena vs Trino
* Data Quality Validation
* Monitoring & Alerting
