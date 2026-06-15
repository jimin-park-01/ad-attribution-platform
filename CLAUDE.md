# Ad Attribution Lakehouse Platform

## 정체성

개인 데이터 엔지니어링 포트폴리오 프로젝트

프로젝트명

Ad Attribution Lakehouse Platform

---

## 미션

광고 Attribution 데이터를 처리하는 End-to-End 데이터 파이프라인 구축

목표

* Bronze / Silver / Gold 메달리온 아키텍처 구현
* Apache Iceberg 기반 데이터 관리
* Late Event 처리
* KPI 집계 자동화
* Superset 시각화

---

## 아키텍처

Kafka
↓
Spark Structured Streaming
↓
Bronze (Raw Parquet)
↓
Silver (Apache Iceberg)
↓
Gold (Campaign KPI)
↓
Athena
↓
Superset

---

## 핵심 설계 원칙

### Bronze

원본 데이터 보존

* Append Only
* 재처리 가능
* 장애 복구 가능

### Silver

정제 및 통합

* Join
* Deduplication
* MERGE INTO
* Late Event 처리

### Gold

비즈니스 KPI 제공

* CTR
* CVR
* CPA

---

## Iceberg 활용 목적

단순 저장 포맷이 아니라 핵심 기능 활용

* MERGE INTO
* Snapshot 관리
* Late Event 반영
* 향후 Backfill 지원

---

## 개발 원칙

* 데이터 흐름을 먼저 설계한다.
* 구현보다 설계 이유를 설명할 수 있어야 한다.
* 하드코딩을 최소화한다.
* 데이터와 로직을 분리한다.
* 재실행 가능한 구조를 유지한다.

---

## Spark 코드 규칙

* DataFrame API 우선 사용
* Transformation 단계 분리
* 핵심 로직에는 Why 주석 작성

---

## Working Style

문제 정의
→ 설계
→ 구현
→ 검증
→ 문서화

순서로 진행한다.

---

## 향후 확장

* Trino
* Grafana
* Data Quality Validation
* AWS EMR
* AWS MSK
* Kubernetes

---

## Notes

본 프로젝트는 학습 및 포트폴리오 목적의 프로젝트이다.

실제 운영 환경을 완전히 재현하는 것이 아니라 데이터 엔지니어링 핵심 개념과 설계 경험을 보여주는 것을 목표로 한다.
