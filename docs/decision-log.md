# Design Decisions

## Why Apache Iceberg?

### Decision

Apache Iceberg 사용

### Alternatives

- Plain Parquet

### Reason

- MERGE INTO 지원
- Snapshot 관리
- Late Event 처리 가능

---

## Why Bronze = Parquet?

### Decision

Bronze는 Raw Parquet 사용

### Alternatives

- Bronze Iceberg

### Reason

- 원본 보존 목적
- 단순한 Append 구조
- 재처리 용이