# Troubleshooting

프로젝트 진행 중 발생한 주요 이슈와 해결 과정을 기록한다.

---

# Issue 001 - AWS Authentication Failure

## Symptoms

AWS CLI 및 Spark 작업 실행 시 인증 오류 발생

```text
InvalidClientTokenId

The security token included in the request is invalid.
```

또는

```text
UnrecognizedClientException
```

## Cause

AWS CLI는 기본적으로 `default` 프로파일을 사용하고 있었지만,
현재 유효한 Access Key는 `iceberg-lab` 프로파일에만 등록되어 있었다.

결과적으로 AWS 요청이 만료되었거나 잘못된 자격 증명으로 수행되었다.

## Investigation

```bash
aws configure list
```

```bash
aws sts get-caller-identity
```

실행 시 인증 실패 확인

```bash
aws sts get-caller-identity --profile iceberg-lab
```

실행 시 정상 응답 확인

## Resolution

유효한 AWS Profile(`iceberg-lab`)을 사용하도록 설정

```bash
export AWS_PROFILE=iceberg-lab
```

또는

```bash
aws ... --profile iceberg-lab
```

사용

## Lesson Learned

AWS CLI, Docker, Spark가 사용하는 프로파일을 명확히 관리해야 한다.

---

# Issue 002 - Docker Compose Network Error

## Symptoms

Docker Compose 실행 시 네트워크 관련 오류 발생

```text
failed to set up container networking

network ... not found
```

또는

```text
Resource is still in use
```

## Cause

이전 Docker Compose 환경 종료 과정에서 네트워크 리소스가 정상적으로 정리되지 않았다.

컨테이너는 제거되었지만 Compose 네트워크가 비정상 상태로 남아 있었다.

## Investigation

```bash
docker network ls
```

```bash
docker compose down
```

실행 후에도 네트워크가 남아있는 것을 확인

## Resolution

```bash
docker compose down --remove-orphans
```

```bash
docker network prune -f
```

실행 후 재기동

## Lesson Learned

컨테이너뿐 아니라 Docker Network와 Volume도 함께 관리해야 한다.

---

# Issue 003 - Bronze Parquet Files Not Created

## Symptoms

Kafka UI에서는 메시지가 정상적으로 확인되지만
S3 Bronze 영역에 Parquet 파일이 생성되지 않았다.

```text
Kafka Message
? 존재

Bronze Parquet
? 미생성
```

## Cause

초기에는 Kafka 또는 Spark Streaming 문제로 의심했지만,
실제 원인은 AWS 인증 문제로 인해 Spark가 S3에 데이터를 쓰지 못하고 있었기 때문이었다.

## Investigation

1. Kafka UI에서 메시지 존재 확인
2. Spark Streaming Job 정상 실행 확인
3. AWS 인증 상태 확인
4. S3 경로 확인

## Resolution

AWS 인증 문제 해결 후 Spark Streaming 재실행

```bash
aws sts get-caller-identity --profile iceberg-lab
```

정상 응답 확인 후 Bronze Streaming 재시작

## Lesson Learned

Kafka → Spark → S3 파이프라인은 단계별로 검증해야 한다.

특정 단계의 성공이 전체 파이프라인 성공을 의미하지 않는다.

* Kafka 메시지 존재 여부
* Spark Streaming 상태
* S3 Write 권한
* Checkpoint 상태

를 각각 확인해야 한다.

---

# Issue 004 - Incomplete Bronze Impression Ingestion

## Symptoms

Producer 실행 결과 Impression 이벤트는 총 1,000,000건 생성되었지만 Bronze 및 Silver 적재 건수가 일치하지 않았다.

```text
Producer

IMP = 1,000,000

Bronze

IMP = 710,427

Silver

processed_events = 710,427
```

Click 및 Conversion 데이터는 정상 적재되었으나 Impression 데이터만 약 29만 건 누락된 상태였다.

```text
Click = 361,203 (정상)

Conversion = 49,118 (정상)

Impression = 710,427 (누락)
```

## Cause

초기에는 Kafka 적재 실패 또는 Spark Streaming 문제를 의심하였다.

그러나 Kafka Topic을 확인한 결과 Impression 이벤트는 정상적으로 저장되어 있었다.

```text
Kafka Topic

ad-impressions

Total Messages = 1,000,000
```

Producer 및 Kafka 적재 과정에는 문제가 없었으며, Bronze Streaming 처리 과정에서 Impression 데이터가 완전하게 반영되지 않은 것으로 판단하였다.

정확한 원인은 재현하지 못했으나,
Kafka 적재 및 Offset 설정 문제는 배제하였다.

Checkpoint 상태 또는 Streaming Job 실행 과정에서
Impression 데이터가 완전하게 반영되지 않은 것으로 판단하였다.

## Investigation

Kafka Topic 상태 확인

```text
ad-impressions

Total Messages = 1,000,000
```

Bronze 데이터 확인

```python
imp.count()

710427
```

Click 및 Conversion 적재 건수 비교

```text
Click = 361,203

Conversion = 49,118
```

Bronze Streaming 코드 확인

```python
.option("startingOffsets", "earliest")
```

설정 확인

Kafka 적재 문제 및 Offset 설정 문제는 아닌 것으로 판단

## Resolution

Impression Bronze 데이터와 Checkpoint를 삭제한 후 Streaming Job을 재실행하였다.
기존 Kafka Topic 데이터는 유지한 상태에서 Consumer만 재실행하여 복구를 수행하였다.

```text
bronze/impressions

checkpoints/bronze/impressions
```

삭제 후

```bash
spark-submit bronze_streaming.py \
  --event-type impression
```

재실행

재실행 결과 Kafka에 저장되어 있던 1,000,000건의 데이터를 다시 읽어 정상 적재하였다.

```python
imp.count()

1000000
```

## Additional Observation

Checkpoint 제거 후 Streaming Job을 재실행하면서 Kafka의 과거 데이터를 처음부터 다시 읽는 Backfill 형태로 동작하였다.

```text
Kafka (1,000,000 Events)

↓

Structured Streaming

↓

Single Large Micro Batch

↓

Bronze Re-ingestion
```

이 과정에서 Kafka에 누적되어 있던 데이터를
단일 대형 Micro Batch로 처리하였다.

향후 운영 환경에서는 지속적인 Streaming 적재를 통해
보다 자연스러운 파일 분산이 이루어질 것으로 예상된다.

현재 단계에서는 데이터 정합성을 우선 확보하였으며,

향후 Airflow 기반 운영 환경에서는

* 지속적인 Streaming 적재
* Iceberg Compaction
* Small File 관리
* Snapshot Maintenance

를 적용하여 파일 레이아웃과 조회 성능을 최적화할 예정이다.

## Lesson Learned

데이터 정합성(Data Completeness)은 파일 수나 파일 크기 최적화보다 우선적으로 확보되어야 한다.

또한 Kafka 기반 Streaming 파이프라인에서는

* Kafka 적재 건수
* Consumer Offset
* Checkpoint 상태
* Bronze 적재 건수

를 함께 검증해야 한다.

문제 발생 시 Kafka에 저장된 과거 데이터를 활용하여 Backfill 방식으로 복구할 수 있음을 확인하였다.

Checkpoint는 장애 복구에 유용하지만,
문제 발생 시 Consumer Offset 상태를 함께 확인해야 한다.
또한 Kafka에 원본 이벤트가 보존되어 있다면 Backfill을 통해 데이터 정합성을 복구할 수 있다.
---
