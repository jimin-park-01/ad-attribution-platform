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
