######################################################################
# Ad Attribution Lakehouse Platform - Spark Runtime Image
#
# Dockerfile 역할
# --------------------------------------------------------------------
# 이 파일은 Bronze / Silver / Gold 파이프라인을 실행하기 위한
# Spark 실행 환경(Container Image)을 정의한다.
#
# Dockerfile = "컨테이너 환경을 만드는 설계도"
#
# 주요 목적
# 1. Spark 실행 환경 제공
# 2. Apache Iceberg 사용 환경 구성
# 3. AWS S3 연동 환경 구성
# 4. AWS Glue Catalog 연동 환경 구성
# 5. Kafka Producer 및 데이터 처리용 Python 패키지 제공
#
# 최종 결과
# --------------------------------------------------------------------
# Spark + Iceberg + S3 + Glue + Python 패키지가 포함된
# 데이터 처리용 컨테이너 이미지 생성
#
# 참고
# --------------------------------------------------------------------
# Dockerfile
#   -> 컨테이너 환경 정의
#
# Docker Image
#   -> Dockerfile로 생성된 실행 이미지
#
# Docker Compose
#   -> Spark / Kafka / Airflow / Superset 등 여러 컨테이너를
#      함께 실행하기 위한 오케스트레이션 설정
#
# Base Image
# --------------------------------------------------------------------
# jupyter/pyspark-notebook
# - Spark 3.5.3
# - PySpark
# - Jupyter Notebook
#
# Bronze Layer
# --------------------------------------------------------------------
# hadoop-aws
# aws-java-sdk-bundle
#
# -> Spark가 s3a:// 프로토콜을 이용해
#    Amazon S3에 직접 읽기/쓰기 가능
#
# Silver / Gold Layer
# --------------------------------------------------------------------
# iceberg-spark-runtime
# iceberg-aws-bundle
#
# -> Iceberg Table 사용
# -> MERGE INTO 지원
# -> Snapshot 관리
# -> Glue Catalog 연동
######################################################################

FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

# JAR 설치를 위해 root 권한 사용
USER root

# --------------------------------------------------------------------
# Spark / Iceberg / Hadoop / AWS SDK 버전 관리
#
# 버전 업그레이드 시 ENV 값만 수정하면 되도록 변수화
# --------------------------------------------------------------------
ENV ICEBERG_VERSION=1.5.2
ENV SPARK_MAJOR=3.5
ENV SCALA_MAJOR=2.12
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_BUNDLE_VERSION=1.12.262

# --------------------------------------------------------------------
# Spark에 필요한 추가 JAR 설치
#
# Spark 기본 이미지에는 Iceberg 및 AWS 연동 기능이 없으므로
# 필요한 라이브러리를 직접 추가한다.
#
# iceberg-spark-runtime
#   - Iceberg 테이블 읽기/쓰기
#   - MERGE INTO 지원
#
# iceberg-aws-bundle
#   - AWS Glue Catalog
#   - S3 FileIO
#
# hadoop-aws
#   - Spark의 s3a:// 파일시스템 지원
#
# aws-java-sdk-bundle
#   - AWS 인증 및 S3 접근
# --------------------------------------------------------------------
RUN cd /usr/local/spark/jars && \
    wget -q "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR}_${SCALA_MAJOR}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR}_${SCALA_MAJOR}-${ICEBERG_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

# --------------------------------------------------------------------
# Spark가 AWS CLI 자격증명(~/.aws/credentials)을
# 자동으로 읽을 수 있도록 설정
#
# Access Key를 코드에 직접 작성하지 않고
# 기본 AWS Credential Chain을 사용
# --------------------------------------------------------------------
RUN echo "spark.hadoop.fs.s3a.aws.credentials.provider com.amazonaws.auth.DefaultAWSCredentialsProviderChain" \
      >> /usr/local/spark/conf/spark-defaults.conf

# --------------------------------------------------------------------
# 프로젝트에서 사용하는 Python 라이브러리 설치
#
# kafka-python-ng
#   - Kafka Producer
#
# faker
#   - 테스트 데이터 생성
#
# pandas
#   - CSV 처리
#
# pyarrow
#   - Parquet 처리
#
# boto3
#   - AWS SDK
# --------------------------------------------------------------------
RUN pip install --no-cache-dir \
    kafka-python-ng==2.2.2 \
    faker==28.0.0 \
    pandas \
    pyarrow \
    boto3

# --------------------------------------------------------------------
# Iceberg Local Catalog 실습용 Warehouse 디렉토리
#
# Local Catalog 모드 사용 시
# Iceberg 데이터 및 메타데이터 저장 위치
# --------------------------------------------------------------------
RUN mkdir -p /home/jovyan/warehouse && \
    chown -R ${NB_UID}:${NB_GID} /home/jovyan/warehouse

# 보안상 일반 사용자 권한으로 전환
USER ${NB_UID}

# 컨테이너 기본 작업 디렉토리
WORKDIR /home/jovyan/work