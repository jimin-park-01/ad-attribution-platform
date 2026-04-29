# 광고 데이터 플랫폼 (Ad Data Platform)
Kafka, Spark, Iceberg, Airflow 기반으로 광고 데이터를 수집, 처리, 저장하는  
Lakehouse 아키텍처 기반 데이터 플랫폼을 구축하는 프로젝트입니다.

데이터 수집부터 처리, 저장, 관리, 활용까지 이어지는 전체 흐름을 설계하고  
Streaming 기반 데이터 파이프라인과 데이터 관리 구조를 이해하는 것을 목표로 합니다.


## 🔄 Architecture
External → Kafka → Spark → S3 → Iceberg → Athena → QuickSight  
　　　　　　　　　　　　　　↑  
　　　　　　　　　　　　 Airflow  


## 📌 Tech Stack
Kafka, Spark, Iceberg, S3, Glue Catalog, Athena, Airflow, QuickSight


## 📊 What I’m Building
- Kafka → Spark 기반 데이터 파이프라인 구축
- S3 기반 데이터 레이크 구조 설계
- Iceberg를 활용한 데이터 관리 구조 구현
- Airflow를 활용한 파이프라인 자동화 구성
- Athena 기반 데이터 조회 환경 구성


## 💡 Key Concepts
- Lakehouse 아키텍처 기반 데이터 처리 구조
- Iceberg를 통한 데이터 및 메타데이터 관리
- Streaming 기반 데이터 적재 흐름
- 데이터 수집부터 활용까지 이어지는 데이터 파이프라인 설계


## 📂 Project Structure
```
sql/
├── ddl/
│   └── create_events.sql
├── dml/
│   └── insert_events.sql
└── metadata/

screenshots/
├── athena/
│   ├── snapshots.png
│   ├── history.png
│   └── files.png
└── s3/
    └── iceberg_s3_structure.png
```
