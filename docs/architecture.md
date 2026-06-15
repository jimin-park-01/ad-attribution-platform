# Architecture

## Overview

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

## Layer Responsibilities

### Bronze
원본 이벤트 저장

### Silver
정제 및 Late Event 처리

### Gold
비즈니스 KPI 집계