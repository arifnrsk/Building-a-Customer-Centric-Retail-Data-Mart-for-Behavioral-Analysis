# Technical Documentation
## Building a Customer-Centric Retail Data Mart for Behavioral Analysis

---

## Table of Contents
1. [System Architecture](#1-system-architecture)
2. [Data Pipeline Flow](#2-data-pipeline-flow)
3. [Code Structure](#3-code-structure)
4. [Analytics Methodology](#4-analytics-methodology)
5. [Database Schema](#5-database-schema)
6. [Configuration Management](#6-configuration-management)
7. [Development Guide](#7-development-guide)
8. [Performance Optimization](#8-performance-optimization)
9. [Monitoring and Logging](#9-monitoring-and-logging)
10. [Security Considerations](#10-security-considerations)

---

## 1. System Architecture

### 1.1 High-Level Architecture
The system follows a modern data engineering architecture pattern with containerized microservices:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Source   │───▶│  Orchestration  │───▶│   Processing    │
│  (Generated     │    │   (Airflow)     │    │    (Spark)      │
│   CSV Files)    │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Visualization   │◀───│   Data Mart     │◀───│   Transformed   │
│  (Metabase)     │    │ (PostgreSQL)    │    │      Data       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### 1.2 Technology Stack

#### Core Technologies
- **Containerization**: Docker & Docker Compose
- **Workflow Orchestration**: Apache Airflow 2.7.0
- **Data Processing**: Apache Spark 3.4.0 with PySpark
- **Data Storage**: PostgreSQL 13
- **Data Visualization**: Metabase
- **Data Generation**: Python 3.9 with Pandas

#### Development Tools
- **Version Control**: Git
- **Documentation**: Markdown
- **Configuration**: YAML, Environment Variables
- **Networking**: Docker Bridge Network
- **Containerization**: Custom Airflow Dockerfile with PySpark support

### 1.3 Container Architecture

#### Services Overview
```yaml
Services:
├── airflow-init      # Database initialization
├── airflow-webserver # Web UI (Port 8080)
├── airflow-scheduler # Task scheduling
├── spark-master      # Spark cluster master (Port 8081)
├── spark-worker      # Spark worker nodes
├── postgres          # Data warehouse (Port 5432)
└── metabase         # Analytics dashboard (Port 3000)
```

#### Network Configuration
- **Internal Network**: `data_network` (bridge driver)
- **External Ports**: 8080 (Airflow), 8081 (Spark), 5432 (PostgreSQL), 3000 (Metabase)
- **Volume Mounts**: Persistent data storage and code synchronization

---

## 2. Data Pipeline Flow

### 2.1 End-to-End Data Flow

```mermaid
graph TD
    A[Data Generation Script] --> B[Daily CSV Files]
    B --> C[Airflow DAG Trigger]
    C --> D[Spark Job Initialization]
    D --> E[Data Ingestion]
    E --> F[Data Transformation]
    F --> G[Market Basket Analysis]
    F --> H[RFM Customer Segmentation]
    G --> I[PostgreSQL Data Mart]
    H --> I
    I --> J[Metabase Dashboards]
```

### 2.2 Data Generation Process

#### Input Data Characteristics
- **Volume**: 1,000,000 transaction records
- **Format**: Daily CSV files (`transaksi_YYYY-MM-DD.csv`)
- **Schema**: Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID

#### Data Quality Features
- **Realistic Patterns**: Indonesian retail products with injected business rules
- **Data Anomalies**: Null customer IDs (10%), zero prices (2%), admin fees (1%)
- **Temporal Distribution**: 500 invoices per day simulation
- **Pattern Injection**: Business rules for product associations (Indomie + Telur, Sunlight + Sponge, etc.)

#### 2.2.1 Pattern Injection Logic
The data generation script includes business rules to create realistic product associations:

```python
# Rule 1: If Indomie is in basket, 70% chance to add Egg
if 'IND-001' in basket_codes and random.random() < 0.7:
    basket_codes.add('IND-002')

# Rule 2: If Sunlight is in basket, 60% chance to add Sponge  
if 'HHLD-001' in basket_codes and random.random() < 0.6:
    basket_codes.add('HHLD-002')

# Rule 3: If Teh Botol is in basket, 50% chance to add Aqua
if 'BEV-001' in basket_codes and random.random() < 0.5:
    basket_codes.add('BEV-002')
```

These patterns enable the Market Basket Analysis to discover meaningful product associations that reflect real-world retail behavior.

### 2.3 ETL Process Details

#### Extract Phase
```python
# File pattern matching
data_path = "/opt/bitnami/spark/data/*.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)
```

#### Transform Phase
1. **Data Cleaning**
   - Remove null customer IDs for RFM analysis
   - Filter positive quantities and prices
   - Standardize date formats

2. **Feature Engineering**
   - Calculate recency (days since last purchase)
   - Calculate frequency (number of transactions)
   - Calculate monetary value (total spent)
   - Generate product combinations for market basket analysis

3. **Business Logic Implementation**
   - RFM scoring using quintile-based segmentation
   - Customer segment classification
   - Association rule mining with confidence and lift metrics

#### Load Phase
- **Target Tables**: `market_basket_analysis`, `customer_segmentation`
- **Load Strategy**: Overwrite mode for full refresh
- **Data Validation**: Row count and schema validation

---

## 3. Code Structure

### 3.1 Project Directory Structure

```
final-project-inventory/
├── dags/
│   └── inventory_pipeline_dag.py            # Airflow DAG definition
├── spark_jobs/
│   └── transform_job.py                      # Spark transformation logic
├── images/                                   # Documentation images
├── generated_data/                           # CSV data files (gitignored)
├── docker-compose.yml                        # Service orchestration
├── Dockerfile.airflow                        # Airflow container definition
├── generate_data.py                          # Data generation script
├── README.md                                 # User documentation
├── DOCUMENTATION.md                          # Technical documentation
└── .gitignore                               # Version control exclusions
```

### 3.2 Core Components

#### 3.2.1 Airflow DAG (`dags/inventory_pipeline_dag.py`)

```python
"""
Retail Behavioral Analysis Pipeline DAG

This DAG orchestrates the complete data pipeline for retail behavioral analysis,
including Market Basket Analysis and RFM Customer Segmentation.
"""

from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 12),
    'retries': 1,
}

# DAG Definition
with DAG(
    dag_id='retail_behavioral_analysis_pipeline',
    default_args=default_args,
    description='A DAG to run Spark job for Customer Segmentation (RFM) and Market Basket Analysis (MBA).',
    schedule_interval='@daily',  # Run once every day
    catchup=False,
    tags=['retail', 'spark', 'behavioral-analysis'],
) as dag:
```

**Key Features**:
- Error handling with retries
- Daily scheduling (`@daily`)
- Spark Submit Operator with local mode
- PostgreSQL driver integration
- Resource management

**Spark Submit Configuration**:
```python
submit_spark_job = SparkSubmitOperator(
    task_id='submit_retail_analytics_spark_job',
    application='/opt/bitnami/spark/jobs/transform_job.py', 
    verbose=False,
    conf={'spark.master': 'local[*]'},
    packages='org.postgresql:postgresql:42.7.3'
)
```

#### 3.2.2 Spark Transformation Job (`spark_jobs/transform_job.py`)

**Import Management**:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, when, collect_set, datediff, 
    lit, max as _max, countDistinct, explode, first, size, ntile
)
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.window import Window
```

**Spark Session Configuration**:
```python
spark = SparkSession.builder \
    .appName("RetailBehavioralAnalytics") \
    .master("local[*]") \
    .getOrCreate()
```

#### 3.2.3 Airflow Container (`Dockerfile.airflow`)

**Base Image and Dependencies**:
```dockerfile
FROM apache/airflow:2.8.2

# Install Java for Spark provider
USER root
RUN apt-get update && apt-get install -y default-jre
USER airflow

# Install PySpark and Spark provider
RUN pip install --no-cache-dir pyspark==3.5.0 apache-airflow-providers-apache-spark
```

**Key Features**:
- Apache Airflow 2.8.2 base image
- Java runtime for Spark integration
- PySpark 3.5.0 and Apache Spark provider
- Optimized for retail analytics workloads

### 3.3 Function Specifications

#### 3.3.1 Market Basket Analysis Function

```python
def perform_market_basket_analysis(df):
    """
    Perform Market Basket Analysis using FPGrowth algorithm.
    
    Args:
        df (DataFrame): Input transaction data
        
    Returns:
        DataFrame: Association rules with product names, confidence and lift metrics
        
    Algorithm:
        1. Group transactions by Invoice
        2. Collect unique items per basket
        3. Filter baskets with minimum 2 items
        4. Apply FPGrowth with min_support=0.005, min_confidence=0.05
        5. Join with product lookup to get descriptions
        6. Calculate lift and filter significant rules
    """
```

**Technical Implementation**:
- **Algorithm**: FPGrowth (Frequent Pattern Growth)
- **Parameters**: min_support=0.005, min_confidence=0.05
- **Output**: Antecedent → Consequent rules with product names, confidence and lift

#### 3.3.2 RFM Analysis Function

```python
def perform_rfm_analysis(df):
    """
    Perform RFM (Recency, Frequency, Monetary) Customer Segmentation.
    
    Args:
        df (DataFrame): Input transaction data
        
    Returns:
        DataFrame: Customer segments with RFM scores
        
    Methodology:
        1. Calculate Recency: Days since last purchase
        2. Calculate Frequency: Number of unique transactions
        3. Calculate Monetary: Total purchase amount
        4. Score each dimension using quintiles (1-5)
        5. Assign customer segments based on RFM combination
    """
```

**Segmentation Logic**:
```python
def get_customer_segment(r_score, f_score):
    if r_score >= 4 and f_score >= 4:
        return "Champions"
    elif r_score >= 4 and f_score >= 2:
        return "Potential Loyalists"
    elif r_score >= 3 and f_score >= 3:
        return "Loyal Customers"
    elif r_score <= 2 and f_score >= 3:
        return "At Risk"
    elif r_score >= 3 and f_score <= 2:
        return "New Customers"
    else:
        return "Needs Attention"
```

---

## 4. Analytics Methodology

### 4.1 Market Basket Analysis

#### 4.1.1 Theoretical Foundation
Market Basket Analysis uses association rule mining to discover relationships between products frequently bought together.

**Key Metrics**:
- **Support**: P(A ∪ B) = Frequency of itemset / Total transactions
- **Confidence**: P(B|A) = P(A ∪ B) / P(A)
- **Lift**: P(B|A) / P(B) = Confidence / Expected Confidence

#### 4.1.2 Business Applications
1. **Product Placement**: Position frequently associated items near each other
2. **Cross-selling**: Recommend complementary products
3. **Promotion Strategy**: Bundle products with high lift values
4. **Inventory Management**: Stock associated items together

#### 4.1.3 Implementation Details

**Data Preprocessing**:
```python
# Create transaction baskets
baskets = df.groupBy("Invoice") \
    .agg(collect_set("StockCode").alias("items")) \
    .filter(size(col("items")) >= 2)

# Create product lookup for descriptions
product_lookup = cleaned_df.select("StockCode", "Description").distinct()
```

**Algorithm Configuration**:
- **Minimum Support**: 0.005 (items must appear in 0.5% of baskets)
- **Minimum Confidence**: 0.05 (5% confidence threshold)
- **Product Names**: Integration with product descriptions for better interpretability
- **Lift Threshold**: > 1.0 (positive association only)

**Product Names Integration**:
```python
# Join with product lookup to get antecedent and consequent names
mba_with_antecedent_names = association_rules.withColumn("antecedent_item", explode(col("antecedent"))) \
    .join(product_lookup, col("antecedent_item") == product_lookup.StockCode) \
    .groupBy(association_rules.columns) \
    .agg(collect_set("Description").alias("antecedent_names"))

mba_final_results = mba_with_antecedent_names.withColumn("consequent_item", explode(col("consequent"))) \
    .join(product_lookup, col("consequent_item") == product_lookup.StockCode) \
    .groupBy(mba_with_antecedent_names.columns) \
    .agg(collect_set("Description").alias("consequent_names"))
```

### 4.2 RFM Customer Segmentation

#### 4.2.1 RFM Framework
RFM analysis segments customers based on three behavioral dimensions:

1. **Recency (R)**: How recently did the customer make a purchase?
2. **Frequency (F)**: How often does the customer make purchases?
3. **Monetary (M)**: How much money does the customer spend?

#### 4.2.2 Scoring Methodology

**Quintile-Based Scoring**:
```python
# Calculate RFM scores using ntile function
r_window = Window.orderBy(col("Recency").desc())
f_window = Window.orderBy(col("Frequency").asc())
m_window = Window.orderBy(col("Monetary").asc())

rfm_scores = rfm_data.withColumn("R_Score", ntile(5).over(r_window)) \
                    .withColumn("F_Score", ntile(5).over(f_window)) \
                    .withColumn("M_Score", ntile(5).over(m_window))
```

#### 4.2.3 Customer Segments

| Segment | R Score | F Score | M Score | Characteristics |
|---------|---------|---------|---------|-----------------|
| Champions | 4-5 | 4-5 | - | Recent, frequent, high-value customers |
| Potential Loyalists | 4-5 | 2-3 | - | Recent customers, need engagement |
| Loyal Customers | 3-5 | 3-5 | - | Consistent customers with good value |
| At Risk | 1-2 | 3-5 | - | Previously valuable, now inactive |
| New Customers | 3-5 | 1-2 | - | Recent customers with potential |
| Needs Attention | - | - | - | All other customers requiring attention |

---

## 5. Database Schema

### 5.1 PostgreSQL Data Mart Schema

#### 5.1.1 Market Basket Analysis Table

```sql
CREATE TABLE market_basket_analysis (
    id SERIAL PRIMARY KEY,
    antecedent_names TEXT[],               -- Product descriptions in antecedent
    consequent_names TEXT[],               -- Product descriptions in consequent
    confidence DECIMAL(10,6),              -- Confidence P(B|A)
    lift DECIMAL(10,6),                    -- Lift metric
    support DECIMAL(10,6),                 -- Support of rule (A ∪ B)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_mba_confidence ON market_basket_analysis(confidence DESC);
CREATE INDEX idx_mba_lift ON market_basket_analysis(lift DESC);
CREATE INDEX idx_mba_support ON market_basket_analysis(support DESC);
```

#### 5.1.2 Customer Segmentation Table

```sql
CREATE TABLE customer_segmentation (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,          -- Customer identifier
    recency INTEGER,                       -- Days since last purchase
    frequency INTEGER,                     -- Number of transactions
    monetary DECIMAL(12,2),               -- Total purchase amount
    customer_segment VARCHAR(50),          -- Segment name
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(customer_id)
);

-- Indexes for analytics
CREATE INDEX idx_cs_segment ON customer_segmentation(customer_segment);
CREATE INDEX idx_cs_monetary ON customer_segmentation(monetary DESC);
```

### 5.2 Data Quality Constraints

#### 5.2.1 Business Rules Enforcement
```sql
-- Ensure positive metrics
ALTER TABLE customer_segmentation 
ADD CONSTRAINT chk_positive_values 
CHECK (recency >= 0 AND frequency > 0 AND monetary >= 0);

-- Ensure valid confidence and lift
ALTER TABLE market_basket_analysis 
ADD CONSTRAINT chk_mba_metrics 
CHECK (confidence >= 0 AND confidence <= 1 AND lift >= 0);
```

---

## 6. Configuration Management

### 6.1 Docker Compose Configuration

#### 6.1.1 Environment Variables
```yaml
environment:
  # Airflow Configuration
  AIRFLOW__CORE__EXECUTOR: LocalExecutor
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
  AIRFLOW__WEBSERVER__SECRET_KEY: GUdcCwzi8sz1hZ1cilD7RaNzfUxTP-FCNx1lSsPaG-c=
  
  # Spark Configuration  
  SPARK_MODE: master/worker
  SPARK_MASTER_URL: spark://spark-master:7077
  
  # PostgreSQL Configuration
  POSTGRES_USER: airflow
  POSTGRES_PASSWORD: airflow
  POSTGRES_DB: airflow
  
  # Metabase Configuration
  # Uses default Metabase configuration
```

#### 6.1.2 Volume Mounts
```yaml
volumes:
  # Code synchronization
  - ./dags:/opt/airflow/dags
  - ./spark_jobs:/opt/bitnami/spark/jobs
  - ./generated_data:/opt/bitnami/spark/data
  
  # Persistent storage
  - postgres_data:/var/lib/postgresql/data
```

### 6.2 Spark Configuration Tuning

#### 6.2.1 Memory Management
```python
spark_conf = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.executor.memory": "2g",
    "spark.driver.memory": "1g",
    "spark.sql.shuffle.partitions": "200"
}
```

#### 6.2.2 PostgreSQL Connector
```python
postgres_properties = {
    "user": "airflow",
    "password": "airflow", 
    "driver": "org.postgresql.Driver"
}
```

---

## 7. Development Guide

### 7.1 Local Development Setup

#### 7.1.1 Prerequisites
- Docker Desktop 4.0+
- Python 3.9+
- Git 2.0+
- 8GB RAM minimum
- 20GB free disk space

#### 7.1.2 Development Workflow

**Initial Setup**:
```bash
# Clone repository
git clone <repository-url>
cd final-project-inventory

# Generate sample data
python generate_data.py

# Start development environment
docker-compose up -d --build
```

**Development Cycle**:
1. **Code Changes**: Edit files in `dags/` or `spark_jobs/`
2. **Testing**: Test individual components
3. **Integration**: Run full pipeline
4. **Validation**: Check output in PostgreSQL and Metabase

#### 7.1.3 Debugging Procedures

**Airflow Debugging**:
```bash
# Check Airflow logs
docker logs airflow-webserver
docker logs airflow-scheduler

# Access Airflow container
docker exec -it airflow-webserver bash

# Check DAG parsing
python /opt/airflow/dags/inventory_pipeline_dag.py
```

**Spark Debugging**:
```bash
# Check Spark logs
docker logs spark-master
docker logs spark-worker

# Monitor Spark jobs
# Access Spark UI at http://localhost:8081

# Test Spark job locally
docker exec -it spark-master bash
/opt/bitnami/spark/bin/spark-submit /opt/bitnami/spark/jobs/transform_job.py
```
```

### 7.2 Testing Strategy

#### 7.2.1 Unit Testing
```python
# Example unit test for RFM calculation
def test_rfm_calculation():
    # Create test dataset
    test_data = [
        (1, "2024-01-01", 100.0),
        (1, "2024-01-15", 150.0),
        (2, "2024-01-20", 200.0)
    ]
    
    df = spark.createDataFrame(test_data, ["customer_id", "date", "amount"])
    result = calculate_rfm_metrics(df)
    
    assert result.count() == 2
    assert result.filter(col("customer_id") == 1).collect()[0]["frequency"] == 2
```

#### 7.2.2 Integration Testing
```python
# Test complete pipeline
def test_complete_pipeline():
    # Generate test data
    create_test_data()
    
    # Run pipeline
    execute_dag("retail_behavioral_analysis_pipeline")
    
    # Validate results
    validate_mba_results()
    validate_rfm_results()
```

### 7.3 Code Quality Standards

#### 7.3.1 Python Standards
- **PEP 8**: Code formatting and style
- **Type Hints**: Function parameter and return types
- **Docstrings**: Comprehensive function documentation
- **Error Handling**: Try-catch blocks for external dependencies

#### 7.3.2 Documentation Standards
- **Inline Comments**: Complex business logic explanation
- **Function Documentation**: Parameters, returns, and examples
- **Module Documentation**: Purpose and usage instructions

---

## 8. Performance Optimization

### 8.1 Spark Optimization Strategies

#### 8.1.1 Memory Optimization
```python
# Optimize DataFrame operations
df_optimized = df.persist(StorageLevel.MEMORY_AND_DISK_SER) \
                .coalesce(optimal_partitions) \
                .cache()

# Use broadcast joins for small datasets
broadcast_df = broadcast(small_df)
result = large_df.join(broadcast_df, "key")
```

#### 8.1.2 Query Optimization
```python
# Push down filters early
filtered_df = raw_df.filter(col("customer_id").isNotNull()) \
                   .filter(col("quantity") > 0) \
                   .filter(col("price") > 0)

# Use column pruning
selected_df = filtered_df.select("customer_id", "invoice_date", "amount")
```

### 8.2 Database Optimization

#### 8.2.1 Indexing Strategy
```sql
-- Composite indexes for common queries
CREATE INDEX idx_cs_segment_monetary 
ON customer_segmentation(customer_segment, monetary DESC);

-- Partial indexes for filtered queries
CREATE INDEX idx_mba_high_confidence 
ON market_basket_analysis(lift DESC) 
WHERE confidence > 0.5;
```

#### 8.2.2 Query Optimization
```sql
-- Optimized customer segment analysis
EXPLAIN ANALYZE
SELECT customer_segment, 
       COUNT(*) as customer_count,
       AVG(monetary) as avg_spend,
       SUM(monetary) as total_spend
FROM customer_segmentation 
WHERE customer_segment IN ('Champions', 'Loyal Customers')
GROUP BY customer_segment
ORDER BY total_spend DESC;
```

### 8.3 System Resource Monitoring

#### 8.3.1 Container Resource Usage
```bash
# Monitor resource usage
docker stats

# Check specific container resources
docker exec postgres_db pg_stat_activity

# Monitor Spark job performance
# Access Spark UI: http://localhost:8081
```

---

## 9. Monitoring and Logging

### 9.1 Airflow Monitoring

#### 9.1.1 DAG Monitoring
- **Task Success Rate**: Monitor task failure patterns
- **Execution Duration**: Track performance degradation
- **Resource Usage**: Monitor memory and CPU utilization
- **Data Quality**: Validate output row counts and schema

#### 9.1.2 Alerting Configuration
```python
# Email alerts for failures
default_args = {
    'email_on_failure': True,
    'email_on_retry': True,
    'email': ['admin@company.com']
}

# Slack notifications (if configured)
def send_slack_notification(context):
    slack_webhook = Variable.get("slack_webhook")
    # Send notification logic
```

### 9.2 Application Logging

#### 9.2.1 Structured Logging
```python
import logging

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def log_pipeline_metrics(stage, duration, record_count):
    logger.info(f"Pipeline Stage: {stage}, Duration: {duration}s, Records: {record_count}")
```

#### 9.2.2 Performance Metrics
```python
# Track key metrics
def track_performance():
    metrics = {
        "mba_rules_generated": mba_df.count(),
        "customers_segmented": rfm_df.count(),
        "processing_time": end_time - start_time,
        "data_quality_score": calculate_data_quality()
    }
    
    logger.info(f"Pipeline Metrics: {metrics}")
```

---

## 10. Security Considerations

### 10.1 Access Control

#### 10.1.1 Database Security
```sql
-- Create read-only user for analytics
CREATE USER analytics_reader WITH PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE airflow TO analytics_reader;
GRANT USAGE ON SCHEMA public TO analytics_reader;
GRANT SELECT ON market_basket_analysis, customer_segmentation TO analytics_reader;
```

#### 10.1.2 Airflow Security
```python
# Secure connections using Airflow Variables
db_conn = Variable.get("postgres_connection")
spark_conn = Variable.get("spark_connection")

# Use encrypted passwords
encrypted_password = Variable.get("db_password", deserialize_json=True)
```

### 10.2 Data Privacy

#### 10.2.1 Customer Data Protection
- **Anonymization**: Customer IDs are integer references, not PII
- **Aggregation**: Individual transaction details not exposed in final output
- **Retention**: Configure data retention policies

#### 10.2.2 Network Security
```yaml
# Docker network isolation
networks:
  data_network:
    driver: bridge
```

### 10.3 Backup and Recovery

#### 10.3.1 Database Backup Strategy
```bash
# Automated PostgreSQL backups
docker exec postgres_db pg_dump -U airflow airflow > backup_$(date +%Y%m%d).sql

# Backup restoration
docker exec -i postgres_db psql -U airflow airflow < backup_20240101.sql
```

#### 10.3.2 Disaster Recovery
- **Code Repository**: Git-based version control
- **Configuration Backup**: Environment variables and secrets backup
- **Data Recovery**: PostgreSQL point-in-time recovery capabilities

---

## Conclusion

This technical documentation provides comprehensive coverage of the retail data mart system architecture, implementation details, and operational procedures. The system demonstrates modern data engineering best practices with containerized microservices, automated orchestration, and robust analytics capabilities.

For additional support or questions, refer to the troubleshooting guide in the main README.md.

---

**Last Updated**: July 2025  
**Version**: 1.0  
**Maintainer**: Arif