# Architecture Decision Log

This document records the architectural decisions made for the data engineering capstone project.

---

## 1. Ingestion Layer

### 1.1 Decision: Kafka

**Date**: 2025-09-29  
**Status**: Proposed

**Context**:
The system needs to ingest real-time or near–real-time event data (e.g., customer events, transactions, logs) in a scalable and fault-tolerant way. The ingestion layer must decouple producers from downstream consumers and support streaming pipelines.

**Decision**:
Use Apache Kafka as the primary event streaming and ingestion platform.

**Rationale**:
- Kafka provides durable, distributed, and high-throughput message streaming.
- It allows multiple consumers (Snowflake, Trino, analytics jobs) to independently consume the same data.
- Kafka is an industry-standard tool widely used in modern data engineering architectures.

**Alternatives Considered**:
- AWS Kinesis (cloud-specific, vendor lock-in)
- RabbitMQ (better for task queues, weaker for streaming analytics)
- Direct API ingestion into databases (tight coupling, poor scalability)

**Consequences**:
- **Pros**:
  - Scales horizontally
  - Guarantees message durability and ordering per partition
  - Strong ecosystem (Kafka Connect, Schema Registry)
- **Cons**:
  - Operational complexity
  - Requires careful configuration and monitoring

**Example**:
Stream customer activity events into Kafka topics, which are then consumed by downstream services for storage in Snowflake and real-time analytics.

---

### 1.2 Decision: ZooKeeper

**Date**: 2025-09-29  
**Status**: Proposed

**Context**:
Kafka requires a coordination service to manage broker metadata, leader election, and cluster state.

**Decision**:
Use Apache ZooKeeper to coordinate the Kafka cluster.

**Rationale**:
- ZooKeeper is the traditional and stable coordination layer for Kafka deployments.
- It ensures consistency, leader election, and fault tolerance in distributed systems.

**Alternatives Considered**:
- Kafka KRaft mode (newer, less familiar for academic projects)
- Custom coordination mechanisms (high risk, unnecessary complexity)

**Consequences**:
- **Pros**:
  - Mature and well-documented
  - Reliable for distributed coordination
- **Cons**:
  - Additional component to manage
  - Operational overhead

**Example**:
ZooKeeper manages Kafka broker registrations and topic partition leadership during failures or restarts.

---

## 2. Orchestration Layer

### 2.1 Decision: Prefect v3

**Date**: 2025-09-29  
**Status**: Proposed

**Context**:
The project requires orchestration of batch and streaming workflows, including ingestion, transformation, validation, and loading tasks. Workflows must be observable, retryable, and schedulable.

**Decision**:
Use Prefect v3 as the workflow orchestration tool.

**Rationale**:
- Prefect offers a Python-native orchestration experience with minimal boilerplate.
- It integrates well with data engineering tools (Snowflake, Kafka, Python ETL).
- Prefect v3 provides improved performance, simpler deployment, and better developer ergonomics.

**Alternatives Considered**:
- Apache Airflow (heavier setup, steeper learning curve)
- Dagster (strong but more opinionated)
- Cron jobs (no observability or retries)

**Consequences**:
- **Pros**:
  - Easy to write and maintain
  - Strong observability and retries
  - Lightweight for local and academic setups
- **Cons**:
  - Smaller ecosystem than Airflow
  - Some advanced features require Prefect Cloud

**Example**:
Schedule a daily workflow that pulls data from Kafka, stages it in MinIO, and triggers DBT transformations in Snowflake.

---

## 3. Transformation Layer

### 3.1 Decision: dbt (Data Build Tool)

**Date**: 2025-09-29  
**Status**: Proposed

**Context**:
The project requires SQL-based data transformations to build a dimensional data warehouse from raw data. Transformations must be version-controlled, testable, and maintainable across multiple environments (Trino and Snowflake).

**Decision**:
Use dbt (Data Build Tool) for SQL-based data transformations and modeling.

**Rationale**:
- dbt provides a modern approach to data transformation with version control and testing.
- It supports multiple data warehouses (Snowflake, Trino) through adapters.
- Enables modular, reusable SQL models with Jinja templating.
- Integrates seamlessly with Prefect for orchestrated transformation pipelines.
- Strong ecosystem with packages (dbt-utils) and community support.

**Alternatives Considered**:
- Custom SQL scripts (no versioning, testing, or modularity)
- Airflow DAGs with SQL operators (less structured, harder to maintain)
- Spark SQL (heavier compute requirements, more complex setup)

**Consequences**:
- **Pros**:
  - Version-controlled transformations
  - Built-in testing and documentation
  - Modular and reusable models
  - Strong community and ecosystem
  - Easy integration with orchestration tools
- **Cons**:
  - SQL-only (no Python transformations)
  - Requires understanding of dbt concepts
  - Model dependency management can be complex

**Example**:
Transform raw customer and transaction data from Kafka into dimensional models (fact_transactions, dim_customers) using dbt models, with incremental loads and data quality tests.

---

### 3.2 Decision: SQLFluff

**Date**: 2025-09-29  
**Status**: Proposed

**Context**:
SQL code quality and consistency are critical for maintainability. The project needs automated SQL linting and formatting to ensure code standards across dbt models.

**Decision**:
Use SQLFluff as the SQL linter and formatter for dbt models.

**Rationale**:
- SQLFluff provides comprehensive SQL linting rules and formatting.
- Supports dbt templating through sqlfluff-templater-dbt.
- Integrates with pre-commit hooks for automated code quality checks.
- Enforces consistent SQL style across the codebase.

**Alternatives Considered**:
- Manual code reviews (time-consuming, inconsistent)
- Custom linting scripts (high maintenance)
- No linting (poor code quality and consistency)

**Consequences**:
- **Pros**:
  - Automated code quality checks
  - Consistent SQL formatting
  - Catches common SQL errors early
  - Integrates with CI/CD pipelines
- **Cons**:
  - Requires configuration tuning
  - Can be strict on existing codebases
  - Additional dependency to maintain

**Example**:
Automatically format and lint all dbt SQL models before commits, ensuring consistent indentation, naming conventions, and catching syntax errors.

---

### 3.3 Decision: dbt-score

**Date**: 2025-09-29  
**Status**: Proposed

**Context**:
Data quality and model quality metrics are essential for maintaining a reliable data warehouse. The project needs automated scoring of dbt models based on best practices and quality metrics.

**Decision**:
Use dbt-score to evaluate and score dbt models for quality and best practices.

**Rationale**:
- dbt-score provides automated quality scoring for dbt projects.
- Evaluates models based on documentation, tests, naming conventions, and best practices.
- Configurable thresholds for project-wide and individual model scores.
- Helps maintain high-quality data models over time.

**Alternatives Considered**:
- Manual quality reviews (subjective, time-consuming)
- Custom scoring scripts (high development effort)
- No quality scoring (risk of technical debt)

**Consequences**:
- **Pros**:
  - Automated quality assessment
  - Encourages best practices
  - Configurable quality thresholds
  - Integrates with CI/CD pipelines
- **Cons**:
  - Requires initial configuration
  - May flag existing models that need refactoring
  - Additional dependency

**Example**:
Run dbt-score as part of CI/CD to ensure all dbt models meet quality thresholds (e.g., minimum score of 7.5 for project, 8.0 for individual models) before deployment.

---

## 4. Observability Layer

### 4.1 Decision: PgAdmin

**Date**: 2025-09-29  
**Status**: Proposed

**Context**:
The system uses PostgreSQL for metadata, orchestration state, or application persistence. Developers need a UI for inspection, debugging, and manual queries.

**Decision**:
Use PgAdmin as the PostgreSQL administration and monitoring tool.

**Rationale**:
- PgAdmin provides a user-friendly web interface for managing PostgreSQL databases.
- It simplifies schema inspection, query execution, and troubleshooting.

**Alternatives Considered**:
- psql CLI (less accessible for beginners)
- DBeaver (heavier, generic DB tool)

**Consequences**:
- **Pros**:
  - Easy database exploration
  - Good for debugging and learning
- **Cons**:
  - Not suitable for production monitoring
  - UI can be slow with large schemas

**Example**:
Inspect Prefect metadata tables and verify workflow execution state stored in PostgreSQL.

---

### 4.2 Decision: Prometheus

**Date**: 2025-09-29  
**Status**: Proposed

**Context**:
The platform needs system and application-level metrics for monitoring performance, failures, and resource usage.

**Decision**:
Use Prometheus for metrics collection and monitoring.

**Rationale**:
- Prometheus is a de-facto standard for metrics monitoring in cloud-native systems.
- It supports pull-based metrics, time-series storage, and strong integrations.

**Alternatives Considered**:
- Datadog (commercial, cost)
- CloudWatch (cloud-specific)
- Custom logging scripts (limited insight)

**Consequences**:
- **Pros**:
  - Open-source and reliable
  - Strong Kubernetes and service integrations
- **Cons**:
  - Requires manual dashboard setup
  - Limited long-term storage without extensions

**Example**:
Collect Kafka broker metrics, Prefect task execution times, and system CPU/memory usage.

---

### 4.3 Decision: Grafana

**Date**: 2025-09-29  
**Status**: Proposed

**Context**:
Metrics collected by Prometheus need to be visualized for monitoring and troubleshooting.

**Decision**:
Use Grafana for metrics visualization and dashboards.

**Rationale**:
- Grafana provides powerful, customizable dashboards and integrates seamlessly with Prometheus.
- It is widely used in production-grade observability stacks.

**Alternatives Considered**:
- Kibana (log-focused)
- Custom dashboards (high effort)

**Consequences**:
- **Pros**:
  - Rich visualization options
  - Alerting support
  - Easy integration with Prometheus
- **Cons**:
  - Requires dashboard configuration
  - Learning curve for advanced queries

**Example**:
Build dashboards showing Kafka lag, Prefect workflow success rates, and Snowflake query performance.

---

## 5. Storage Layer

### 5.1 Decision: Snowflake

**Date**: 2025-09-29  
**Status**: Proposed

**Context**:
The project requires a scalable, cloud-based data warehouse for analytics, reporting, and SQL-based transformations.

**Decision**:
Use Snowflake as the primary analytical data warehouse.

**Rationale**:
- Snowflake separates compute and storage, enabling cost-efficient analytics.
- It integrates well with dbt, Kafka, and Python-based tools.

**Alternatives Considered**:
- BigQuery (GCP lock-in)
- Redshift (more operational overhead)
- PostgreSQL (limited analytical scalability)

**Consequences**:
- **Pros**:
  - High performance
  - Minimal infrastructure management
  - Excellent SQL support
- **Cons**:
  - Usage-based costs
  - Requires cloud connectivity

**Example**:
Store transformed fact and dimension tables for analytics and dashboarding.

---

### 5.2 Decision: Trino

**Date**: 2025-09-29  
**Status**: Proposed

**Context**:
The system needs federated querying across multiple data sources (Snowflake, PostgreSQL, MinIO).

**Decision**:
Use Trino as a distributed SQL query engine.

**Rationale**:
- Trino allows querying data across heterogeneous systems using a single SQL interface.
- It is well-suited for ad-hoc analytics and exploration.

**Alternatives Considered**:
- PrestoDB (older, less active)
- Spark SQL (heavier compute requirements)

**Consequences**:
- **Pros**:
  - Fast federated queries
  - No data movement required
- **Cons**:
  - Requires cluster management
  - Not designed for heavy ETL

**Example**:
Run SQL queries joining Snowflake tables with raw files stored in MinIO.

---

### 5.3 Decision: PostgreSQL

**Date**: 2025-09-29  
**Status**: Proposed

**Context**:
A relational database is needed for metadata storage, orchestration state, and application persistence.

**Decision**:
Use PostgreSQL as the primary transactional database.

**Rationale**:
- PostgreSQL is reliable, open-source, and widely supported.
- It works well with Prefect, PgAdmin, and Python applications.

**Alternatives Considered**:
- MySQL (less advanced SQL features)
- SQLite (not suitable for concurrent workloads)

**Consequences**:
- **Pros**:
  - ACID-compliant
  - Strong ecosystem
- **Cons**:
  - Requires tuning for high concurrency
  - Not an analytical database

**Example**:
Store workflow metadata, application state, and user session data.

---

### 5.4 Decision: MinIO

**Date**: 2025-09-29  
**Status**: Proposed

**Context**:
We need a reliable, S3-compatible storage solution to act as a data lake for staging and storing raw datasets, including CSVs and JSONs, before ETL/ELT processes.

**Decision**:
Use MinIO as the on-premise object storage solution.

**Rationale**:
- S3-compatible API works with Python, Snowflake, and dbt.
- Lightweight, easy to deploy locally or in Docker.
- Supports versioning and large datasets.

**Alternatives Considered**:
- AWS S3 (cloud-dependent, not fully on-premise)
- HDFS (heavier, more complex to manage)
- Snowflake (not suitable for raw file storage)

**Consequences**:
- **Pros**:
  - Fast local deployments
  - Free and open-source
- **Cons**:
  - Limited enterprise features compared to AWS S3
  - Requires maintenance

**Example**:
Store raw CSV/JSON files from crypto APIs and batch datasets, then read them for dbt transformations and Snowflake loading.

---

### 5.5 Decision: Pinecone

**Date**: 2025-09-29  
**Status**: Proposed

**Context**:
The AI chatbot application requires a vector database for storing document embeddings to enable semantic search and Retrieval-Augmented Generation (RAG) capabilities.

**Decision**:
Use Pinecone as the vector database for document embeddings and semantic search.

**Rationale**:
- Pinecone provides managed vector database services with high performance.
- Integrates seamlessly with LangChain and OpenAI embeddings.
- Supports namespaces for organizing different document sets.
- Serverless architecture reduces operational overhead.

**Alternatives Considered**:
- Chroma (local-only, requires infrastructure)
- Weaviate (more complex setup)
- PostgreSQL with pgvector (less optimized for vector search)
- FAISS (in-memory, not persistent)

**Consequences**:
- **Pros**:
  - Managed service with high performance
  - Easy integration with LangChain
  - Supports large-scale vector operations
  - Serverless architecture
- **Cons**:
  - Requires API key and cloud connectivity
  - Usage-based pricing
  - Vendor dependency

**Example**:
Store PDF document chunks as embeddings in Pinecone, enabling the chatbot to retrieve relevant context for answering questions about policies, documentation, or historical data.

---

## 6. Application Layer

### 6.1 Decision: LangGraph

**Date**: 2025-09-29  
**Status**: Proposed

**Context**:
The project includes an AI-powered chatbot that must handle multi-step reasoning, tool usage, and conversation memory.

**Decision**:
Use LangGraph to build the AI agent workflow.

**Rationale**:
- LangGraph enables graph-based control over LLM reasoning and tool execution.
- It supports complex flows such as database querying, OCR, and context retention.
- Provides explicit state management for conversational agents.

**Alternatives Considered**:
- Plain LangChain (less control over state)
- Custom Python logic (harder to maintain)

**Consequences**:
- **Pros**:
  - Explicit state management
  - Supports tool-based RAG workflows
  - Better control over agent behavior
- **Cons**:
  - Newer framework
  - Requires understanding graph-based logic

**Example**:
An AI agent that receives a user question, queries Snowflake, retrieves documents from Pinecone, and returns results with conversational context.

---

### 6.2 Decision: Streamlit

**Date**: 2025-09-29  
**Status**: Proposed

**Context**:
The project needs a simple web interface to interact with the AI chatbot and visualize results.

**Decision**:
Use Streamlit for the application frontend.

**Rationale**:
- Streamlit allows rapid development of interactive data applications using Python.
- It integrates easily with LangGraph and backend services.

**Alternatives Considered**:
- React (higher development effort)
- Flask + HTML (more boilerplate)

**Consequences**:
- **Pros**:
  - Fast development
  - Python-native
  - Ideal for demos and capstone projects
- **Cons**:
  - Limited UI customization
  - Not suited for large-scale production apps

**Example**:
A web UI with a chat interface and image upload button that sends queries to the AI agent.

---

### 6.3 Decision: Tesseract OCR

**Date**: 2025-09-29  
**Status**: Proposed

**Context**:
The AI chatbot needs to extract text from images, particularly ID cards and documents, to enable automated customer identification and data extraction workflows.

**Decision**:
Use Tesseract OCR (via pytesseract) for optical character recognition and image-to-text extraction.

**Rationale**:
- Tesseract is a mature, open-source OCR engine with strong accuracy.
- pytesseract provides Python bindings for easy integration.
- Supports preprocessing with OpenCV for improved accuracy.
- Well-documented and widely used in production systems.

**Alternatives Considered**:
- Google Cloud Vision API (cloud-dependent, cost)
- OpenAI Vision (cloud-dependent, cost)
- AWS Textract (vendor lock-in, cost)
- EasyOCR (less mature, different accuracy characteristics)
- Custom OCR solutions (high development effort)

**Consequences**:
- **Pros**:
  - Open-source and free
  - Good accuracy with preprocessing
  - Python integration via pytesseract
  - Supports multiple image formats
- **Cons**:
  - Requires system-level installation
  - Accuracy depends on image quality
  - May need preprocessing for optimal results

**Example**:
Extract customer names and IDs from uploaded ID card images, then query Snowflake for customer transaction history using the extracted identifiers.

---

## Summary

This decision log documents the architectural choices made for the data engineering capstone project, covering:

- **Ingestion**: Kafka and ZooKeeper for event streaming
- **Orchestration**: Prefect v3 for workflow management
- **Transformation**: dbt, SQLFluff, and dbt-score for SQL transformations and quality
- **Observability**: PgAdmin, Prometheus, and Grafana for monitoring
- **Storage**: Snowflake, Trino, PostgreSQL, MinIO, and Pinecone for various data needs
- **Application**: LangGraph, Streamlit, and Tesseract OCR for the AI chatbot interface

Each decision includes context, rationale, alternatives considered, consequences, and examples of usage.
