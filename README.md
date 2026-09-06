# Olist Retail Data Pipeline - Databricks Lakehouse

## Project Overview
This project implements an end-to-end, automated Data Engineering pipeline using the Databricks Lakehouse Platform. It processes the Brazilian E-Commerce public dataset (Olist) through a modern Medallion Architecture, strictly adhering to DataOps and CI/CD best practices.

The infrastructure and orchestration are entirely managed as code (IaC) using Databricks Asset Bundles (DABs), enabling strict isolation between Development, Staging, and Production environments via Unity Catalog.

## Architecture & Tech Stack

### Technology Stack
* **Compute & Processing:** Databricks, Apache Spark (PySpark)
* **Data Flow & Quality:** Delta Live Tables (DLT), Databricks Auto Loader
* **Governance & Security:** Unity Catalog
* **Orchestration & IaC:** Databricks Asset Bundles (DABs), Databricks Workflows
* **CI/CD:** GitHub Actions, pytest, Black, Ruff

### Environment Isolation (Unity Catalog)
The project utilizes a 3-tier environment strategy, ensuring absolute data and execution isolation:
* **Dev:** Sandbox environment for active development (Branch: `develop`).
* **Staging:** Pre-production environment for integration testing and User Acceptance Testing (Branch: `staging`).
* **Prod:** Live environment serving downstream business intelligence tools (Branch: `main`).

## Data Pipeline Details (Medallion Architecture)

### 1. Bronze Layer (Raw Data Ingestion)
* **Mechanism:** Dynamic pipeline generation using Databricks Auto Loader (`cloudFiles`).
* **Functionality:** Ingests raw CSV files from the Unity Catalog Landing Zone into Delta format. The ingestion framework is fully dynamic, automatically looping through source directories to generate DLT tables without code duplication.

### 2. Silver Layer (Cleansing & Standardization)
* **Mechanism:** Delta Live Tables (Streaming Tables).
* **Functionality:** 
  * Data deduplication and type casting.
  * Enforcement of Data Quality rules using DLT Expectations (e.g., `@dlt.expect_or_drop` for invalid prices or missing IDs).
  * Standardization of string formats and timestamps.

### 3. Gold Layer (Business Aggregations & Star Schema)
* **Mechanism:** Delta Live Tables (Materialized Views).
* **Functionality:** 
  * Constructs a clean Star Schema optimized for BI tools (PowerBI, Tableau, Databricks SQL).
  * Creates Dimension tables (`dim_customers`, `dim_products`).
  * Creates Fact tables (`fact_sales`) by performing complex joins across cleaned orders, items, and payments.

## CI/CD & DataOps Workflow

This repository relies on a robust GitHub Actions workflow to ensure code quality and seamless deployment:

1. **Continuous Integration (CI):**
   * Code formatting check using `Black`.
   * Static code analysis and linting using `Ruff`.
   * Execution of local PySpark unit tests using `pytest` to validate transformation logic prior to deployment.
2. **Continuous Deployment (CD):**
   * Automated deployment to the Databricks workspace via Databricks Asset Bundles.
   * Dynamic target resolution based on the Git branch (`develop` -> `dev`, `staging` -> `staging`, `main` -> `prod`).
3. **Integration Testing:**
   * A post-deployment validation task runs on Databricks to assert the integrity of the Gold layer (checking for nulls, row counts, and business logic validity). The pipeline halts if validation fails.

## Repository Structure

```text
├── .github/workflows/   # CI/CD pipeline definitions
├── resources/           # Databricks Asset Bundles YAML configs (jobs, pipelines)
├── src/                 # PySpark and DLT source code
│   ├── bronze/          # Dynamic Auto Loader scripts
│   ├── silver/          # Cleansing and Data Quality rules
│   └── gold/            # Materialized Views and Star Schema definitions
├── tests/               # Local PySpark unit tests and remote integration tests
├── databricks.yml       # DABs project configuration and targets
├── requirements-dev.txt # Python dependencies for local testing and CI
└── pyproject.toml       # Linter and formatter configurations
<img width="1376" height="768" alt="enterprise_detailed_architecture_1788711632922" src="https://github.com/user-attachments/assets/a917da41-9e98-4ea8-8740-8f525e252519" />
