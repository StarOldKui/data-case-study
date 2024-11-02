# Project: Data Case Study

## Table of Contents

1. [Project Overview](#project-overview)
2. [Project Structure](#project-structure)
3. [Task 1: Building an ETL pipeline](#task-1-building-an-etl-pipeline)
    - 3.1 [Demo Video](#demo-video)
    - 3.2 [Architecture Diagram](#architecture-diagram)
    - 3.3 [Intermediate Data Models](#intermediate-data-models)
    - 3.4 [Design Choices and Considerations](#design-choices-and-considerations)

## Project Overview

Provide a brief introduction to the project, describing its purpose, key features, and any relevant background
information. Explain the importance of data processing and analysis for business insights.

TODO: 待补充

## Project Structure

```text
data-case-study/
│
├── data/                                   # Raw data files
│   └── raw/                                # Contains original CSV files
│       ├── accounts.csv
│       ├── invoice_line_items.csv
│       ├── invoices.csv
│       └── skus.csv
│
├── deliveries/                             # Project deliverables and completed tasks
│   └── task_1/                             # Task 1 files
│       ├── 1_cloudformation_template.yaml  # CloudFormation template for resources
│       ├── 2_upload_raw_data_to_s3.py      # Script to upload CSV files to S3
│       └── 3_data_case_study_etl_job.json  # Configuration for AWS Glue ETL job
│
└── docs/                                   # Documentation and instructions
    └── tasks/
        ├── README-task-one.md
        └── README-tasktwo.txt
TODO: 待补充
```

## Task 1: Building an ETL pipeline

### Demo Video

For a detailed walkthrough, [Please Watch This Demo Video](link_to_your_demo_video)

### Architecture Diagram

<img src="deliverables/task_1/diagrams/task_1_architecture.png" alt="Task 1 Architecture Diagram" width="800"/>

### Intermediate Data Models

- **Raw Data Model**: The initial dataset is stored in its raw form in the **data-case-study-raw-data** S3 bucket. This
  includes CSV files for **accounts**, **invoice line items**, **invoices**, and **skus** tables, as provided in the
  input dataset.

- **Processed Data Model**: After ETL transformations, the resulting features are stored in the
  **data-case-study-processed-data** S3 bucket. Two distinct levels of datasets are created in this process:
    - **Account-Level Aggregations**: Features aggregated by account ID to provide a summary of each account's
      activities.
    - **Account-Invoice-Level Aggregations**: Detailed features at the invoice level to capture data on individual
      invoice payments and related behaviors.

**Reasoning for Model Choice**: The separation into account-level and account-invoice-level models provides flexibility
for analysis. Account-level data supports high-level trend analysis across accounts, while invoice-level data allows for
a deeper exploration of individual transaction patterns, enhancing predictive modeling for invoice payment behaviors.

### Design Choices and Considerations

- **Feature Extraction**: Based on the requirement, the extracted features should predict how likely customers are to
  pay invoices on time. These features are split
  into account-level and account-invoice-level details:

    - **Account-Level Features**:
        - **feature_account_age_day** and **feature_account_age_year**: These features capture the duration a customer
          has been with the company, indicating account longevity. Longer relationships might suggest loyalty, while new
          accounts might show different payment behaviors.
        - **feature_customer_age_over_one_year**: This binary feature marks customers who have been active for over a
          year, distinguishing newer accounts from more established ones. Established accounts may exhibit different
          payment reliability.
        - **feature_total_invoices**: Total invoices associated with an account reflect transaction frequency and
          engagement level. High invoice frequency may correlate with consistent payment behavior.
        - **feature_avg_invoice_interval_day**: Average days between invoices indicate payment frequency. Shorter
          intervals may suggest regular payment habits, which could be predictive of timely payments.

    - **Account-Invoice-Level Feature**:
        - **feature_invoice_spending**: This feature calculates the total amount spent per invoice. Higher spending
          amounts might correlate with varying payment tendencies, such as delayed payments for larger invoices.

- **Choice of AWS Services**: To meet the requirement of using a big data framework, AWS Glue was chosen as it is built
  on Apache Spark, enabling scalable, distributed data processing. Key reasons for using AWS Glue and other AWS services
  include:

    - **AWS Glue ETL**: AWS Glue ETL jobs leverage **Spark** under the hood, offering the flexibility of distributed
      data processing with control over CPU and memory resources. Glue also supports **Job Bookmarks**, which track
      previously processed data to prevent reprocessing, optimizing for both performance and cost.

    - **AWS S3**: Used for storing raw and processed data, S3 provides a cost-effective and scalable storage solution.

    - **AWS Glue Crawler**: Automatically detects schema changes in raw data and catalogs them, simplifying data
      discovery and metadata management. This ensures the ETL pipeline remains adaptable to evolving data structures.

    - **AWS Glue Data Catalog**: Serves as a centralized metadata repository, making data assets accessible for querying
      by Athena and other services. This centralization enables seamless integration across the data pipeline.

    - **AWS Athena**: Allows querying of processed data directly from S3 using SQL, supporting ad hoc and analytical
      queries on the transformed dataset without the need for a dedicated database, thus lowering infrastructure costs.

- **Scaling**: As data volume grows, Glue’s Spark-based architecture can scale horizontally by increasing the number of
  Data Processing Units (DPUs).

- **Scheduling**: AWS Glue ETL jobs and Crawlers can be set to run on a scheduled basis (e.g. daily, hourly).

- **Partitioned Data Storage**: The current ETL job design stores processed data based on the job's run date as
  partitions in S3, structured by `ingest_year`, `ingest_month`, and `ingest_day`. This structure facilitates efficient
  querying and data
  organization. By enabling the **Job Bookmark** feature in AWS Glue, the job ensures that only new, unprocessed data is
  handled during each run, preventing duplication and optimizing resource use.

- **Error Handling and Monitoring**: AWS Glue provides built-in job monitoring and logging via CloudWatch, enabling
  tracking of job performance, debugging, and alerts for any failures.

- **Security and Compliance**: By using AWS IAM roles and policies, access to S3 and Glue resources is tightly
  controlled. Encryption options are also available to secure data at rest in S3 and in transit during Glue job
  execution.

- **Cost Optimization**: Glue auto-scaling capabilities help to optimize resource usage and cost by automatically
  adjusting the required DPUs based on workload, preventing over-provisioning and minimizing costs.

- **Data Consistency and Schema Evolution**: Glue Crawlers dynamically detect changes in data schema and update the Data
  Catalog.

## Task 2: System Design

### Architecture Diagram

...
