# Enterprise IoT BigData Pipeline

> End-to-end data pipeline processing 1.8M records/hour from edge devices to analytics dashboard.

![banner](https://github.com/FauzanAcyuto/enterprise-iot-bigdata-pipeline/blob/master/media/banner.png)

## 📖Overview

At pamapersada, the largest mining contractor in Indonesia, constant RnD is always happening to develop solutions for complex mining requirements. One of the most complex is safety and productivity. For this my team and I developed *Mata Hati 02* (The Hearts Eyes) which are Edge computing IoT Devices installed in heavy machinery such as: Dump Trucks, Coal Trucks,  Busses, Water Trucks, and many more.

Each of these devices monitor everything from engine pressure to the drivers eyelid aspect ratio (for fatigue detection). Needless to say this system required some insane solution for its infrastructure.

We used to rely on a data streaming pipeline of MQTT -> mongoDB -> pentaho -> hive cluster. Which was great for streaming the amount of data from the number of machinery we used to have (-+100 units). But as time went on that number grew to (500+) units and their working area spread out deeper into the jungles of borneo.

This caused availability issues with network outages that broke the pipeline in various places, resulting in an abysmal availability number of 40%. Plus with lacking network coverage, we experienced massive data losses from prolonged MQTT disconnection that passed the configured buffer in the devices only achieving 30% data delivery fulfillment.

This prompted the project that I am showcasing, I coordinated with stakeholders to make architectural decision trade-offs, architected the system, lead two developers, and built the code you see here.

## 🔥Impact

- Increased data delivery from 30% -> 80% (ongoing project to 100%)
- Increased pipeline availability from 40% -> 90% (in network constrained areas in Borneo)
- Increased data read performance from 15min+ -> 2 seconds (partition improvement + cloud migration)

## 💡Highlights

- **Zero-copy architecture** — Polars + DuckDB for memory-efficient processing
- **Airflow orchestration** — Scheduled, monitored, with alerting
- **dbt transforms** — Data quality tests and auto-generated documentation
- **Evolution story** — From basic script to production pipeline

## 🏗️Architecture

![architecture](https://github.com/FauzanAcyuto/enterprise-iot-bigdata-pipeline/blob/master/media/detailed-architecture.png)

## 📁 Project Structure

| Component | Description | Tech Stack | Status |
|-----------|-------------|------------|--------|
| [v1-basic-etl](./v1-basic-etl/README-compacterv1.md) | Initial pipeline with limitations | Python, Polars, DuckDB | ✅ Complete |
| [v2-orchestrated-pipeline](./v2-orchestrated-pipeline) | Production-grade with orchestration | Airflow, dbt, Docker |  In Progress |
| [streamlit-dashboard](./streamlit-dashboard) | Operational analytics UI | Streamlit, Plotly | ✅ Complete |
| [docs](./docs) | Architecture & design decisions | Markdown, diagrams | ✅ Complete |

## 🚀 Project Evolution

This project evolved from a quick script to a production-ready system. The journey is documented to show engineering maturity and iterative improvement.

### V1: Basic ETL

The initial implementation that got the job done, but had limitations:

- ✅ Functional TXT → Parquet conversion
- ✅ Handles 1.8M records/hour
- ❌ Manual scheduling in script
- ❌ No data validation
- ❌ 24 small files per partition (query performance hit)

### V2: Production Pipeline

Enterprise-grade solution addressing all V1 limitations:

- ✅ Airflow DAGs (scheduled, monitored, alerting)
- ✅ dbt transforms + data quality tests
- ✅ Automatic daily compaction (24 files → 1)
- ✅ Malformed file handling + notifications
- ✅ Timezone-agnostic (UTC internal, convert at display)
- ✅ Zero-copy architecture preserved

| Decision | Why | Tradeoff |
|----------|-----|----------|
| **Batch upload over MQTT** | Prioritize throughput for analytics workloads | Higher latency, but 99% data availability during outages |
| **Zero-copy (Polars + DuckDB)** | Process larger-than-memory data on suboptimal hardware | Requires understanding lazy evaluation patterns |
| **UTC internal timestamps** | Server-agnostic processing | Display timezone conversion at presentation layer |
| **Hourly Parquet + daily compaction** | Balance data freshness vs query performance | Two-stage pipeline adds complexity |
| **Database-based state tracking** | Know what's processed vs pending | Database connection to maintain |

## 🔧 Tech Stack

**Processing:**
![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Polars](https://img.shields.io/badge/Polars-CD792C?style=flat-square&logo=polars&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=flat-square&logo=duckdb&logoColor=black)

**Orchestration & Transforms:**
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat-square&logo=dbt&logoColor=white)

**Infrastructure:**
![AWS S3](https://img.shields.io/badge/S3-569A31?style=flat-square&logo=amazons3&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)

**Visualization:**
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)

## 🤝 About This Project

This is a portfolio project demonstrating production data engineering practices. It's based on real-world experience managing 500+ IoT edge devices, adapted for public sharing.

**Built as part of my transition to the Australian data market.**

---

## 👤 Author

**Fauzan Acyuto**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/fauzan-actyuto)
[![Email](https://img.shields.io/badge/Email-EA4335?style=for-the-badge&logo=gmail&logoColor=white)](mailto:acyuto.professional@gmail.com)
