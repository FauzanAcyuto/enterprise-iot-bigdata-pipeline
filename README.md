# Enterprise IoT BigData Pipeline

> End-to-end data pipeline processing 1.8M records/hour from edge devices to analytics dashboard.

![banner](https://github.com/FauzanAcyuto/enterprise-iot-bigdata-pipeline/blob/master/media/banner.png)

## Overview

At pamapersada, the largest mining contractor in Indonesia, constant RnD is always happening to develop solutions for complex mining requirements. One of the most complex is safety and productivity. For this my team and I developed *Mata Hati 02* (The Hearts Eyes) which are Edge computing IoT Devices installed in heavy machinery such as: Dump Trucks, Coal Trucks,  Busses, Water Trucks, and many more.

Each of these devices monitor everything from engine pressure to the drivers eyelid aspect ratio (for fatigue detection). Needless to say this system required some insane solution for its infrastructure.

We used to rely on a data streaming pipeline of MQTT -> mongoDB -> pentaho -> hive cluster. Which was great for streaming the amount of data from the number of machinery we used to have (-+100 units). But as time went on that number grew to (500+) units and their working area spread out deeper into the jungles of borneo.

This caused availability issues with network outages that broke the pipeline in various places, resulting in an abysmal availability number of 40%. Plus with lacking network coverage, we experienced massive data losses from prolonged MQTT disconnection that passed the configured buffer in the devices only achieving 30% data delivery fulfillment.

This prompted the project that I am showcasing, I coordinated with stakeholders to make architectural decision trade-offs, architected the system, lead two developers, and built the code you see here.

## Impact

- Increased data delivery from 30% -> 80% (ongoing project to 100%)
- Increased pipeline availability from 40% -> 90% (in network constrained areas in Borneo)
- Increased data read performance from 15min+ -> 2 seconds (partition improvement + cloud migration)

## Highlights

- **Zero-copy architecture** — Polars + DuckDB for memory-efficient processing
- **Airflow orchestration** — Scheduled, monitored, with alerting
- **dbt transforms** — Data quality tests and auto-generated documentation
- **Evolution story** — From basic script to production pipeline

## Architecture

![architecture](https://github.com/FauzanAcyuto/enterprise-iot-bigdata-pipeline/blob/master/media/detailed-architecture.png)

## 📁 Project Structure

| Component | Description | Tech Stack | Status |
|-----------|-------------|------------|--------|
| [v-basic-etl](./v1-basic-etl/README-compacterv1.md) | Initial pipeline with limitations | Python, Polars, DuckDB | ✅ Complete |
| [v2-orchestrated-pipeline](./v2-orchestrated-pipeline) | Production-grade with orchestration | Airflow, dbt, Docker |  In Progress |
| [streamlit-dashboard](./streamlit-dashboard) | Operational analytics UI | Streamlit, Plotly | ✅ Complete |
| [docs](./docs) | Architecture & design decisions | Markdown, diagrams | ✅ Complete |
