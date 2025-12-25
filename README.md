# Enterprise IoT BigData Pipeline

> End-to-end data pipeline processing 1.8M records/hour from edge devices to analytics dashboard.

![banner](https://github.com/FauzanAcyuto/iot-bigdata-streamlit-dashboard/blob/master/media/banner.png)

## Impact

1. Increased data delivery from 30% -> 80% (ongoing project to 100%)
2. Increased pipeline availability from 40% -> 90% (in network constrained areas in Borneo)
3. Increased data read performance from 15min+ -> 2 seconds (partition improvement + cloud migration)

## 📁 Project Structure

| Component | Description | Tech Stack | Status |
|-----------|-------------|------------|--------|
| [v1-basic-etl](./v1-basic-etl) | Initial pipeline with limitations | Python, Polars, DuckDB | ✅ Complete |
| [v2-orchestrated-pipeline](./v2-orchestrated-pipeline) | Production-grade with orchestration | Airflow, dbt, Docker |  In Progress |
| [streamlit-dashboard](./streamlit-dashboard) | Operational analytics UI | Streamlit, Plotly | ✅ Complete |
| [docs](./docs) | Architecture & design decisions | Markdown, diagrams | ✅ Complete |



