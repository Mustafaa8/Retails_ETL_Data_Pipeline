# 🛒 Retail Data ETL Pipeline with Kestra

This repository contains an end-to-end **ETL (Extract, Transform, Load)** pipeline designed to process **retail sales data** using [**Kestra**](https://kestra.io) for workflow orchestration.

The pipeline extracts raw retail data from source files, transforms it using Python scripts (e.g., Pandas), and loads the results into a structured format or destination system.

---

## 🧩 Overview

This project demonstrates how to:

- Orchestrate ETL workflows using **Kestra**
- Extract data from CSV file
- Transform data using Python and Pandas
- Load processed data into database
- Run everything in a containerized environment via Docker

---

## 📁 Project Structure

```
retail-etl-pipeline/
├── flows/                   # Kestra workflow definitions (YAML)
├── scripts/                 # Python scripts for transformation logic
├── data/                    # Input and output data files
├── docker-compose.yml       # Docker configuration for local development
└── README.md                # This file
```

---

## ⚙️ Pipeline Structure and Technologies Used

![Pipeline Diagram](https://github.com/user-attachments/assets/3d4f5fb3-ac29-4b87-8c00-8bd0ad6af915)

| Tool        | Purpose                         |
|-------------|----------------------------------|
| Kestra      | Workflow orchestration           |
| Pandas      | Data transformation              |
| Python      | Scripting language               |
| Docker      | Containerization                 |
| Docker Compose | Multi-container orchestration |

---

## ▶️ Getting Started

### Prerequisites

Ensure you have the following installed:

- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)

No need to install Python or Kestra separately — everything runs inside containers.

---

## 📋 Sample Workflow

A basic ETL workflow includes:

![ETL Flow](https://github.com/user-attachments/assets/4564b1d9-6252-4d8b-a2e0-002bbea9b358)

1. **Extract**: Read raw sales data from CSV
2. **Transform**: Clean and aggregate data using Pandas (`scripts/transform.py`)
3. **Load**: Save transformed output in dimensional model in the database

## 🛠️ Customization

You can easily extend this pipeline by:

- Adding more transformation steps in `scripts/`
- Modifying the flow to include databases (PostgreSQL, MySQL), object storage (S3), or cloud platforms
- Enhancing validation, error handling, and alerting in Kestra

---

## 📊 Dashboard

Dashboard was made using Power BI including some important insights: 
- Competitors price analysis
- Category Dominance
- Month Sales
- Summary Stats

![Dashboard](Power\ BI\ Dashboard.PNG)

---

## 📝 License

This project is licensed under the **MIT License** – see the [LICENSE](LICENSE) file for details.

