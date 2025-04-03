# COVID-19 ETL Pipeline

## Overview
This project implements an end-to-end ETL (Extract, Transform, Load) pipeline for COVID-19 data from the World Health Organization (WHO). The pipeline extracts raw COVID-19 datasets, transforms them through a series of cleaning steps, and loads the processed data into a data warehouse for analysis and visualization.

![Architecture Diagram](/covid%20etl%20diagram.drawio.png)

## Tech Stack

| Component | Technology |
|-----------|------------|
| Infrastructure | Docker |
| Data Warehouse | PostgreSQL |
| Orchestration | Apache Airflow |
| Data Processing | Python (Pandas) |
| Visualization | PowerBI |

## Data Source
The data is sourced from the World Health Organization (WHO), which provides weekly updates of COVID-19 statistics including case counts and mortality data across different countries.

## Data Cleaning Methodology
The following steps address missing values in key columns (New cases, New deaths, Country_code):

1. **Zero Equivalence**: Missing values corresponding to zero in cumulative columns are filled with 0
2. **Differential Calculation**: For each country, remaining missing values are calculated from differences in cumulative columns
3. **Initial Value Treatment**: First values (which become NaN after differential calculations) are filled with the corresponding cumulative value
4. **Negative Value Handling**: Negative values resulting from adjustments in cumulative data are treated as corrections by health authorities and set to 0
5. **Country Code Completion**: Missing Country_code values for Namibia are filled with "NA"

## Data Warehouse Model
The data warehouse implements a dimensional model using Type 1 slowly changing dimensions.

![Schema Diagram](schema%20Diagram.png)

## Dashboard
The transformed data powers analytical dashboards in PowerBI:

![Dashboard Preview](covid%20dashboard.PNG)

## Getting Started

### Prerequisites
- Docker
- Docker Compose

### Installation & Setup

1. Clone this repository
   ```
   git clone [repository-url]
   ```

2. Navigate to the project directory
   ```
   cd covid-19-etl-pipeline
   ```

3. Build and start the infrastructure
   ```
   docker compose up -d
   ```

4. Access the Airflow UI
   ```
   http://localhost:8080/
   ```

5. Run the ETL DAG to process the WHO data and load it into PostgreSQL

6. Connect PowerBI to the data warehouse:
   - Open PowerBI and select "Get Data" > "Database" > "PostgreSQL Database"
   - Enter connection details:
     ```
     Server: localhost:5442
     Database: postgres
     ```
   - Authenticate and load the data

## Project Structure
```
├── airflow/            # Airflow DAGs and configuration
├── data/               # Data storage directory
├── scripts/            # ETL Python scripts
├── sql/                # SQL scripts for table creation and queries
├── docker-compose.yml  # Infrastructure setup
└── README.md           # Project documentation
```

