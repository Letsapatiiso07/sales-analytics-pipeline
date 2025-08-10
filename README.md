# Sales Analytics Pipeline
A complete data engineering pipeline built in Python and Databricks, demonstrating end-to-end data processing skills.

## Overview
This project showcases a sales analytics pipeline, including:
- **Data Generation**: Simulated 1,000 sales transactions with realistic attributes.
- **Data Quality**: Validation checks for missing values, duplicates, and invalid data.
- **ETL Pipeline**: Transformations for time-based features and customer segmentation.
- **Business Analytics**: Calculated KPIs like total revenue, average order value, and customer retention.
- **Export & Reporting**: Prepared datasets for visualization and saved to DBFS.

## Skills Demonstrated
- Data ingestion and generation
- Data quality validation
- ETL pipeline development
- Customer segmentation
- KPI calculation and business intelligence
- Databricks notebook development

## How to Run
1. Import `Sales_Analytics_Pipeline.dbc` into Databricks to run the notebook.
2. Alternatively, run `Sales_Analytics_Pipeline.py` in a Python environment with pandas and numpy installed.
3. Ensure a Databricks cluster with sufficient resources is available for execution.

## Files
- `Sales_Analytics_Pipeline.py`: Python source code of the pipeline.
- `Sales_Analytics_Pipeline.dbc`: Databricks notebook archive for import.

## Portfolio Note
This project is designed to showcase data engineering capabilities for job applications. It’s production-ready and includes error handling (e.g., fixed export summary display issue).

## Requirements
- Python 3.8+
- pandas, numpy
- Databricks (for .dbc file)
