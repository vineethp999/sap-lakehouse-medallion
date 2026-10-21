# sap-lakehouse-medallion
Create and ingest SAP sales data (customers, materials, sales orders) delivered as # SAP → Databricks Lakehouse

Goal: ingest SAP sales data (customers, materials, sales orders) delivered as
delta extracts, and build a Medallion lakehouse on Databricks Free Edition.

- Bronze: raw SAP extracts, loaded with Auto Loader
- Silver: cleaned and typed data, with customer history kept as SCD Type 2
- Gold: a star schema for Power BI and Genie

Status: work in progress, built step by step.
