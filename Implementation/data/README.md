# Data Folder

This folder contains the dataset used in the project.  
The data is organized following the Medallion Architecture, a widely adopted pattern for structuring data pipelines in a layered and scalable way.

## Medallion Architecture Overview

The Medallion Architecture consists of three core layers — **Bronze**, **Silver**, and **Gold** — each serving a distinct purpose in the data lifecycle:

### 1. Bronze Layer (Raw Data)
- Stores raw, unprocessed data exactly as ingested from the source systems.  
- Acts as a single source of truth and ensures historical retention.  
- May contain duplicates, schema inconsistencies, or incomplete records.

### 2. Silver Layer (Cleansed & Enriched Data)
- Cleans and standardizes the raw data.  
- Removes duplicates, enforces schemas, and applies basic transformations.  
- Prepares data for analytics by ensuring consistency and reliability.

### 3. Gold Layer (Business-Ready Data)
- Contains curated, aggregated, and business-friendly datasets.  
- Optimized for reporting, dashboards, and advanced analytics.  
- Provides a trusted source for decision-making and insights.
