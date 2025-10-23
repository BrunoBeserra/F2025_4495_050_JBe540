# Backend README

A Django REST backend that serves HR analytics from the Gold layer outputs of the Dayforce data pipeline. The service ingests CSV exports produced by the Gold layer and exposes clean, versioned APIs for the front end to visualize headcount, retention, turnover, and related metrics.

## 1. Overview

### Goal:
Provide a reliable API over curated HR datasets so users can explore historical trends without accessing Databricks directly.

### Key Features:

- Ingest Gold layer CSVs into a relational database for fast queries

- RESTful endpoints for metrics such as headcount, movements, tenure, and payroll history

## 2. Architecture

- Source: Gold layer CSV outputs (Delta Lake to CSV export)

- Backend: Django, Django REST Framework

- Data Access: ORM models that mirror Gold layer semantic tables

- Auth: Token or session based (choose one in .env)

- DB: SQLite for development, PostgreSQL for production

`Gold CSVs  ->  Ingestion Job  -> REST API  ->  Front End`
