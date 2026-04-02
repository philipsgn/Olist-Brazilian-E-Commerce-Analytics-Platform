-- Initialize Airflow metadata database and app schemas

-- Create a dedicated database for Airflow metadata
CREATE DATABASE airflow;

-- Connect to ecommerce_db to create schemas
\c ecommerce_db;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

SELECT 'Initialized successfully!' AS status;
