-- Create schemas for data organization
-- raw: source data as-is, no transformations
-- staging: cleaned and typed data
-- marts: business-ready aggregated tables

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

SELECT 'Schemas created successfully!' AS status;
