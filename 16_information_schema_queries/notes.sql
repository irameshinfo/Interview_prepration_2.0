Permissions required
===================
-BigQuery Resource Viewer
-BigQuery Metadata Viewer

1. JOBS / JOBS_BY_USER (total_slot_ms,total_bytes_processed)

Used for:

Query monitoring
Slot usage
Performance tuning
Troubleshooting

select * from `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT;

2. TABLES

Used to view dataset tables metadata.

SELECT * FROM `rameshgcplearning.emp_dataset.INFORMATION_SCHEMA.TABLES`;

3. COLUMNS

Shows column details.

SELECT * FROM `rameshgcplearning.emp_dataset.INFORMATION_SCHEMA.COLUMNS`;

4. PARTITIONS

Used for partition monitoring.

SELECT * FROM `rameshgcplearning.emp_dataset.INFORMATION_SCHEMA.PARTITIONS`;

5. SCHEMATA

Lists datasets.

SELECT * FROM `rameshgcplearning.INFORMATION_SCHEMA.SCHEMATA`;

6. ROUTINES

Shows procedures and functions.

SELECT * FROM `rameshgcplearning.emp_dataset.INFORMATION_SCHEMA.ROUTINES`;

7. VIEWS

Lists all views.

SELECT *
FROM `rameshgcplearning.emp_dataset.INFORMATION_SCHEMA.VIEWS`;

8. MATERIALIZED_VIEWS

Lists materialized views.

SELECT *
FROM `rameshgcplearning.emp_dataset.INFORMATION_SCHEMA.MATERIALIZED_VIEWS`;

9. TABLE_STORAGE

Storage usage monitoring.

SELECT *
FROM `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE;

SELECT
  table_schema,
  table_name,
  ROUND(total_logical_bytes/1024/1024,2) AS size_mb
FROM `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
WHERE table_schema = 'masking'
ORDER BY size_mb DESC;