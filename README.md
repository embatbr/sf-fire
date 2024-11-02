# sf-fire

## Proposed architecture

- Use dbt to acquire the data on a daily basis, downloading it;
- Run Spark to process the data, ensuring schema and modifying whatever needed. Triggered by dbt;
- Load into PostgreSQL (the "data wharehouse").
