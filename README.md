# sf-fire


## Proposed architecture

- Use dbt to acquire the data on a daily basis, downloading it;
- Run Spark to process the data, ensuring schema and modifying whatever needed. Triggered by dbt;
- Load into PostgreSQL (the "data wharehouse").


## Workflow

1. Run script `run-db.sh` at first. That will create a database in your local PostgreSQL and the tables used to store the data. Take notice that this must be executed only once, or the progress from previous executions will be lost;
2. 


## Observations

- The dictionary, used to create the raw table, is a combinations from the dictionary provisioned by the source and the fields from the actual data. Parts of the dictionary were incorrect, so I decided to change it to fit the data;
