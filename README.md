# sf-fire


## Workflow

1. Run script `run-db.sh` at first. That will create a database in your local PostgreSQL and the tables used to store the data. Take notice that this must be executed only once, or the progress from previous executions will be lost;
2. Download the data. Possibly there is a way to automate this, but the platform doesn't help. I am downloading manually and made the bash script parameterized to receive the date;
3. Run `run-spark.sh` with the date parameter (e.g, `run-spark.sh 20241029` for Oct 29th);
4. Query data on both schemas:
    - landing: raw data;
    - business: fact and dimensions tables


## Observations

- The dictionary, used to create the raw table, is a combinations from the dictionary provisioned by the source and the fields from the actual data. Parts of the dictionary were incorrect, so I decided to change it to fit the data.
