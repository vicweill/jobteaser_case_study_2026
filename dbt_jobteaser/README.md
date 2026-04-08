Here is a very simple dbt project made as a concrete example for my case study.

I used DuckDB with DBT to create a simple database from the csv files that were given.

### Run the data

Run the following commands :
- Load data and queries : `uv run dbt run --profiles-dir .`
- Check if we don't have any bug with dbt tests : `uv run dbt test`

To execute queries, you can use :
- uv run dbt show --select <dbt_model> to preview only a specific model
- `uv run duckdb database.duckdb` at the same path as the .duckdb file to execute other queries

## Go further

We could have linked DBT with a visualisation tool such as Metabase to experience the dbt models created here in a realistic way, and use them in dashboards that could be used by a Product Manager.