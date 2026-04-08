Welcome on my application for an analytics engineer role at JobTeaser !

# Objective

The objective of this technical case study is to evaluate my skills with a realistic project. JobTeaser is developing a product called Career Explorer, a tool that enables universities to offer career guidance programs to their students. This tool is hosted on OpenEDX, where some information is stored. As an analytics engineer, I need to design a dbt architecture to help a Product Manager, and use OpenEDX API to get each course name.

# Launch project

I used `uv` to manage this project.

## Install necessary packages

Run the following command : `uv sync`

## Part 1 : dbt

You'll need to move to the dbt folder : `cd dbt_jobteaser`

Then, execute : `uv run dbt run --profiles-dir .`

## Part 2 : Python & Aiflow

For this part, the project is splitted in two files :
- scripts/fetch_openedx.py : call OpenEDX API to get block names
- dags/sync_career_explorer.py : execute functions to run code with Airflow
