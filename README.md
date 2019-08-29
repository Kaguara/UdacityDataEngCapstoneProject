## Purpose of this Database
The purpose of this project is to build an ETL pipeline using Apache Airflow to create dimension tables for mobile wallet transactions. The project leverages Airflow's scheduling and orchetration capabilities to run the extract and load functions on the raw data stored in an S3 bucket.

The oriignal data set is sourced from Kaggle, referencing a research project that generated a synthetix data set of mobile money wallet transactions. 

Source: https://www.kaggle.com/ntnu-testimon/paysim1

Organising the database into these tables will help us answer questions like:
- Who are the top merchants in the dataset?
- What is the most common type of transaction?
- Is a transaction suspicious or fraudulent?

### Running the Airflow Job
Step 1: Pull the latest version of the project from 

Step 2: In a terminal, run "airflow webserver -p 3000". Note you can use a different port number, depending on which ports are available on your machine.

Step 3: In a separate tab, run: "airflow scheduler"

Step 4: Open your browser and navigate to: "http://localhost:3000/"

Step 5: Tap on the admin menu option and in the drop down, select "Connections". In the Connections page, enter information on your aws_credentials and a redshift connection pointing to your redshift instance in AWS.



### Explanations of files in the repository
1. *kaguara_capstone_dag.py*: This file contains the main dag definition and instantiation of the different tasks as well as the task orchestration worflow.
2. *sql_statements.py*: This file contains all the SQL commands required to build the tables, copy data from s3 to redshift and insert data into the dimension tables.
3. *create_tables.py*: Create tables custom operator. 
4. *load_dimension.py*: Redshift custom operator to insert data into the dimension tables.
5. *stage_redshift*: Redshift custom operator to copy data over from S3 to Redshift.


## Database Design
Coming soon...

## ETL Pipeline
Coming soon...

## Example Analysis Queries
Coming soon...