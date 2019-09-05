## Purpose of this Database
The purpose of this project is to build an ETL pipeline using Apache Airflow to create dimension tables for mobile wallet transactions. The project leverages Airflow's scheduling and orchestration capabilities to run the extract and load functions on the raw data stored in an S3 bucket.

The original data set is sourced from Kaggle, referencing a research project that generated a synthetic data set of mobile money wallet transactions.

Source: https://www.kaggle.com/ntnu-testimon/paysim1

Organising the database into these tables will help us answer questions like:
- Who are the top merchants in the dataset?
- What is the most common type of transaction?
- Is a transaction suspicious or fraudulent?

### Running the Airflow Job
Step 1: Pull the latest version of the project from the master branch of this Git repository, git@github.com:Kaguara/UdacityDataEngCapstoneProject.git.

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
6. *stage_redshift_json*: Redshift custom operator to copy data stored in JSON format over from S3 to Redshift.


### Database Design 
The database was designed to make it easy to get insights on merchant and consumer behavior on the transaction network. As a result, the approach I took was to copy all the mobile money transaction data to a large staging table. These tables contain all the unique transaction records. With this table in place, I created two additional tables, merchants and customers. For each table I curated a set of features that would help me understand merchant and customer usage patterns. By taking this approach I was able to reduce the need to query the staging transactions table directly as some of the key insights had been aggregated in the customers and merchants table.

The second data source I used was from the World Bank. This data set contains survey responses on different financial habits for consumers in emerging markets. The target markets for mobile money deployments. By collecting information from the World Bank on how users spend at merchant locations we can compare that aginst the simulated spend in the synthetic mobile money data set.

### Handling Large Datasets
In order to support 100x volume of the data, I would recommend integrating an AWS EMR cluster running Spark. This will allow you to leverage Spark's in-memory processing to speed up query and compute performance over the larger dataset. In addition to this, I would recommend looking into a more efficient distribution key strategy that would allow you to allocate data across the diffrent workers in a manner that can increase query performance.

If the pipelines would be run on a daily basis by 7 am every day, I would recommend setting the schedule_interval to '@daily'or '0 0 * * *' for the cron format, indicating the dag needs to run daily.

 To enable the database for access by multiple people, you would need to account for the supporting a large number of concurrent read/write connections. To account for this, please utilize AWS' Concurrency Scaling feature for Redshift. This will allow for automatic scaling of "just enough" additional clusters to handle the increased database read/write load. This will ensure extra resources are provisioned only when required, which will ultimately save on costs.

### ETL Pipeline
The ETL pipeline is orchestrated by Airflow. The first step is creating the required tables and then reading source data files stored in S3 and using Airflow's AWS operators to write the data to Redshift. I used default configurations for my project's Airflow DAG, setting retries to 0 for quick trouble-shooting, and setting retry delay to 30 seconds for the same trouble shooting reasons. In production, I would likely set the retries to a range of 1-3, depending on the complexity of the tasks, and the retry delay to some measure in minutes.

### Example Analysis Queries
- select count(*) from customers where payment_count > 0
- select merchant_name from merchants where distinct_customer_count>10