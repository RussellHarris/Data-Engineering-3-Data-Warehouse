# Purpose
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

# Project Description
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

# Files
* **IaC.ipynb** - Creates your Redshift cluster and updates 'dwh.cfg' with new DWH_ENDPOINT and DWH_ROLE_ARN values.
* **dwh.cfg** - Add your AWS credentails here.
* **create_tables.py** - Creates all of the tables.
* **etl.py** - Ingests data to the staging tables, then to the final tables.
* **sql_queries.py** - Contains all table drop, create, and insert queries.
* **queries.txt** - Sample SQL queries used throughout the project.

# Instructions
1. **Update** 'dwh.cfg' with your AWS credentials.
2. **Run** 'IaC.ipynb'
3. **Run** 'python create_tables.py'
4. **Run** 'time python etl.py' (time is optional to see ingest timing)
5. **Check** results on the database to see if data was populated as expected.

# Schema Design
* **staging_events** - staging table with no referential integrity incorporated into the design.  
*ts* designated DISTKEY & SORTKEY due to *uniqueness*.
* **staging_songs** - staging table with no referential integrity incorporated into the design.  
*song_id* designated DISTKEY & SORTKEY due to *uniqueness*.
* **songplays** - fact table, referential integrity is informational, NULL columns based on data analysis.  
*songplay_id* designated DISTKEY & SORTKEY due to *uniqueness*.
* **users** - dimension table, referential integrity is informational, all columns are NOT NULL.  
*user_id* designated DISTKEY & SORTKEY due to *uniqueness*.
* **songs** - dimension table, referential integrity is informational, all columns are NOT NULL.  
*song_id* designated DISTKEY & SORTKEY due to *uniqueness*.
* **artists** - dimension table, referential integrity is informational, NULL columns based on data analysis.  
*artist_id* designated DISTKEY & SORTKEY due to *uniqueness*.
* **time** - dimension table, referential integrity is informational, all columns are NOT NULL.  
*start_time* designated DISTKEY & SORTKEY due to *uniqueness*.
