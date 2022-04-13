# Data-Lake-ETL-via-AWS-EMR-and-Apache-Spark
In this project, ETL was implemented on a simulative big data which states on cloud storage (AWS s3) in order to build a database with ease of analytics.
## Project Summary
In this project, Sparkfy music streaming app data was extracted using by ETL pipeline in order to create a data lake from s3 bucket as JSON format and transferred star schema tabulation where was "songplays" table as a fact table.Also, tables "users", "songs", "artists", "time" are dimension tables. These tables were saved into user specified location folder by folder in parquet format. Thanks to this, analitical approaches are going to be feasible.

## ETL Pipeline & Star Schema
In order to load the JSON formatted data from s3 bucket pyspark module has been used. In order to process for different files same method have been used. On the other hand, required expressions were transformed into table without duplicates. On the other hand, in order to create "songplays" table songs_data and log_data were left joined. In that manner, ETL process for this project was executed in three phases, loading data from source, converting or joining the data into required specifications and record them into specific files. 
Star schema is very suitable in order to perform analytical solutions for needs. For instance, which song is listening more and how many paid instances happened for a specific time period can be exemine on ease. 

## Code Statement
*In `etl.py`, code implemention steps was explained exhaustively.
*Data which is processed in the project stated in `data.zip`.
*Configuration file (`dl.cfg`), necessary AWS access key and AWS secret access key should be stated properly.
*`dl.cfg`,`etl.py` scripts should be stated at the same folder directory.

## Suggestions
Due to data skewness for eliminate the bottlenecks of this project, more partitions can be used. Also, malformed data can be checked and filtered in order to get cleaner results.
