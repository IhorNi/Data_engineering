# Intro to Data_engineering - ETL Project

Build a data pipeline using Spark, HDFS, Airflow:

1. Upload data from dshop PostgreSQL tables to RAW (Bronze).
2. Download the data for the current day from the out_of_stock API. If there is no data for today, then Airflow should give an error with the corresponding log message in the Logs to Airflow Task.
3. Data in Bronze must be partitioned by date.
4. Transfer data from Bronze to Silver, along the way clearing them and removing duplicates.
5. Data in Silver must be stored in Parquet format.
6. Add Airflow logs for every action.
7. Processing data from PostgreSQL and from API must be in different Airflow DAGs. Each separate processing step is a separate Airflow Task.

## PySpark Homework

Using **https://github.com/devrimgunduz/pagila** database solve the following tasks with PySpark

1. Display the number of films in each category, sort in descending order.
2. Display 10 actors, whose films were rented the most, sort in descending order.
3. Display the category of films on which you spent the most money.
4. Display the names of movies that are not in inventory.
5. Bring out the top 3 actors who have appeared in the most films in the “Children” category. If several actors have the same number of films, display all ..
6. Display cities with the number of active and inactive customers (active - customer.active = 1). Sort by number of inactive clients in descending order.
7. Display the category of films that has the largest number of total rental hours in cities (customer.address_id in this city), and which start with the letter “a”. Do the same for cities with a “-” symbol.


