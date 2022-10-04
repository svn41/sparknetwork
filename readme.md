## Data Warehouse Challenge 2021: Jr Data Engineer

***instructions on how to reproduce it***


### Procedure 1: run it on docker

> run docker on your machine
> clone this repository
> create virtual environmenton on docker with the help of dockerfile in this repository 
> run docker image

### Procedure 2: run it on local virtual environment 

> clone this repository
> activate venv virtual environment which includes all packages required to run the script 
> run main.py on that virtual environment  

you'll find snowflake credentilas inside snowCred.json file. 


### task 1) Collect all the data just once from these endpoints and create/populate the tables (user, subscription and message)

To see the raw data that was directly imported from api end point, please see RAW_SPARKNERWORK database in snowflake 


### task 2) Product Owners do intend to produce metrics based on date, age, city, country, email domain, gender, smoking condition, income, subscriptions and messages. It is your responsibility to propose how to model the tables, columns and relationships. PII handling should be considered, so that no sensitive data can be accessed by the final users.


maintaininge all rules, finall dataset for the end consumer was prepared in GOLD_SPARKNERWORK database. 

### task 3) The Product Owner asked you to provide the queries for some scenarios as they are wondering about possibilities of quality issues in the data, please add a file sql_test.sql in your project with the queries that solve the below questions:

In order to run sql query on snowflake worksheet: 
> please run main.py to create database on snowflake.
> long in to snowflake using credentials 
> create a worksheet, select a warehouse
> run the script
