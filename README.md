# SalesDataPipline-DataMart-Project

This Data Engineering aims to provide you with insights into the functioning of projects within a real-time environment.
--> Data will be pulled from upstream(Amazon S3) and then downloaded to on-premise(local) and processed using the business logic.
--> Processed data then will be written into Customer and Sales Datamart in Mysql Table in warehouse and for same data partioned parquet will be generated and uploaded to 
    Amazon S3.
--> Downstream and reporting team can access datamart tables and can also use parquet file provided on Amazon S3 bucket for analysis.
--> This project aims to replicate production-env so includes features like audit-table, archiving of processed data, schema validation of the files, handling extra and less 
    column, detecting wrong format file, logging etc.
--> Buiness logic like customer total sales by month,giving incetive to top sales_man etc.

The code has been meticulously crafted with careful consideration for various aspects. It not only nurtures your coding skills but also imparts a comprehensive comprehension of project structures.

Let's Start with requirement to complete the projects:-

You should have laptop with minimum 4 GB of RAM, i3 and above (Better to have 8GB with i5).
Local setup of spark. 
PyCharm installed in the system. 
MySQL workbench should also be installed to the system. 
GitHub account is good to have but not necessary.
You should have AWS account.
Understanding of spark,sql and python is required.
Project structure:-
my_project/
├── docs/
│   └── readme.md
├── resources/
│   ├── __init__.py
│   ├── dev/
│   │    ├── config.py
│   │    └── requirement.txt
│   └── qa/
│   │    ├── config.py
│   │    └── requirement.txt
│   └── prod/
│   │    ├── config.py
│   │    └── requirement.txt
│   ├── sql_scripts/
│   │    └── table_scripts.sql
├── src/
│   ├── main/
│   │    ├── __init__.py
│   │    └── delete/
│   │    │      ├── aws_delete.py
│   │    │      ├── database_delete.py
│   │    │      └── local_file_delete.py
│   │    └── download/
│   │    │      └── aws_file_download.py
│   │    └── move/
│   │    │      └── move_files.py
│   │    └── read/
│   │    │      ├── aws_read.py
│   │    │      └── database_read.py
│   │    └── transformations/
│   │    │      └── jobs/
│   │    │      │     ├── customer_mart_sql_transform_write.py
│   │    │      │     ├── dimension_tables_join.py
│   │    │      │     ├── main.py
│   │    │      │     └──sales_team_mart_sql_transformation_write.py
│   │    └── upload/
│   │    │      └── upload_to_s3.py
│   │    └── utility/
│   │    │      ├── encrypt_decrypt.py
│   │    │      ├── logging_config.py
│   │    │      ├── s3_client_object.py
│   │    │      ├── spark_session.py
│   │    │      └── my_sql_session.py
│   │    └── write/
│   │    │      ├── database_write.py
│   │    │      └── parquet_write.py
│   ├── test/
│   │    ├── scratch_pad.py.py
│   │    └── generate_csv_data.py
|   |    └── extra_column_csv_generated_data.py
|   |    └── generate_customer_table_data.py
|   |    └── generate_datewise_sales_data.py
|   |    └── less_column_csv_generated_data.py
|   |    └── mysql_table_to_dataframe.py
|   |    └── sales_data_upload_s3.py

How to run the program in Pycharm:-

Open the pycharm editor.
Upload or pull the project from GitHub.
Open terminal from bottom pane.
Goto virtual environment and activate it. Let's say you have venv as virtual environament.i) cd venv ii) cd Scripts iii) activate (if activate doesn't work then use ./activate)
You will have to create a user on AWS also and assign s3 full access and provide secret key and access key to the config file.
Run main.py from green play button on top right hand side.
