# SalesDataPipline-DataMart-Project

This Data Engineering aims to provide you with insights into the functioning of projects within a real-time environment.
- Data will be pulled from upstream (Amazon S3) and then downloaded to on-premise (local) and processed using the business logic.
- Processed data then will be written into Customer and Sales Datamart in MySQL Table in the warehouse and for the same data partitioned parquet will be generated and uploaded to Amazon S3.
- Downstream and reporting team can access datamart tables and can also use parquet files provided on the Amazon S3 bucket for analysis.
- This project aims to replicate the production environment so includes features like audit-table, archiving of processed data, schema validation of the files, handling extra and less column, detecting the wrong format file, logging, etc.
- Business logic like customer total sales by month, giving incentive to top salesman, etc.

The code has been meticulously crafted with careful consideration for various aspects. It not only nurtures your coding skills but also imparts a comprehensive comprehension of project structures.

## Requirements:

- Laptop with minimum 4 GB of RAM, i3 and above (Better to have 8GB with i5).
- Local setup of Spark.
- PyCharm installed on the system.
- MySQL Workbench should also be installed on the system.
- GitHub account is good to have but not necessary.
- AWS account.
- Understanding of Spark, SQL, and Python is required.

## Project Structure:
          my_project
           ├── docs
           │   └── readme.md
           ├── resources
           │   ├── init.py
           │   ├── dev
           │   │   ├── config.py
           │   │   └── requirement.txt
           │   ├── qa
           │   │   ├── config.py
           │   │   └── requirement.txt
           │   ├── prod
           │   │   ├── config.py
           │   │   └── requirement.txt
           │   └── sql_scripts
           │       └── table_scripts.sql
           └── src
               ├── main
               │   ├── init.py
               │   ├── delete
               │   │   ├── aws_delete.py
               │   │   ├── database_delete.py
               │   │   └── local_file_delete.py
               │   ├── download
               │   │   └── aws_file_download.py
               │   ├── move
               │   │   └── move_files.py
               │   ├── read
               │   │   ├── aws_read.py
               │   │   └── database_read.py
               │   ├── transformations
               │   │   └── jobs
               │   │       ├── customer_mart_sql_transform_write.py
               │   │       ├── dimension_tables_join.py
               │   │       ├── main.py
               │   │       └── sales_team_mart_sql_transformation_write.py
               │   ├── upload
               │   │   └── upload_to_s3.py
               │   └── utility
               │       ├── encrypt_decrypt.py
               │       ├── logging_config.py
               │       ├── s3_client_object.py
               │       ├── spark_session.py
               │       └── my_sql_session.py
               └── test
                   ├── scratch_pad.py.py
                   ├── generate_csv_data.py
                   ├── extra_column_csv_generated_data.py
                   ├── generate_customer_table_data.py
                   ├── generate_datewise_sales_data.py
                   ├── less_column_csv_generated_data.py
                   ├── mysql_table_to_dataframe.py
                   └── sales_data_upload_s3.py


## How to run the program in PyCharm:

1. Open the PyCharm editor.
2. Upload or pull the project from GitHub.
3. Open the terminal from the bottom pane.
4. Go to the virtual environment and activate it. Let's say you have venv as a virtual environment.
    - `cd venv`
    - `cd Scripts`
    - `activate` (if activate doesn't work then use ./activate)
5. You will have to create a user on AWS also and assign S3 full access and provide a secret key and access key to the config file.
6. Run main.py from the green play button on the top right-hand side.

