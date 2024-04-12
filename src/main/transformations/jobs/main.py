import datetime
import os
import shutil
import sys

from pyspark.sql.functions import lit, concat_ws, expr
from pyspark.sql.types import *

from resources.dev import config
from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.aws_read import S3Reader
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.transformations.jobs.sales_team_mart_sql_transformation_write import calcualtion_incentive_for_sales_team
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.s3_client_object import *
from src.main.utility.encrypt_decrypt import *
from src.main.utility.logging_config import *
from src.main.utility.my_sql_session import *
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import DataWriter
from src.test.mysql_table_to_dataframe import *

######################### GET S3 CLIENT ##################################
aws_access_key= config.aws_access_key
aws_secret_key= config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key),decrypt(aws_secret_key))
s3_client= s3_client_provider.get_client()

## Now we can use S3 Client for all S3 operation

response = s3_client.list_buckets()
print(response)
logger.info('List of S3 Buckets: %s', response['Buckets'])

#############Now we are going to check if last run was successful from audit table and log in#####################
################we will also establish connection to the mysql staging_product_table_to check this.
###### A status means job failed last time ################ I is success #########
######## We can also implement the login to generaate email ##################

csv_files= [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
connection= get_mysql_connection()
cursor = connection.cursor()

if csv_files:
    statement= f"select distinct file_name " \
               f"from {config.database_name}.{config.product_staging_table} " \
               f"where file_name in ({str(csv_files)[1:-1]}) and status ='A' "
    logger.info(f"dynamically created statement: {statement}")
    cursor.execute(statement)
    data=cursor.fetchall()
    if data:
        logger.info(f"you last run was unsuccessful please check and take appropriate action")
    else:
        logger.info("No matching file found")
else:
    logger.info("Last run was successful")

#### Going through s3 bucket and seeing if any file is there if not throw error and log it.. ############
########### logging all file with full path in s3 bucket folder sales_data/ #############################
try:
    s3_reader= S3Reader()
    folder_path= config.s3_source_directory
    print(folder_path)
    s3_file_absolute_path=s3_reader.list_files(s3_client,config.bucket_name,folder_path=folder_path)
    logger.info("Absolute path for files on s3 bucket: %s ",s3_file_absolute_path)
    if not s3_file_absolute_path:
        logger.info(f"No file available at Path on S3 Bucket {folder_path}")
        raise Exception("No data available to process")
except Exception as e:
    logger.error("Exited with error:- %s",e)
    raise e

##########Now lets down the file from the S3 bucket to locally so that we can process them##########
########## We will be pulling all the file and can also raise exception if file type other than .csv #######

prefix=f"s3://{config.bucket_name}/"
file_paths=[ url[len(prefix):] for url in s3_file_absolute_path ] # this willl give the directory path+file ingnoring path till bucketname
#print(file_paths)
logger.info("File path available on s3 under %s bucket and folder the is %s",config.bucket_name,file_paths)

try:
    downloader= S3FileDownloader(s3_client,config.bucket_name,config.local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("File download error: %s", e)
    raise e

# Now lets check if all the files downloaded from the s3 bucket is in our local specified path
all_files=os.listdir(config.local_directory)
logger.info("List of all the present at my local directory after the download: %s", all_files)

## Now lets filter out the files with ".csv" and get the absolute path
if all_files:
    csv_files=[]
    error_files=[]
    for file in all_files:
        if file.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(config.local_directory,file)))
        else:
            error_files.append(os.path.abspath(os.path.join(config.local_directory,file)))
    if not csv_files:
        logger.error("No csv file is present at the local directory")
        raise Exception("No csv file is present at the local directory")
else:
    logger.error("There is no file to process")
    raise Exception("There is no file to process")

logger.info("List of the csv files present to be processed: %s",csv_files)

##creating spark session

logger.info("------------------Creating Spark Session ------------------------")

spark=spark_session()

logger.info("------------------Spark Session CREATED------------------------")

logger.info("------------------Checking Schema of the files loaded from S3---------------------------")

#check required columns in the csv_files if schema correct move to correct_files ... else move to error file..

correct_files=[]
for file in csv_files:
    df_schema=spark.read.format("csv")\
        .option("header","true")\
        .load(file).columns #reading file and getting column names... for check
    logger.info(f"Schema for the {os.path.basename(file)} is :- {df_schema}")
    logger.info(f"Manadatory columns required are:- {config.mandatory_columns}")
    missing_columns= set(config.mandatory_columns) - set(df_schema) # getting the set diff to get missing columns
    logger.info(f"Missing columns are:- {missing_columns}")
    if missing_columns:
        error_files.append(file)
    else:
        logger.info(f"No Missing columns for the {file} ")
        correct_files.append(file)

logger.info("List of correct files:- %s ",correct_files)
logger.info("List of error files:- %s ",error_files)

logger.info("Moving the error_file to error files folder both locally and on s3")

if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name=os.path.basename(file_path)
            destination_path=os.path.join(config.error_folder_path_local,file_name)

            shutil.move(file_path,destination_path)
            logger.info(f"{file_name} moved from {file_path} to {destination_path}")

            ## now doing moving for s3-- in s3 we need to first copy and then delete from source,,below function will handle

            source_prefix=config.s3_source_directory
            destination_prefix=config.s3_error_directory
            #below function s3 to s3 is defined by us.... it will move file and return msg here after successful
            message=move_s3_to_s3(s3_client,config.bucket_name,source_prefix,destination_prefix,file_name)
            logger.info(f"{message}")
        else:
            logger.info(f"{file_path}:- Path does not exists")
else:
    logger.info(f"There is no error file at our dataset")

### Now need to take acre of additional columns....
### Determine the extra column..
### Before we handle extra column and laod in our dataframe... we need to make entry in staging or audit table...

logger.info("Updating the staging table to make audit entry as we are stating the Load")
insert_statements=[]
db=config.database_name
current_date=datetime.datetime.now()
formatted_date=current_date.strftime('%Y-%m-%d %H:%M:%S')
if correct_files:
    for file_path in correct_files:
        statement = f"""INSERT INTO {db}.{config.product_staging_table}(file_name,file_location,created_date,status) 
                    VALUES('{os.path.basename(file_path)}','{os.path.basename(file_path)}','{formatted_date}','A')"""

        print(statement)

        insert_statements.append(statement)
    logger.info("--------------Insert statement created for staging table-----------------")
    logger.info("--------------Connecting with MYsql Server-----------------")
    connection=get_mysql_connection()
    cursor=connection.cursor()
    logger.info("--------------Connection established Successfully-----------------")
    for query in insert_statements:
        cursor.execute(query)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("------------No file to process----------------")
    raise Exception("No correct file available for this run")

logger.info("----------------Staging table update successfully-----------------------------")

logger.info("----------------Now handling the extra column coming fromm the Source---------------")

schema=StructType([
    StructField('customer_id',IntegerType(),True),
    StructField('store_id',IntegerType(),True),
    StructField('product_name',StringType(),True),
    StructField('sales_date',DateType(),True),
    StructField('sales_person_id',IntegerType(),True),
    StructField('price',FloatType(),True),
    StructField('quantity',IntegerType(),True),
    StructField('total_cost',FloatType(),True),
    StructField('additional_columns',StringType(),True)
])

final_df_to_process=spark.createDataFrame([],schema=schema) #creating empty df
logger.info("----------Empty DF craeted with required schema")
final_df_to_process.show()

for file_path in correct_files:
    data_df=spark.read.format("csv")\
        .option("header","true")\
        .option("inferSchema","true")\
        .load(file_path)
    data_df_schema=data_df.columns
    extra_columns=set(data_df_schema)-set(config.mandatory_columns)
    logger.info(f"Extra columns for {os.path.basename(file_path)} are:- {extra_columns} ")

    ## Now in file there can be extra colummn or not so we will handle
    if extra_columns:
        ##concat_ws -> takes seperator and utiple columns and it merge themm by using seperator
        data_df=data_df.withColumn("additional_columns",concat_ws(',',*extra_columns))\
                       .select("customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost","additional_columns")
        logger.info(f"{os.path.basename(file_path)} procesed with extra columns and added in additional_columns")
    else:
        data_df=data_df.withColumn("additional_columns",lit(None))\
                       .select("customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost","additional_columns")

    #### Now lets merge the file empty df we already created... and loop will run and keep adding data until all correct file's data added to df
    final_df_to_process=final_df_to_process.union(data_df)
    logger.info("-------Final from created from taking data from multiple source files----------------- ")

final_df_to_process.show()

# File we received format he source is same as our Fact Table
#Now enrich the data from all dimension tables
#also create data_mart for sales team with incetive to be given
#also create data_mart for customer with total who broght how much on each day of the month
#for every month there should be a file and inside that partiton based on store_id

logger.info("Connection established to Mysql using DatabaseReader")
#Creating data client which will help us connect to all out mmysql tables and create dataframe from it..
database_client=DatabaseReader(config.url,config.properties)
logger.info("Loading all the tables from mysql to dataframe")

## JDBC connection for me not working so manually creating the df similar to after we get from mysql. PLease use below code for directly getting the table..

logger.info("******************Loading product_staging_table data in customer_table_df*********************************")
customer_table_df=database_client.create_dataframe(spark,config.customer_table_name)
#customer_table_df.printSchema()
#customer_table_df.show()

logger.info("******************Loading product_staging_table data in product_staging_table_df*********************************")
product_staging_table_df=database_client.create_dataframe(spark,config.product_staging_table)
#product_staging_table_df.printSchema()
#product_staging_table_df.show()

logger.info("******************Loading product_table table data in product_table_df*********************************")
product_table_df=database_client.create_dataframe(spark,config.product_table)
#product_table_df.printSchema()
#product_table_df.show()

logger.info("******************Loading sales_team_table data in sales_team_table_df*********************************")
sales_team_table_df=database_client.create_dataframe(spark,config.sales_team_table)
#sales_team_table_df.printSchema()
#sales_team_table_df.show()

logger.info("******************Loading store_table data in store_table_df*********************************")
store_table_df=database_client.create_dataframe(spark,config.store_table)
#store_table_df.printSchema()
#store_table_df.show()

#########################################################################################################
## can use below code to manually created f if connection not working..
"""
logger.info("-----Enriching data by joining data from source ,customer,stores,sales_team dfs-------------------")
#Manually creating

customer_table_df=get_customer_df(spark)
#print("Customer Table")
#customer_table_df.printSchema()

store_table_df=get_store_df(spark)
#print("Store Table")
#store_table_df.printSchema()

sales_team_table_df=get_sales_team_df(spark)
#print("Sales Team Table")
#sales_team_table_df.printSchema()

product_table_df=get_product_df(spark)
#print("Sales Team Table")
#product_table_df.printSchema()
"""
s3_customer_store_sales_df_join=dimesions_table_join(final_df_to_process,
                                                     customer_table_df,
                                                     store_table_df,
                                                     sales_team_table_df)

logger.info("-------------Enriched DataFrame----------------------")
s3_customer_store_sales_df_join.show()


logger.info("****************** Write customers data in customer Data_Mart *************")

## 1 -> Data written locally
final_customer_data_mart_df=s3_customer_store_sales_df_join\
                            .select("ct.customer_id","ct.first_name","ct.last_name","ct.address","ct.pincode","phone_number",
                                    "sales_date","total_cost")# these 2 columns coming from fact table
logger.info("-------------------Final data for customer_data_mart-----------------------------")
final_customer_data_mart_df.show()
# we  have classs build to handle the login in scr/main/write/parquet_writer.py  simple class but we can also use direct method
pq_qrite=DataWriter("overwrite","parquet")
pq_qrite.dataframe_writer(final_customer_data_mart_df,config.customer_data_mart_local_file)
logger.info(f"***********Data written locally to {config.customer_data_mart_local_file}***********************")

## 2-> data written to s3
logger.info(f"***********Data movement from  local to S3 customer_data_mart  ***********************")
## data from s3 can be taken by the reporting or downstream team
s3_upload_customer_datamart=UploadToS3(s3_client)
message=s3_upload_customer_datamart.upload_to_s3(config.s3_customer_datamart_directory,config.bucket_name,config.customer_data_mart_local_file)
logger.info(f"{message}")
logger.info("****************** Write Sales Team data to Sales Data_Mart *************")
# First locally then copy data from local to S3
sales_team_final_data_mart_df=s3_customer_store_sales_df_join.select("store_id",
                                                         "sales_person_id","sales_person_first_name","sales_person_last_name",
                                                         "store_manager_name","manager_id","is_manager",
                                                         "sales_person_address","sales_person_pincode",
                                                         "sales_date","total_cost",expr("SUBSTRING(sales_date,1,7) as sales_month")
                                                         )
logger.info("-------------------Final data for Sales_Team_data_mart-----------------------------")
sales_team_final_data_mart_df.show()
## writing data without partitioning it..
##locally
pq_qrite.dataframe_writer(sales_team_final_data_mart_df,config.sales_team_data_mart_local_file)
logger.info(f"***********Data written locally to {config.sales_team_data_mart_local_file}***********************")
## to s3
s3_upload_sales_datamart=UploadToS3(s3_client)
message=s3_upload_sales_datamart.upload_to_s3(config.s3_sales_datamart_directory,config.bucket_name,config.sales_team_data_mart_local_file)

##### 2nd Approach send Partitoned data locally and S3

# Here we will also partition our sales data by month and inside month folder by store id
sales_team_final_data_mart_df.write.format("parquet") \
                .option("header", "true") \
                .mode("overwrite") \
                .partitionBy("sales_month","store_id")\
                .option("path", config.sales_team_data_mart_partitioned_local_file) \
                .save()
logger.info(f"***********Data written locally to {config.sales_team_data_mart_partitioned_local_file}***********************")

### Sending data to S3
S3_prefix="sales_partitioned_data_mart"
current_epoch=int(datetime.datetime.now().timestamp())*1000
for root,dirs,files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    print("Root path:",root)
    for file in files:
        print("Files:- ",file)
        local_file_path=os.path.join(root,file)
        print("Local Path(root+file):- ",local_file_path)
        relative_file_path=os.path.relpath(local_file_path,config.sales_team_data_mart_partitioned_local_file)
        print("Relative Path:",relative_file_path)
        s3_key=f"{S3_prefix}/{current_epoch}/{relative_file_path}"
        print("S3_Key:- ",s3_key)
        s3_client.upload_file(local_file_path,config.bucket_name,s3_key)
        print("---------------------------------------------")

logger.info(f"***********Partitioned Data written to S3 {config.s3_partitioned_data_mart}/{current_epoch}/***********************")

##Calculation for Datamart
## Calculate total sales for each customer by month
##Then write this details to mysql table

logger.info("---------Calculating the customer Every month purchase amount--------------")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("--------Calculation for Customer data mart done and data written to mysql table--------------")

## Now we will calculate the Sales person incentive and write detail to the
## We are only giving incentive to top performer and 1% incentive
logger.info("---------Calculating the Sales person incentive each month--------------")
calcualtion_incentive_for_sales_team(sales_team_final_data_mart_df)
logger.info("--------Calculation for Sales person incentive done and data written to mysql table--------------")

## Move the file on S3 to processed folder so that next day file can come
logger.info("---------Moving Data from S3 source directory to processed since data loaded in mysql table and processed file pushed to s3-----------")
source_prefix=config.s3_source_directory
destination_prefix=config.s3_processed_directory
message=move_s3_to_s3(s3_client,config.bucket_name,source_prefix,destination_prefix)
logger.info(f"{message}")

#Now
logger.info("--------------------------Delete all the local files and make folders Clean for next Run-------------------------")

logger.info("-----------Deleting Sales data from Local----------------------------------------------")
delete_local_file(config.local_directory)
logger.info("-----------Deleted Sales data from Local-----------------------------------------------")

logger.info("-----------Deleting sales_team_data_mart_partitioned_local_file data from Local----------------------------------------------")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("-----------Deleted sales_team_data_mart_partitioned_local_file data from Local-----------------------------------------------")

logger.info("-----------Deleting customer_data_mart_local_file data from Local----------------------------------------------")
delete_local_file(config.customer_data_mart_local_file)
logger.info("-----------Deleted customer_data_mart_local_file data from Local-----------------------------------------------")

logger.info("-----------Deleting sales_team_data_mart_local_file) data from Local----------------------------------------------")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("-----------Deleted sales_team_data_mart_local_file) data from Local-----------------------------------------------")

logger.info("********************Updating Staging table with 'I' status *******************")

current_date=datetime.datetime.now()
formatted_date=current_date.strftime('%Y-%m-%d %H:%M:%S')
update_statement=[]
if correct_files:
    for file in correct_files:
        file_name=os.path.basename(file)
        statement=f"UPDATE {config.database_name}.{config.product_staging_table} "\
                  f" SET status='I' , updated_date='{formatted_date}' "\
                  f" WHERE file_name= '{file_name}' "

        update_statement.append(statement)
        logger.info(f"Update statement created for staging table --------------------{statement}")
    mysql_conn=get_mysql_connection()
    cursor=mysql_conn.cursor()
    for query in update_statement:
        cursor.execute(query)
        mysql_conn.commit()
    cursor.close()
    mysql_conn.close()
else:
    logger.error("*******There is some error in the process in between**********************")
    sys.exit()

#input("Press enter to close the application")














