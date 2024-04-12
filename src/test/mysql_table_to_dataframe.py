from pyspark.sql.functions import to_date, to_timestamp
from pyspark.sql.types import *
def get_customer_df(spark):
    schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("pincode", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("customer_joining_date", StringType(), True)
    ])

    # Insert data into DataFrame
    data = [
        (1, 'Saanvi', 'Krishna', 'Delhi', '122009', '9173121081', '2021-01-20'),
        (2, 'Dhanush', 'Sahni', 'Delhi', '122009', '9155328165', '2022-03-27'),
        (3, 'Yasmin', 'Shan', 'Delhi', '122009', '9191478300', '2023-04-08'),
        (4, 'Vidur', 'Mammen', 'Delhi', '122009', '9119017511', '2020-10-12'),
        (5, 'Shamik', 'Doctor', 'Delhi', '122009', '9105180499', '2022-10-30'),
        (6, 'Ryan', 'Dugar', 'Delhi', '122009', '9142616565', '2020-08-10'),
        (7, 'Romil', 'Shanker', 'Delhi', '122009', '9129451313', '2021-10-29'),
        (8, 'Krish', 'Tandon', 'Delhi', '122009', '9145683399', '2020-01-08'),
        (9, 'Divij', 'Garde', 'Delhi', '122009', '9141984713', '2020-11-10'),
        (10, 'Hunar', 'Tank', 'Delhi', '122009', '9169808085', '2023-01-27'),
        (11, 'Zara', 'Dhaliwal', 'Delhi', '122009', '9129776379', '2023-06-13'),
        (12, 'Sumer', 'Mangal', 'Delhi', '122009', '9138607933', '2020-05-01'),
        (13, 'Rhea', 'Chander', 'Delhi', '122009', '9103434731', '2023-08-09'),
        (14, 'Yuvaan', 'Bawa', 'Delhi', '122009', '9162077019', '2023-02-18'),
        (15, 'Sahil', 'Sabharwal', 'Delhi', '122009', '9174928780', '2021-03-16'),
        (16, 'Tiya', 'Kashyap', 'Delhi', '122009', '9105126094', '2023-03-23'),
        (17, 'Kimaya', 'Lala', 'Delhi', '122009', '9115616831', '2021-03-14'),
        (18, 'Vardaniya', 'Jani', 'Delhi', '122009', '9125068977', '2022-07-19'),
        (19, 'Indranil', 'Dutta', 'Delhi', '122009', '9120667755', '2023-07-18'),
        (20, 'Kavya', 'Sachar', 'Delhi', '122009', '9157628717', '2022-05-04'),
        (21, 'Manjari', 'Sule', 'Delhi', '122009', '9112525501', '2023-02-12'),
        (22, 'Akarsh', 'Kalla', 'Delhi', '122009', '9113226332', '2021-03-05'),
        (23, 'Miraya', 'Soman', 'Delhi', '122009', '9111455455', '2023-07-06'),
        (24, 'Shalv', 'Chaudhary', 'Delhi', '122009', '9158099495', '2021-03-14'),
        (25, 'Jhanvi', 'Bava', 'Delhi', '122009', '9110074097', '2022-07-14')
    ]
    customer_df = spark.createDataFrame(data, schema)
    customer_df = customer_df.withColumn("customer_joining_date", to_date("customer_joining_date"))
    return customer_df

def get_store_df(spark):
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("address", StringType(), True),
        StructField("store_pincode", StringType(), True),
        StructField("store_manager_name", StringType(), True),
        StructField("store_opening_date", StringType(), True),
        StructField("reviews", StringType(), True)
    ])

    data = [
        (121, 'Delhi', '122009', 'Manish', '2022-01-15', 'Great store with a friendly staff.'),
        (122, 'Delhi', '110011', 'Nikita', '2021-08-10', 'Excellent selection of products.'),
        (123, 'Delhi', '201301', 'vikash', '2023-01-20', 'Clean and organized store.'),
        (124, 'Delhi', '400001', 'Rakesh', '2020-05-05', 'Good prices and helpful staff.')
    ]
    store_df = spark.createDataFrame(data, schema)
    store_df = store_df.withColumn("store_opening_date", to_date("store_opening_date"))
    return store_df

def get_sales_team_df(spark):
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("manager_id", IntegerType(), True),
        StructField("is_manager", StringType(), True),
        StructField("address", StringType(), True),
        StructField("pincode", StringType(), True),
        StructField("joining_date", StringType(), True)
    ])

    data = [
        (1, 'Rahul', 'Verma', 10, 'N', 'Delhi', '122007', '2020-05-01'),
        (2, 'Priya', 'Singh', 10, 'N', 'Delhi', '122007', '2020-05-01'),
        (3, 'Amit', 'Sharma', 10, 'N', 'Delhi', '122007', '2020-05-01'),
        (4, 'Sneha', 'Gupta', 10, 'N', 'Delhi', '122007', '2020-05-01'),
        (5, 'Neha', 'Kumar', 10, 'N', 'Delhi', '122007', '2020-05-01'),
        (6, 'Vijay', 'Yadav', 10, 'N', 'Delhi', '122007', '2020-05-01'),
        (7, 'Anita', 'Malhotra', 10, 'N', 'Delhi', '122007', '2020-05-01'),
        (8, 'Alok', 'Rajput', 10, 'N', 'Delhi', '122007', '2020-05-01'),
        (9, 'Monica', 'Jain', 10, 'N', 'Delhi', '122007', '2020-05-01'),
        (10, 'Rajesh', 'Gupta', 10, 'Y', 'Delhi', '122007', '2020-05-01')
    ]

    sales_team_df=spark.createDataFrame(data,schema)
    sales_team_df=sales_team_df.withColumn("joining_date", to_date("joining_date"))
    return sales_team_df

def get_product_df(spark):
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("current_price", FloatType(), True),
        StructField("old_price", FloatType(), True),
        StructField("created_date", StringType(), True),
        StructField("updated_date", StringType(), True),
        StructField("expiry_date", StringType(), True)
    ])

    data = [
        (1, 'quaker oats', 212.00, 212.00, '2022-05-15', None, '2025-01-01'),
        (2, 'sugar', 50.00, 50.00, '2021-08-10', None, '2025-01-01'),
        (3, 'maida', 20.00, 20.00, '2023-03-20', None, '2025-01-01'),
        (4, 'besan', 52.00, 52.00, '2020-05-05', None, '2025-01-01'),
        (5, 'refined oil', 110.00, 110.00, '2022-01-15', None, '2025-01-01'),
        (6, 'clinic plus', 1.50, 1.50, '2021-09-25', None, '2025-01-01'),
        (7, 'dantkanti', 100.00, 100.00, '2023-07-10', None, '2025-01-01'),
        (8, 'nutrella', 40.00, 40.00, '2020-11-30', None, '2025-01-01')
    ]
    product_df=spark.createDataFrame(data,schema)
    product_df=product_df.withColumn("expiry_date", to_date("expiry_date"))
    product_df = product_df.withColumn("created_date", to_timestamp("created_date")) \
                           .withColumn("updated_date", to_timestamp("updated_date"))
    return product_df
