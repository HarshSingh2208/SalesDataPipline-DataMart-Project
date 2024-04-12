from pyspark.sql.functions import *
from pyspark.sql.window import Window
from resources.dev import config
from src.main.write.database_write import DatabaseWriter
def calcualtion_incentive_for_sales_team(sales_team_final_data_mart_df):
    window1=Window.partitionBy("store_id","sales_person_id","sales_month")
    final_sales_team_data_mart=sales_team_final_data_mart_df.withColumn("sales_month",substring(col("sales_date"),1,7))\
                               .withColumn("total_sales_handled_by_sales_person_each_month",sum(col("total_cost")).over(window1))\
                               .select("store_id","sales_person_id",
                                       concat(col("sales_person_first_name"),lit(" "),col("sales_person_last_name")).alias("full_name"),
                                       "sales_month","total_sales_handled_by_sales_person_each_month"
                                       ).distinct()

    final_sales_team_data_mart.show()
    window2=Window.partitionBy("store_id","sales_month").orderBy(col("total_sales_handled_by_sales_person_each_month").desc())
    final_sales_team_data_mart_table=final_sales_team_data_mart.withColumn("rnk",rank().over(window2))\
                                     .withColumn("incentive",when(col("rnk")==1,col("total_sales_handled_by_sales_person_each_month")*0.01).otherwise(lit(0)))\
                                     .withColumn("incentive",round(col("Incentive"),2))\
                                     .withColumn("total_sales",col("total_sales_handled_by_sales_person_each_month"))\
                                     .select("store_id","sales_person_id","full_name","sales_month","total_sales","incentive")

    final_sales_team_data_mart_table.show()
    db_writer=DatabaseWriter(config.url,config.properties)
    db_writer.write_dataframe(final_sales_team_data_mart_table,config.sales_team_data_mart_table)