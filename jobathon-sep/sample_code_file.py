# Import libraries
from pyspark.sql.functions import *



# Your code goes inside this function
# Function input - spark object, click data path, resolved data path
# Function output - final spark dataframe
def sample_function(spark, s3_clickstream_path, s3_login_path):

	# Your code goes below
	df_clickstream =  spark.read.format("json").load(s3_clickstream_path)
	user_mapping =  spark.read.format("csv").option("header",True).load(s3_login_path)

	df_clickstream = df_clickstream.withColumn("current_date",substring(col("event_date_time"),1,10))
	df_clickstream = df_clickstream.withColumn("number_of_clicks",lit(1)) \
	                        .withColumn("number_of_pageloads",lit(0)) \
	                        .withColumn("user_id",lit(1)) \
	                        .withColumn("logged_in",lit(0)) \
	                        .withColumn("first_url",lit("abc")) \
	                        .withColumn("browser_id",lit("abc"))
	df_union = df_clickstream.select("current_date","browser_id","user_id","logged_in","first_url","number_of_clicks","number_of_pageloads")
	
	df_union.createOrReplaceTempView("df_union_tbl")
	
	df_result = spark.sql("select * from df_union_tbl limit 1000")
	
	# Return your final spark df
	return df_result
