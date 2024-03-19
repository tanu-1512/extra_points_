import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, sum, avg
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

def create_spark_session(master_node_url):
    return SparkSession.builder \
        .appName('ECommerceAnalysis') \
        .master(master_node_url) \
        .getOrCreate()

def read_data(spark, file_path, schema):
    return spark.read.csv(file_path, schema=schema, header=True)

def get_schema():
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("purchase_amount", DoubleType(), True),
        StructField("product_category", StringType(), True),
        StructField("transaction_date", TimestampType(), True)
    ])

def basic_analysis(df):
    return {
        "distinct_customers": df.select(countDistinct("customer_id")),
        "total_revenue": df.select(sum("purchase_amount")),
        "average_purchase": df.select(avg("purchase_amount"))
    }

def advanced_analysis(df):
    return {
        "popular_categories": df.groupBy("product_category").count().orderBy('count', ascending=False),
        "high_value_customers": df.groupBy("customer_id").sum("purchase_amount").orderBy('sum(purchase_amount)', ascending=False),
        "low_value_customers": df.groupBy("customer_id").sum("purchase_amount").orderBy('sum(purchase_amount)', ascending=True)
    }

def save_to_file(df, output_path):
    df.coalesce(1).write.option("header", True).csv(output_path)

def main(master_node_url, input_file_path, output_dir):
    spark = create_spark_session(master_node_url)
    schema = get_schema()
    data = read_data(spark, input_file_path, schema)
    data.printSchema()

    basic_results = basic_analysis(data)
    advanced_results = advanced_analysis(data)

    os.makedirs(output_dir, exist_ok=True)
    
    for key, df in basic_results.items():
        save_to_file(df, os.path.join(output_dir, key))
    
    for key, df in advanced_results.items():
        save_to_file(df, os.path.join(output_dir, key))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: program.py <input_file_path> <output_directory>")
        sys.exit(1)
    input_file_path = sys.argv[1]
    output_dir = sys.argv[2]
    master_node_url= "spark://2b2d1b89594d:7077"
    main(master_node_url,input_file_path, output_dir)