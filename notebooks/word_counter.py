from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, count
import sys

def create_spark_session(master_node_url):
    return SparkSession.builder \
        .appName('TextAnalysis') \
        .master(master_node_url) \
        .getOrCreate()

def load_text_file(spark, file_path):
    return spark.read.text(file_path)

def preprocess_text(df):
    return df.select(explode(split(lower(df.value), '\\W+')).alias('word')) \
        .filter('word != ""')

def count_words(df):
    return df.groupBy('word').agg(count('word').alias('frequency'))

def get_top_n_words(df, n):
    return df.orderBy('frequency', ascending=False).limit(n)

def main(master_node_url, file_path, top_n):
    spark = create_spark_session(master_node_url)
    text_df = load_text_file(spark, file_path)
    processed_text = preprocess_text(text_df)
    word_counts = count_words(processed_text)
    top_words = get_top_n_words(word_counts, top_n)

    top_words.show(truncate=False)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: program.py <master_node_url> <file_path> <top_n>")
        sys.exit(1)

    master_node_url = "spark://2b2d1b89594d:7077"
    file_path = sys.argv[1]
    top_n = int(sys.argv[2])
    main(master_node_url, file_path, top_n)
