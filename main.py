import pyspark
import pandas as pd
from pyspark.sql import SparkSession


def main():
    pizza_dataset_path = "./datasets/pizza_sales/order_details.v2.csv"
    spark = SparkSession.builder.appName('Learning Spark').getOrCreate()
    data = spark.read.option('header', 'true').csv(pizza_dataset_path, inferSchema=True)

    print(data)
    new_data = data.na.fill('Missing values')
    print(data)



if __name__ == "__main__":
    main()
