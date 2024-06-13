import pyspark
import pandas as pd
from pyspark.sql import SparkSession


def main():
    data = pd.read_csv("./datasets/pizza_sales/order_details.csv")
    print(data)
    spec = SparkSession.builder.appName('Learning Spark').getOrCreate()
    spec.stop()
    print(spec)



if __name__ == "__main__":
    main()
