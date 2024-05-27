import sys
import os

# Thêm thư mục gốc vào sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import  pickle
import ast
from xgboost import XGBRegressor
from transform import *
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType
from pyspark.ml.feature import VectorAssembler

from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Read CSV from HDFS to Spark") \
    .getOrCreate()


def return_spark_df():
    hdfs_client = InsecureClient('http://localhost:9870')

    with hdfs_client.read("/batch-layer/raw_data.csv") as reader:
        pandas_df = pd.read_csv(reader)

    spark_df = spark.createDataFrame(pandas_df)

    return spark_df




import numpy as np

from pyspark.sql.functions import lit

def convert_spark_df_to_numpy(spark_df, feature_cols):
    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = spark_df.toPandas()
    # Convert Pandas DataFrame to NumPy array
    numpy_array = pandas_df[feature_cols].values
    return numpy_array

def spark_transform():
    spark_df = return_spark_df()

    spark_df = spark_df.dropna()

    # Convert columns to numeric types
    numeric_columns = ["screen_size", "ram", "rom", "battary"]
    for column in numeric_columns:
        spark_df = spark_df.withColumn(column, spark_df[column].cast("float"))

    # Map commercial values to numerical using UDFs
    map_brand_udf = udf(lambda brand: map_brand_to_numeric(brand), IntegerType())
    map_sim_type_udf = udf(lambda sim_type: map_sim_type_to_numeric(sim_type), IntegerType())

    spark_df = spark_df.withColumn("brand", map_brand_udf(spark_df["brand"]))
    spark_df = spark_df.withColumn("sim_type", map_sim_type_udf(spark_df["sim_type"]))

    # Load pre-trained XGBoost model
    model = pickle.load(open("/home/khaihadoop/Workspace/BigDataProject/Big-Data-Project/Main/Lambda/ML_operations/xgb_model.pkl", "rb"))

    # Create features for prediction
    feature_cols = ["brand", "screen_size", "ram", "rom", "sim_type", "battary"]
    numpy_array = convert_spark_df_to_numpy(spark_df, feature_cols)

    # Predict prices
    predictions = model.predict(numpy_array)

    # Add predicted column to DataFrame
    spark_df = spark_df.withColumn("price", lit(predictions))

    print("Data transformed successfully")
    return spark_df








