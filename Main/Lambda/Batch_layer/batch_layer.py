from spark_tranformation import spark_transform

from save_data_postgresql import  save_data


def batch_layer():
    data = spark_transform()
    save_data(data)
