
import pandas as pd
from hdfs import InsecureClient
import threading

lock = threading.Lock()

def store_data_in_hdfs(transaction_data):
    columns = ['id','brand','model_name','screen_size','ram','rom','cams','sim_type','battary',
               'sale_percentage','product_rating','seller_name','seller_score','seller_followers','Reviews']
    transaction_df = pd.DataFrame([transaction_data], columns=columns)

    hdfs_host = 'localhost'
    hdfs_port = 9870

    client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}')
    file_path = '/batch-layer/raw_data.csv'
    dir_path = '/batch-layer'

    try:
        with lock:
            # Tạo thư mục nếu chưa tồn tại
            if not client.status(dir_path, strict=False):
                client.makedirs(dir_path)
                print(f"Created directory {dir_path} on HDFS")

            if not client.status(file_path, strict=False):
                with client.write(file_path, overwrite=True) as writer:
                    transaction_df.to_csv(writer, index=False, header=True)
                    print(f"Created new file {file_path} and wrote data")
            else:
                with client.read(file_path) as reader:
                    try:
                        existing_df = pd.read_csv(reader)
                        print(f"Read existing data from {file_path}")
                        # Kiểm tra nếu tệp rỗng
                        if existing_df.empty:
                            existing_df = pd.DataFrame(columns=columns)
                    except pd.errors.EmptyDataError:
                        # Nếu tệp rỗng, tạo DataFrame trống với cột tương ứng
                        existing_df = pd.DataFrame(columns=columns)
                        print(f"File {file_path} is empty, created new DataFrame with columns")

                combined_df = pd.concat([existing_df, transaction_df], ignore_index=True)
                with client.write(file_path, overwrite=True) as writer:
                    writer.write(combined_df.to_csv(index=False, header=True).encode('utf-8'))
                    print(f"Wrote combined data to {file_path}")
    except Exception as e:
        print(f"Error accessing HDFS or writing data: {e}")






