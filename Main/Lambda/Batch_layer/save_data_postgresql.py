import pandas as pd
from sqlalchemy import create_engine



def save_data(data):
    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = data.toPandas()

    # Create engine
    engine = create_engine('postgresql://postgres:aymane2002@localhost:5432/Big-Data-Project')

    # Write pandas DataFrame to PostgreSQL
    pandas_df.to_sql('Phone', engine, if_exists='replace', index=False)

    print("Data stored in PostgreSQL")