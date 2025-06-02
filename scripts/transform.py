from extract import db_connection
import pandas as pd
from sqlalchemy import text

DBURL = "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"

def load_raw_data(engine):
    """
    loading the raw data from the postgres database using sqlalchemy engine preparing for transformation
    
    Args:
    - engine : sqlalchemy engine of database to be connected
    """
    try:
        print("Loading Raw Data...")
        dataframe = pd.DataFrame()
        iterator = pd.read_sql("SELECT * FROM raw_data",engine,chunksize=5000)
        for chunk in iterator:
            dataframe = pd.concat([dataframe,chunk])
        return dataframe
    except Exception as e:
        print("Data Couldn't be loaded",e)
        raise

def transofrm_raw_data(dataframe):
    """
    transform raw data into more meaningful data preparing it for the final stage

    Args:
    - dataframe : the data loaded and wanted to be transofmred 
    """
    try:
        print("Transforming Loaded Data...")
        dataframe['date'] = pd.to_datetime(dataframe['date'])
        dataframe['discount'] = dataframe['discount'].astype('float64') / 100
        return dataframe
    except Exception as e:
        print("Data couldn't be transformed",e)
        raise

    
def silver_layer_data(dateframe,engine):
    """
    loading the transofmred data into the database and removing the raw data table

    Args:
    - dataframe : the table of the data
    - engine : sqlalchmey engine the connects to it
    """
    try:
        print("Loading transofmred data...")
        dateframe.to_sql("silver_layer",con = engine,if_exists = "replace",index = False,chunksize=5000)
        print("Removing Raw Data...")
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE raw_data"))
        print("Silver Layer data is in database and Raw Data Removed")
    except Exception as e:
        print("Data couldn't be loaded to database",e)
        raise


if __name__ == "__main__":
    engine = db_connection(DBURL)
    df = load_raw_data(engine = engine)
    df = transofrm_raw_data(df)
    silver_layer_data(df,engine)