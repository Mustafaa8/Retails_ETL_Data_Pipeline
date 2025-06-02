from extract import db_connection
import pandas as pd
from sqlalchemy import text

DBURL = "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"

def create_schema(engine):
    try:
        print("Creating Schema For the data warehouse...")
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS dwh;"))
            conn.commit()
    except Exception as e:
        print("Couldn't create the Schema",e)
        raise

def load_silver_layer_data(engine):
    """
    load the silver layer(transformed data) from the database

    Args :
    - engine : sqlalchemy engine to be connected
    """
    try:
        print("Loading Silver Layer Data...")
        dataframe = pd.DataFrame()
        iterator = pd.read_sql("SELECT * FROM silver_layer",con=engine,chunksize=5000)
        for chunk in iterator:
            dataframe = pd.concat([dataframe,chunk])
        return dataframe
    except Exception as e:
        print("Couldn't load data from the database",e)
        raise

def product_dim_model(dataframe,engine):
    try:
        print("Creating Product Dim...")
        product_dim = dataframe[["product_id","category"]].drop_duplicates(subset='product_id').reset_index(drop=True).reset_index().rename({"index":"product_id","product_id":"product"},axis=1)
        dataframe = pd.merge(dataframe,product_dim,how='inner',left_on='product_id',right_on='product').drop(['category_x','product_id_x','category_y','product'],axis=1).rename(columns={"product_id_y":"product_id"})
        product_dim.to_sql("productDim",con=engine,schema='dwh',if_exists='replace',index=False)
    except Exception as e:
        print("Product Dimension Couldn't Be created",e)
        raise  

def store_dim_model(dataframe,engine):
    try:
        print("Creating Store Dim...")
        store_dim = dataframe[["store_id","region"]].drop_duplicates(subset='store_id').reset_index(drop=True).reset_index().rename({"index":"store_id","store_id":"store"},axis=1)
        dataframe = pd.merge(dataframe,store_dim,how='inner',left_on='store_id',right_on='store').drop(['region_x','store_id_x','region_y','store'],axis=1).rename(columns={"store_id_y":"store_id"})
        store_dim.to_sql("storeDim",con=engine,schema='dwh',if_exists='replace',index=False)
    except Exception as e:
        print("Store Dimension Couldn't Be created",e)
        raise

def date_dim_model(dataframe,engine):
    try:
        print("Creating Date Dim...")
        date_dim = dataframe[["date","seasonality","weather_condition"]].drop_duplicates(subset='date').reset_index(drop=True).reset_index().rename({"index":"date_id"},axis=1)
        dataframe = pd.merge(dataframe,date_dim,how='inner',left_on='date',right_on='date').drop(['date','seasonality_x','seasonality_y','weather_condition_x','weather_condition_y'],axis=1)
        date_dim.to_sql("dateDim",con=engine,schema='dwh',if_exists='replace',index=False)
    except Exception as e:
        print("Product Dimension Couldn't Be created",e)
        raise
    return

def fact_table_model(dataframe,engine):
    try:
        print("Creating the Fact Table...")
        dataframe = dataframe.iloc[:,[11,10,9,0,1,2,3,4,5,6,7,8]]
        dataframe.to_sql("Fact_table",con=engine,schema='dwh',if_exists='replace',index=False)
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE silver_layer"))
    except Exception as e:
        print("Couldn't create the Fact table",e)
        raise


if __name__ == "__main__":
    engine = db_connection(DBURL)
    df = load_silver_layer_data(engine)
    create_schema(engine)
    product_dim_model(df,engine)
    store_dim_model(df,engine)
    date_dim_model(df,engine)
    fact_table_model(df,engine)