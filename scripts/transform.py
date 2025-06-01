from .extract import db_connection
import pandas as pd

def load_raw_data(engine):
    df = pd.read_sql("SELECT * FROM raw_data",engine)