import pandas as pd
import numpy as np
from urllib.request import urlretrieve
import os
import logging

# configurations 
raw_tax_data = "https://assets.datacamp.com/production/repositories/6236/datasets/ec1e6401783122f7a46324a4d251ccf79063e6e4/tax_rate_cleaned.csv"
file_name = "raw_tax_data.csv"
if not os.path.exists(file_name):
    urlretrieve(raw_tax_data , file_name)
logging.basicConfig(format = "%(asctime)s - %(levelname)s : %(message)s" , filename = "logging" , level = logging.DEBUG)

# Extract data from CSV
def extract(file_path):
    logging.info(f"{file_path} has been extracted")
    return pd.read_csv(file_path)

# Transformation of data 
def transform(raw_data: pd.DataFrame):
    logging.debug(f"raw data shape : {raw_data.shape}")

    # add tax rate column 
    raw_data["tax_rate"] = raw_data["total_taxes_paid"] / raw_data["total_taxable_income"]
    
    # total taxable income per firm of each industry name
    raw_data["average_taxable_income"] = raw_data["total_taxable_income"] / raw_data["number_of_firms"]
    logging.info("average taxable income column was created")

    # filter for records with average_taxable_income > 100
    mask = raw_data["average_taxable_income"] > 100
    clean_data = raw_data.loc[mask]
    logging.info("data has been filtered for average taxable income larger than 100")
    
    # changing the index of the cleaned data
    clean_data.set_index("industry_name" , inplace = True)
    logging.info("industry_name has been set as index column")
    logging.debug(f"clean data shape : {clean_data.shape}")

    return clean_data

# Loading the data to parquet
def load(clean_data : pd.DataFrame , clean_data_path):
    logging.debug(f"data has been loaded as parquet to {clean_data_path}")
    clean_data.to_parquet(clean_data_path)

# EXTRACT , TRANFORM , LOAD TAX DATA 
try: 
    raw_data = extract("raw_tax_data.csv")
    clean_data = transform(raw_data)
    load(clean_data , "clean_tax_data.parquet")
    logging.info("Successfully extracted , transformed , and loaded data to the destination.")
except Exception as e :
    logging.error(f"Pipeline failed with error : {e}")
