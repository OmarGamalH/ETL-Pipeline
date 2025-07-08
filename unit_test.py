import pytest
from ETL import extract , transform , load
import pandas as pd

@pytest.fixture()
def clean_tax_data():
    """ Returing the clean data"""
    raw_data = extract("raw_tax_data.csv")
    clean_data = transform(raw_data=raw_data)
    return clean_data

# Testing of the shape of data
def test_clean_data(clean_tax_data):
    expected_shape = 6
    assert clean_tax_data.shape[1] == expected_shape

# Testing for the instance of Dataframe
def test_clean_data2(clean_tax_data):
    assert isinstance(clean_tax_data , pd.DataFrame)

# Testing for outline of tax_rate column
def test_tax_rate(clean_tax_data):
    assert clean_tax_data["tax_rate"].min() <= 1 and clean_tax_data["tax_rate"].max() >= 0

# Testing to make sure that data has been loaded successfully
def test_data_loaded(clean_tax_data):
   extracted_data =  pd.read_parquet("clean_tax_data.parquet")
   assert extracted_data.equals(clean_tax_data)