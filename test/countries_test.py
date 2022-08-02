import os
import csv
import pytest


@pytest.fixture(name="data")
def get_country_data():
    curr_path = os.path.abspath(__file__)
    test_csv_path = os.path.abspath(os.path.join(curr_path, "..", "countries.csv"))
    data = []
    with open(test_csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    return data
