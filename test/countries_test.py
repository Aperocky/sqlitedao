import os
import csv
import pytest
from sqlitedao import SqliteDao, TableItem, ColumnDict, SearchDict


TEST_DB_NAME = "test.db"


class Country(TableItem):
    TABLE_NAME = "countries"
    INDEX_KEYS = ["name"]
    ALL_COLUMNS = {
        "name": str,
        "area": float,
        "population": float,
        "gdp": float,
        "gdp_per_capita": float,
    }

    def __init__(self, row_tuple=None, **kwargs):
        super().__init__(row_tuple, **kwargs)
        self.load_tuple()

    def load_tuple(self):
        self.name = self.row_tuple.get("name")
        self.area = self.row_tuple.get("area")
        self.population = self.row_tuple.get("population")
        self.gdp = self.row_tuple.get("gdp")
        self.gdp_per_capita = self.row_tuple.get("gdp_per_capita")


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


@pytest.fixture(name="cdao")
def get_cdao():
    if os.path.exists(TEST_DB_NAME):
        os.remove(TEST_DB_NAME)
    dao = SqliteDao.get_instance(TEST_DB_NAME)
    columns = ColumnDict()\
        .add_column("name", "text", primary_key=True)\
        .add_column("area", "real")\
        .add_column("population", "real")\
        .add_column("gdp", "real")\
        .add_column("gdp_per_capita", "real")
    dao.create_table("countries", columns)
    yield dao
    SqliteDao.terminate_instance(TEST_DB_NAME)
    if os.path.exists(TEST_DB_NAME):
        os.remove(TEST_DB_NAME)


@pytest.fixture(name="prepared_cdao")
def prepared_cdao(data, cdao):
    for country in data:
        try:
            country = Country(name=country["country"], area=country['Surface area (km2)'], population=country['Population in thousands (2017)'], gdp=country['GDP: Gross domestic product (million current US$)'], gdp_per_capita=country['GDP per capita (current US$)'])
            cdao.insert_item(country)
        except ValueError as e:
            pass
    return cdao


def test_loading_from_csv(data, cdao):
    # This is not using sqlite default loading of csv, rather just testing/example manual loading
    for country in data:
        try:
            country = Country(name=country["country"], area=country['Surface area (km2)'], population=country['Population in thousands (2017)'], gdp=country['GDP: Gross domestic product (million current US$)'], gdp_per_capita=country['GDP per capita (current US$)'])
            cdao.insert_item(country)
        except ValueError as e:
            # Holy See has non-float count
            assert "could not convert" in str(e)
    # Rank by area!
    countries = cdao.get_items(Country, {}, order_by=["area"])
    assert countries[0].name == "Russian Federation"


def test_basic_item_pagination(prepared_cdao):
    countries = prepared_cdao.get_items_page(Country, SearchDict(), None, limit=10, desc=False)
    alphabetical_firsts = ['Afghanistan', 'Albania', 'Algeria', 'American Samoa', 'Andorra', 'Angola', 'Anguilla', 'Antigua and Barbuda', 'Argentina', 'Armenia']
    assert alphabetical_firsts == [c.name for c in countries]
    next_countries = prepared_cdao.get_items_page(Country, SearchDict(), countries[-1], limit=10, desc=False)
    alphabetical_seconds = ['Aruba', 'Australia', 'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangladesh', 'Barbados', 'Belarus', 'Belgium']
    assert alphabetical_seconds == [c.name for c in next_countries]


def test_item_pagination_with_size(prepared_cdao):
    size_requirement = SearchDict().add_filter("area", 100000, ">")
    alpha = prepared_cdao.get_items_page(Country, size_requirement, None, limit=5, desc=False)
    beta = prepared_cdao.get_items_page(Country, size_requirement, alpha[-1], limit=5, desc=False)
    alpha_expected = ['Afghanistan', 'Algeria', 'Angola', 'Argentina', 'Australia']
    beta_expected = ['Bangladesh', 'Belarus', 'Benin', 'Bolivia (Plurinational State of)', 'Botswana']
    assert alpha_expected == [c.name for c in alpha]
    assert beta_expected == [c.name for c in beta]


def test_item_pagination_with_multiple_criteria(prepared_cdao):
    large_and_rich = SearchDict().add_filter("area", 100000, ">").add_filter("gdp_per_capita", 10000, ">")
    alpha = prepared_cdao.get_items_page(Country, large_and_rich, None, limit=5, desc=False)
    beta = prepared_cdao.get_items_page(Country, large_and_rich, alpha[-1], limit=5, desc=False)
    alpha_expected = ['Argentina', 'Australia', 'Canada', 'Chile', 'Finland']
    beta_expected = ['France', 'Germany', 'Greece', 'Greenland', 'Iceland']
    assert alpha_expected == [c.name for c in alpha]
    assert beta_expected == [c.name for c in beta]
