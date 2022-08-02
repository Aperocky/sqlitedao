"""

Testing more flexibility in table structure and items

"""

from sqlitedao import SqliteDao, ColumnDict, TableItem, NoIndexError
import os
import pytest


TEST_DB_NAME = "test.db"
TEST_TABLE_NAME = "relations"


# Test creating tables without primary key specified
@pytest.fixture
def dao():
    # Before test
    if os.path.exists(TEST_DB_NAME):
        os.remove(TEST_DB_NAME)
    # Pass dao instance to test
    dao = SqliteDao.get_instance(TEST_DB_NAME)
    columns = ColumnDict()
    columns\
        .add_column("name1", "text")\
        .add_column("name2", "text")\
        .add_column("relation", "integer")
    create_table_indexes = {
        "name1_index": ["name1"],
        "name2_index": ["name2"]
    }
    dao.create_table(TEST_TABLE_NAME, columns, create_table_indexes)
    yield dao
    # Deconstruct
    SqliteDao.terminate_instance(TEST_DB_NAME)
    if os.path.exists(TEST_DB_NAME):
        os.remove(TEST_DB_NAME)


class Relation(TableItem):

    TABLE_NAME = TEST_TABLE_NAME
    INDEX_KEYS = []
    ALL_COLUMNS = {
        "name1": str,
        "name2": str,
        "relation": int
    }

    def __init__(self, row_tuple=None, **kwargs):
        super().__init__(row_tuple, **kwargs)
        self.load_tuple()

    def load_tuple(self):
        self.name1 = self.row_tuple["name1"]
        self.name2 = self.row_tuple["name2"]
        self.relation = self.row_tuple["relation"]


def test_item_creation(dao):
    rel1 = Relation(name1="US", name2="Russia", relation=2)
    rel2 = Relation(name1="US", name2="China", relation=3)
    rel3 = Relation(name1="US", name2="UK", relation=9)
    rel4 = Relation(name1="Russia", name2="China", relation=7)
    dao.insert_items([rel1, rel2, rel3, rel4])
    with pytest.raises(NoIndexError) as e:
        dao.find_item(Relation(name1="Russia"))
    usrels = dao.get_items(Relation, {"name1": "US"})
    assert len(usrels) == 3
    rurels = dao.get_items(Relation, {"name1": "Russia"})
    assert len(rurels) == 1
    assert rurels[0].relation == 7
