"""

Testing more flexibility in table structure and items

"""

from sqlitedao import SqliteDao, ColumnDict, TableItem, NoIndexError, DuplicateError
import os
import pytest
import uuid


TEST_DB_NAME = "test.db"
TEST_TABLE_NAME = "relations"
SURVEY_TABLE_NAME = "surveys"


# Test creating tables without primary key specified
@pytest.fixture
def dao():
    # Before test
    if os.path.exists(TEST_DB_NAME):
        os.remove(TEST_DB_NAME)
    # Pass dao instance to test
    dao = SqliteDao.get_instance(TEST_DB_NAME)
    columns = ColumnDict()\
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

# Test creating tables that accept nullable values
@pytest.fixture
def xdao():
    # Before test
    if os.path.exists(TEST_DB_NAME):
        os.remove(TEST_DB_NAME)
    # Pass dao instance to test
    dao = SqliteDao.get_instance(TEST_DB_NAME)
    columns = ColumnDict()\
        .add_column("id", "text", primary_key=True)\
        .add_column("answer1", "text")\
        .add_column("answer2", "integer")
    dao.create_table(SURVEY_TABLE_NAME, columns)
    yield dao
    # Deconstruct
    SqliteDao.terminate_instance(TEST_DB_NAME)
    if os.path.exists(TEST_DB_NAME):
        os.remove(TEST_DB_NAME)

# Test creating tables with multiple primary keys
@pytest.fixture
def ydao():
    # Before test
    if os.path.exists(TEST_DB_NAME):
        os.remove(TEST_DB_NAME)
    # Pass dao instance to test
    dao = SqliteDao.get_instance(TEST_DB_NAME)
    columns = ColumnDict()\
        .add_column("year", "integer", primary_key=True)\
        .add_column("month", "integer", primary_key=True)\
        .add_column("day", "integer", primary_key=True)\
        .add_column("mood", "text")
    dao.create_table("mood", columns)
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

    def update_relations(self, new_relation):
        self.relation = new_relation
        self.row_tuple["relation"] = new_relation


class Survey(TableItem):

    TABLE_NAME = SURVEY_TABLE_NAME
    INDEX_KEYS = ["id"]
    ALL_COLUMNS = {
        "id": str,
        "answer1": str,
        "answer2": int
    }

    def __init__(self, row_tuple=None, **kwargs):
        super().__init__(row_tuple, **kwargs)
        self.load_tuple()

    def load_tuple(self):
        self.id = self.row_tuple["id"]
        self.answer1 = self.row_tuple["answer1"]
        self.answer2 = self.row_tuple["answer2"]

    def set_answers(self, answer1, answer2):
        self.answer1 = answer1
        self.answer2 = answer2
        self.row_tuple["answer1"] = answer1
        self.row_tuple["answer2"] = answer2


class Mood(TableItem):

    TABLE_NAME = "mood"
    INDEX_KEYS = ["year", "month", "day"]
    ALL_COLUMNS = {
        "year": int,
        "month": int,
        "day": int,
        "mood": str
    }

    def __init__(self, row_tuple=None, **kwargs):
        super().__init__(row_tuple, **kwargs)
        self.load_tuple()

    def load_tuple(self):
        self.year = self.row_tuple["year"]
        self.month = self.row_tuple["month"]
        self.day = self.row_tuple["day"]
        self.mood = self.row_tuple["mood"]


def test_item_creation_for_no_index_table(dao):
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


def test_refute_item_updates_for_no_index_table(dao):
    rel = Relation(name1="US", name2="China", relation=3)
    dao.insert_item(rel)
    rels = dao.get_items(Relation, {"name1": "US"})
    item = rels[0]
    item.update_relations(5)
    with pytest.raises(NoIndexError) as e:
        dao.update_item(item)
    with pytest.raises(NoIndexError) as e:
        dao.update_items([item])
    with pytest.raises(NoIndexError) as e:
        dao.delete_item(item)


def test_item_delete_for_no_index_table(dao):
    rel1 = Relation(name1="US", name2="Russia", relation=2)
    rel2 = Relation(name1="US", name2="China", relation=3)
    rel3 = Relation(name1="US", name2="UK", relation=9)
    rel4 = Relation(name1="Russia", name2="China", relation=7)
    dao.insert_items([rel1, rel2, rel3, rel4])
    assert dao.get_row_count(TEST_TABLE_NAME) == 4
    dao.delete_rows(Relation.TABLE_NAME, {"name2": "China"})
    assert dao.get_row_count(TEST_TABLE_NAME) == 2


def test_insert_item_with_null_fields(xdao):
    curr_id = str(uuid.uuid4())
    sur1 = Survey(id=curr_id)
    xdao.insert_item(sur1)
    found_item = xdao.find_item(Survey(id=curr_id))
    assert found_item.answer1 == None
    assert found_item.answer2 == None
    assert found_item.row_tuple["answer1"] is None
    assert found_item.row_tuple["answer2"] is None


def test_update_for_null_valued_items(xdao):
    curr_id = str(uuid.uuid4())
    sur1 = Survey(id=curr_id)
    xdao.insert_item(sur1)
    found_item = xdao.find_item(Survey(id=curr_id))
    found_item.set_answers("Yes", 1)
    xdao.update_item(found_item)
    found_item_again = xdao.find_item(Survey(id=curr_id))
    assert found_item_again.answer1 == "Yes"
    assert found_item_again.answer2 == 1


def test_refuting_insertion_for_items_of_different_type(xdao):
    curr_id = str(uuid.uuid4())
    sur1 = Survey(id=curr_id)
    rel1 = Relation(name1="US", name2="Russia", relation=2)
    with pytest.raises(ValueError) as e:
        xdao.insert_items([sur1, rel1])


def test_insert_items_for_multiple_primary_key_cols(ydao):
    mooda = Mood(year=2020, month=12, day=24, mood="happy")
    moodb = Mood(year=2020, month=12, day=25, mood="happy")
    moodc = Mood(year=2020, month=12, day=26, mood="sad")
    ydao.insert_items([mooda, moodb, moodc])
    with pytest.raises(DuplicateError) as e:
        dupmood = Mood(year=2020, month=12, day=26, mood="happy")
        ydao.insert_item(dupmood)
