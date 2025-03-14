"""

Test basic dao/orm functionalities.

"""

from sqlitedao import SqliteDao, TableItem, SearchDict, DuplicateError
from .dao_test import prepopulated_dao
from .dao_test import TEST_TABLE_NAME
import pytest


class Player(TableItem):
    # Old (<0.4.0 pattern, use PlayerX pattern below for more brevity)

    TABLE_NAME = TEST_TABLE_NAME
    INDEX_KEYS = ["name"]

    def __init__(self, row_tuple=None, name=None, position=None, age=None, height=None):
        if row_tuple is not None:
            self.row_tuple = row_tuple
            self.load_tuple()
        else:
            self.name = name
            self.position = position
            self.age = age
            self.height = height
            self.sync_tuple()

    def sync_tuple(self):
        self.row_tuple = {
            "name": self.name,
            "position": self.position,
            "age": self.age,
            "height": self.height,
        }

    def load_tuple(self):
        self.name = self.row_tuple["name"]
        self.position = self.row_tuple["position"]
        self.age = self.row_tuple["age"]
        self.height = self.row_tuple["height"]

    def grow(self):
        self.age += 1
        self.sync_tuple()


# Test new init changes while maintaining compatibility
class PlayerX(TableItem):
    TABLE_NAME = TEST_TABLE_NAME
    INDEX_KEYS = ["name"]
    ALL_COLUMNS = {"name": str, "position": str, "age": int, "height": str}

    def __init__(self, row_tuple=None, **kwargs):
        super().__init__(row_tuple, **kwargs)
        self.load_tuple()

    def sync_tuple(self):
        self.row_tuple = {
            "name": self.name,
            "position": self.position,
            "age": self.age,
            "height": self.height,
        }

    def load_tuple(self):
        self.name = self.row_tuple["name"]
        self.position = self.row_tuple["position"]
        self.age = self.row_tuple["age"]
        self.height = self.row_tuple["height"]

    def grow(self):
        self.load_tuple()
        self.age += 1
        self.sync_tuple()


zion = Player(name="Zion Williamson", position="PF", age=20, height="6-6")
harden = Player(name="James Harden", position="SG", age=30, height="6-5")
zionx = PlayerX(name="Zion Williamson", position="PF", age=20, height="6-6")


def test_insert_item(xdao):
    xdao.insert_item(zion)
    assert len(xdao.search_table(TEST_TABLE_NAME, {})) == 4
    zion_from_duke = Player(
        xdao.search_table(TEST_TABLE_NAME, {"name": "Zion Williamson"})[0]
    )
    assert zion == zion_from_duke


def test_insert_item_x(xdao):
    xdao.insert_item(zionx)
    assert len(xdao.search_table(TEST_TABLE_NAME, {})) == 4
    zion_from_duke = PlayerX(
        xdao.search_table(TEST_TABLE_NAME, {"name": "Zion Williamson"})[0]
    )
    assert zion == zion_from_duke
    zion_from_duke = xdao.find_item(PlayerX(name="Zion Williamson"))
    assert zion == zion_from_duke


def test_update_item(xdao):
    xdao.insert_item(zion)
    middle_zion = Player(
        xdao.search_table(TEST_TABLE_NAME, {"name": "Zion Williamson"})[0]
    )
    middle_zion.grow()
    xdao.update_item(middle_zion)
    assert (
        xdao.search_table(TEST_TABLE_NAME, {"name": "Zion Williamson"})[0]["age"] == 21
    )


def test_update_item_x(xdao):
    xdao.insert_item(zionx)
    middle_zion = PlayerX(
        xdao.search_table(TEST_TABLE_NAME, {"name": "Zion Williamson"})[0]
    )
    middle_zion.grow()
    xdao.update_item(middle_zion)
    assert (
        xdao.search_table(TEST_TABLE_NAME, {"name": "Zion Williamson"})[0]["age"] == 21
    )


def test_insert_items(xdao):
    xdao.insert_items([zion, harden])
    assert len(xdao.search_table(TEST_TABLE_NAME, {})) == 5


def test_insert_duplicate(xdao):
    with pytest.raises(DuplicateError) as e:
        xdao.insert_items([zion, harden])
        xdao.insert_item(zion)
    xdao.insert_items([zion, harden])
    # Should raise no error if inserted in batch
    xdao.insert_items([zion])


def test_insert_or_update(xdao):
    lebron = xdao.find_item(Player(name="LeBron James"))
    lebron.grow()
    xdao.insert_item(lebron, True)
    lebron_now = xdao.find_item(Player(name="LeBron James"))
    assert lebron_now.age == 36


def test_update_items(xdao):
    xdao.insert_items([zion, harden])
    search = SearchDict().add_filter("age", 40, "<")
    current = [Player(c) for c in xdao.search_table(TEST_TABLE_NAME, search)]
    for each in current:
        each.grow()
    xdao.update_items(current)
    assert (
        xdao.search_table(TEST_TABLE_NAME, {"name": "Zion Williamson"})[0]["age"] == 21
    )
    assert xdao.search_table(TEST_TABLE_NAME, {"name": "James Harden"})[0]["age"] == 31


def test_delete_item(xdao):
    xdao.delete_item(Player(name="Michael Jordan"))
    assert len(xdao.search_table(TEST_TABLE_NAME, {})) == 2


def test_get_items(xdao):
    xdao.insert_items([zion, harden])
    search = SearchDict().add_filter("age", 40, "<")
    youth = xdao.get_items(Player, search)
    assert zion in youth
    assert len(youth) == 3


def test_get_items_x(xdao):
    xdao.insert_items([zion, harden])
    search = SearchDict().add_filter("age", 40, "<")
    youth = xdao.get_items(PlayerX, search)
    assert zion in youth
    assert len(youth) == 3


def test_get_items_orderby(xdao):
    xdao.insert_items([zion, harden])
    search = SearchDict().add_filter("age", 40, "<")
    youth = xdao.get_items(Player, search, order_by=["age"])
    assert youth[2] == zion


def test_get_items_orderby_x(xdao):
    xdao.insert_items([zion, harden])
    search = SearchDict().add_filter("age", 40, "<")
    youth = xdao.get_items(PlayerX, search, order_by=["age"])
    assert youth[2] == zion


def test_get_items_offset(xdao):
    xdao.insert_items([zion, harden])
    players = xdao.get_items(
        Player, {}, order_by=["age"], desc=False, limit=2, offset=2
    )
    kobe = xdao.find_item(Player(name="Kobe Bryant"))
    lebron = xdao.find_item(Player(name="LeBron James"))
    assert kobe in players
    assert lebron in players


def test_find_item(xdao):
    lebron = xdao.find_item(Player(name="LeBron James"))
    assert lebron.height == "6-8.5"
