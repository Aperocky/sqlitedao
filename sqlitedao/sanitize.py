# This validates the parameters to prevent injection attacks.


def validate_table_name(table_name):
    # Validate table and index names
    # Index is a special table in SQLITE
    if not isinstance(table_name, str):
        raise ValueError("Table name must be a string")

    if not table_name:
        raise ValueError("Table name cannot be empty")

    # these are valid characters, but not having them is a
    # opinionated choice for sqlitedao.
    dangerous_chars = [";", "--", "/", "\\", "'", '"', "`", " "]

    for char in dangerous_chars:
        if char in table_name:
            raise ValueError(
                f"Table name contains dangerous character sequence: '{char}'"
            )

    return True


def quote_string(string):
    escaped_string = string.replace('"', '""')
    return f'"{escaped_string}"'
