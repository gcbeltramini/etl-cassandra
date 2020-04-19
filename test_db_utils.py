from pytest import mark

from db_utils import create_insert_command

@mark.parametrize('table_name, table_info, expected', [
    ('foo', '(bar INT, baz TEXT, qwerty1 TEXT)',
     'INSERT INTO foo (bar, baz, qwerty1)\nVALUES (%s, %s, %s);'),
    ('my_table', '(c1 TEXT, c2 INT, PARTITION KEY ((c1), c2));',
     'INSERT INTO my_table (c1, c2)\nVALUES (%s, %s);'),
])
def test_create_insert_command(table_name, table_info, expected):
    result = create_insert_command(table_name, table_info)
    assert result == expected
