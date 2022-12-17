"""Test cases for templating functionality."""

from dataclasses import dataclass


@dataclass
class MyNestedArg:
    """Dummy class for testing argument rendering."""

    another_property: str


@dataclass
class MyArg:
    """Dummy class for testing argument rendering."""

    my_arg_property: MyNestedArg


MUNG_SYMBOL = "%s"
EMPLOYEE = {
    "first_name": "Tom",
    "last_name": "Smith",
    "username": "tsmith",
    "email": "tsmith@email",
    "department_id": 589,
}

# The ordering of these tuples is:
# - Template init arguments,
# - Template's expected argument's property,
# - Keyword arguments to call with template.render(...)
# - Expected return values from template.render(...)
GOOD_CASES = (
    (  # Valid but kind of dumb, we're not validating their SQL with the template obviously
        ("  #{arg1} #{arg2} #{arg3}  ",),
        (("arg1",), ("arg2",), ("arg3",)),
        {"arg1": 1, "arg2": "test", "arg3": 2.0},
        ("  %s %s %s  ", (1, "test", 2.0)),
    ),
    (  # Another valid but kind of dumb test checking direct replacement
        ("  !{arg1} !{arg2} !{arg3}  ",),
        (("arg1",), ("arg2",), ("arg3",)),
        {"arg1": 1, "arg2": "test", "arg3": 2.0},
        ("  1 test 2.0  ", ()),
    ),
    (  # Basic test case
        ("INSERT INTO my_table (#{my_arg.my_arg_property.another_property}, #{my_other_arg})",),
        (("my_arg", "my_arg_property", "another_property"), ("my_other_arg",)),
        {"my_arg": MyArg(MyNestedArg("test")), "my_other_arg": 5.4},
        ("INSERT INTO my_table (%s, %s)", ("test", 5.4)),
    ),
    (  # Repetitive replacement
        (
            "INSERT INTO employees VALUES "
            "  (#{e.first_name}, #{e.last_name}, #{e.username}, #{e.department_id}, #{e.email}) "
            "ON CONFLICT (username) DO "
            "UPDATE SET "
            "  email = #{e.email} "
            "  first_name = #{e.first_name} "
            "  last_name = #{e.last_name} "
            "  department_id = #{e.department_id} ",
        ),
        (
            ("e", "first_name"),
            ("e", "last_name"),
            ("e", "username"),
            ("e", "department_id"),
            ("e", "email"),
            ("e", "email"),
            ("e", "first_name"),
            ("e", "last_name"),
            ("e", "department_id"),
        ),
        {"e": EMPLOYEE},
        (
            "INSERT INTO employees VALUES "
            "  (%s, %s, %s, %s, %s) "
            "ON CONFLICT (username) DO "
            "UPDATE SET "
            "  email = %s "
            "  first_name = %s "
            "  last_name = %s "
            "  department_id = %s ",
            ("Tom", "Smith", "tsmith", 589, "tsmith@email", "tsmith@email", "Tom", "Smith", 589),
        ),
    ),
    (  # Test case for not mangling complex strings
        (
            "SELECT * FROM table_name WHERE"
            "  some_column = #{argument.member.sub_member} AND "
            '  json_column @> \'{"some": [{"nested": [{"json_in_sql": "#{another_argument}"}]}]}\' '
            " AND some_column != !{argument.member.sub_member}",
        ),
        (("argument", "member", "sub_member"), ("another_argument",), ("argument", "member", "sub_member")),
        {"argument": {"member": {"sub_member": "test"}}, "another_argument": "something"},
        (
            "SELECT * FROM table_name WHERE"
            "  some_column = %s AND "
            '  json_column @> \'{"some": [{"nested": [{"json_in_sql": "%s"}]}]}\' '
            " AND some_column != test",
            ("test", "something"),
        ),
    ),
)

# The ordering of these tuples is:
# - Template init arguments,
# - Exception message fragement expected
INVALID_CASES = (
    (  # Dangling bracket for variable
        (
            "INSERT INTO table VALUES (#{myarg1}, #{myarg2})"
            "  ON CONFLICT DO UPDATE"
            "SET mycol1 = #{myarg1"
            "WHERE mycol2 = #{marg2}",
        ),
        "SET mycol1 = #{myarg1",
    ),
    (("INSERT INTO TABLE !{}",), "!{}"),  # Empty replacement case 1
    (("INSERT INTO TABLE #{}",), "#{}"),  # Empty replacement case 2
    (("INSERT INTO table VALUES (#{!{marg3}})",), "#{!{marg3}}"),  # Nested template replacements case 1
    (("INSERT INTO table VALUES (!{!{marg3}})",), "!{!{marg3}}"),  # Nested template replacements case 2
    (("INSERT INTO table VALUES (!{#{marg3}})",), "!{#{marg3}}"),  # Nested template replacements case 3
    (("INSERT INTO table VALUES (#{#{marg3}})",), "#{#{marg3}}"),  # Nested template replacements case 4
)
