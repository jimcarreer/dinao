"""Test cases for templating functionality."""

# The ordering of these tuples is (<Template init arguments>, <expected munged sql>, <expected arguments>)
GOOD_CASES = (
    (  # Valid but kind of dumb, we're not validating their SQL with the template obviously
        ("  #{arg1} #{arg2} #{arg3}  ", "%s"),
        "  %s %s %s  ",
        (("arg1",), ("arg2",), ("arg3",)),
    ),
    (  # Basic test case
        ("INSERT INTO my_table (#{my_arg.my_arg_property.another_property}, #{my_other_arg})", "%s"),
        "INSERT INTO my_table (%s, %s)",
        (("my_arg", "my_arg_property", "another_property"), ("my_other_arg",)),
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
            "%s",
        ),
        (
            "INSERT INTO employees VALUES "
            "  (%s, %s, %s, %s, %s) "
            "ON CONFLICT (username) DO "
            "UPDATE SET "
            "  email = %s "
            "  first_name = %s "
            "  last_name = %s "
            "  department_id = %s "
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
    ),
    (  # Test case for not mangling complex strings
        (
            "SELECT * FROM table_name WHERE"
            "  some_column = #{argument.member.sub_member} AND "
            '  json_column @> \'{"some": [{"nested": [{"json_in_sql": "#{another_argument}"}]}]}\' ',
            "%s",
        ),
        (
            "SELECT * FROM table_name WHERE"
            "  some_column = %s AND "
            '  json_column @> \'{"some": [{"nested": [{"json_in_sql": "%s"}]}]}\' '
        ),
        (("argument", "member", "sub_member"), ("another_argument",)),
    ),
)
