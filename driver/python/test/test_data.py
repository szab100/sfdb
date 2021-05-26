""" Test queries to be used by tests."""

# dict containing query:number_of_results
QUERIES = {
    "DROP TABLE People IF EXISTS;": 0,
    "CREATE TABLE People (name string, age int64);": 0,
    "INSERT INTO People (name, age) VALUES ('bob', 67);": 0,
    "INSERT INTO People (name, age) VALUES ('jon', 44);": 0,
    "SELECT name FROM People WHERE age % 2 = 0;": 1,
    "UPDATE People SET age = age + 1 WHERE true;": 0,
    "SELECT name FROM People;": 2,
}

FILE_DESCRIPTOR_SET = (
    'descriptors {'
    '  file {'
    '    name: "sfdb/runtime/Map140698766613568.proto"'
    '    package: "sfdb.runtime"'
    '    message_type {'
    '      name: "Map140698766613568"'
    '      field {'
    '        name: "_1"'
    '        number: 1'
    '        label: LABEL_OPTIONAL'
    '        type: TYPE_INT64'
    '      }'
    '      field {'
    '        name: "_2"'
    '        number: 2'
    '        label: LABEL_OPTIONAL'
    '        type: TYPE_STRING'
    '      }'
    '    }'
    '  }'
    '}'
    'rows {'
    '  type_url: "/sfdb.runtime.Map140698766613568"'
    '  value: "\010\n\022\004test"'
    '}'
    'rows {'
    '  type_url: "/sfdb.runtime.Map140698766613568"'
    '  value: "\010\n\022\004test20"'
    '}'
    'rows {'
    '  type_url: "/sfdb.runtime.Map140698766613568"'
    '  value: "\010\n\022\004test20"'
    '}'
    'rows {'
    '  type_url: "/sfdb.runtime.Map140698766613568"'
    '  value: "\010\n\022\004test220"'
    '}'
)
