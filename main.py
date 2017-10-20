from json2sql import engine

test = engine.DynamicSQLGenerator()

test._generate_where_phrase({
    "primary_value": "abc",
    "data_type": "integer",
    "primary_operator": "=",
    "table": "table_name",
    "attribute": "col_name"
})
