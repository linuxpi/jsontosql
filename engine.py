import MySQLdb

import datetime
from collections import namedtuple


class DynamicSQLGenerator(object):

    _all_tables = set()

    WHERE_CONDITION = 'where_condition'
    AND_CONDITION = 'and'
    OR_CONDITION = 'or'
    NOT_CONDITION = 'not'
    EXISTS_CONDITION = 'exists'
    NOT_EXISTS_CONDITION = 'not_exists'
    SUBQUERY = 'sub_query'

    BETWEEN_OPERATOR = 'between'

    INTEGER='INT',
    STRING='STR',
    DATE='DATE',
    DATE_TIME='DATE_TIME',
    BOOLEAN='BOOL' 

    VALUE_OPERATORS = namedtuple('VALUE_OPRATORS', [
        'equals', 'greater_than', 'less_than',
        'greater_than_equals', 'less_than_equals',
        'not_equals', 'is_op', 'in_op', 'like', 'between'
    ])(
        equals='=',
        greater_than='>',
        less_than='<',
        greater_than_equals='>=',
        less_than_equals='<=',
        not_equals='<>',
        is_op='IS',
        in_op='IN',
        like='LIKE',
        between=BETWEEN_OPERATOR
    )

    DATA_TYPES = namedtuple('DATA_TYPES', [
        'integer', 'string', 'date', 'date_time', 'boolean'
    ])(
        integer=INTEGER,
        string=STRING,
        date=DATE,
        date_time=DATE_TIME,
        boolean=BOOLEAN
    )


    def __init__(self):
        self.WHERE_CONDITION_MAPPING = {
            self.WHERE_CONDITION: self._generate_where_phrase,
            self.AND_CONDITION: self._parse_and_in_where,
            self.OR_CONDITION: self._parse_or_in_where,
            self.NOT_CONDITION: self._parse_not_in_where,
            self.EXISTS_CONDITION: self._parse_exists_in_where,
            self.SUBQUERY: self._parse_sub_query_in_where
        }

    def generate_sql(self, json_input):

        select_data, from_data, where_data, join_data = self._decouple_json(json_input)

        # Verify if the data's are in the expected type. ex list | dict 

        # These two function calls will set the _all_tables data,
        # This will help to verify the tables and columns in select_data
        from_phrase = self._parse_from(from_data)
        join_phrase = self._prase_join(join_data)

        # Columns mentioned in where clause will be verified in during parsing.
        where_phrase = self._parse_where(where_data)

        # This function will use the _all_tables to check,
        # if all the tables and columns mentioned in select_data are included in join and from data
        select_phrase = self._parse_select(select_data)

        return u'SELECT {select_phrase} FROM {from_phrase} {join_phrase} WHERE {where_phrase}'.format(
                    select_phrase=select_phrase,
                    from_phrase=from_phrase,
                    join_phrase=join_phrase,
                    where_phrase=where_phrase
                )

    def _decouple_json(self, json_input):
        try:
            return json_input['select'], json_input['from'], json_input.get('where', {}), json_input.get('join', {})
        except KeyError as e:
            raise KeyError('[{}] - key not found in json input'.format(
                e.args[0]
            ))

    def _parse_select(self, select_data):
        """
        Input - ((table_name, col_name), (), ...)
        """
        generated_phrase = bytearray('')

        for table_name, col_name in select_data:
            # Check if the table is present in the _all_tables involved in the SQL, and
            # To check if the col_name belongs to the table
            _validate_table_column(table_name, col_name)

            # If above validation was successful then append information to phrase
            generated_phrase.extend('{table}.{column},'.format(
                table=table_name, column=col_name
            ))

        if generated_phrase:
            # Remove the last comma
            generated_phrase.pop()
        else:
            # If no column provided, use `*`
            generated_phrase.append('*')

        return generated_phrase.decode('utf-8')

    def _parse_from(self, from_data):
        # Check if table exists in the DB
        if self._validate_table(from_data):
            # Add table to _all_tables
            self._all_tables.add(from_data)
            # Return the table as is
            return from_data
        else:
            raise ValueError('Invalid table name - [{}] in from clause'.format(
                from_data
            ))

    def _parse_where(self, where_data):

        generated_phrase = bytearray('')

        for key, value in where_data.iteritems():
            if key.lower() in self.WHERE_CONDITION_MAPPING:
                generated_phrase.extend(
                    bytearray(self.WHERE_CONDITION_MAPPING[key](value), 'utf-8')
                )
            else:
                raise ValueError(
                    u'Unexpected SQL keyword in WHERE data - [{}]'.format(
                        key
                    )
                )

        return generated_phrase.decode('utf-8')

    def _parse_join(self, join_data):
        return ''

    def _validate_table_column(self, table_name, col_name):
        # Use _all_tables to check column validity with table
        if table in self._all_tables:
            if col_name in self._all_tables[table]:
                return True
            else:
                raise ValueError(
                    'Invalid column - [{}] for table - [{}] in select clause'.format(
                        col_name, table_name
                    )
                )
        else:
            raise ValueError(
                'Invalid table - [{}] in select clause'.format(table_name)
            )
        return False

    def _generate_where_phrase(self, where):
        # Check if the data is in the form of a dict
        if not isinstance(where, dict):
            raise ValueError('Where condition data must be in dict.')

        # Get all required keys if present, else raise error
        try:
            data_type = where['data_type'].lower()
            primary_operator = where['primary_operator'].lower()
            primary_value = where['primary_value']
            attribute = where['attribute']
            table = where['table']
        except KeyError as e:
            raise KeyError(u'Missing key - [{}] in where condition dict'.format(e.args[0]))
        else:
            # Get optional secondary value
            secondary_value = where.get('secondary_value')

            # Check if secondary_value is present for binary operators
            if primary_operator == self.BETWEEN_OPERATOR and not secondary_value:
                raise ValueError(
                    u'Missing key - [secondary_value] for operator - [{}]'.format(
                        primary_operator
                    )
                )

            # Validate primary operator and data type
            try:
                primary_sql_operator = getattr(self.VALUE_OPERATORS, primary_operator)
                data_type = getattr(self.DATA_TYPES, data_type)
            except AttributeError as e:
                raise e
            else:
                # Check if the attribute and data_type are in compliance
                if not self._santize_attribute(table, attribute, data_type):
                    raise ValueError(
                        u'Invalid data_type selected for attribute - [{}] of table - [{}]'.format(
                            attribute, table
                        )
                    )

                # Check if the primary_value and data_type are in sync
                if not self._sanitize_value(primary_value, data_type):
                    raise ValueError(
                        u'Invalid data_type selected for primary_value - [{}]'.format(
                            value
                        )
                    )

                # Check if the secondary_value and data_type are in sync
                if secondary_value and not self._sanitize_value(
                    secondary_value, data_type
                ):
                    raise ValueError(
                        u'Invalid data_type selected for secondary_value - [{}]'.format(
                            value
                        )
                    )     

                # Make value sql proof. For ex: if abc is string convert it to 'abc'
                primary_sql_value, secondary_sql_value = self._convert_values([primary_value, secondary_value], data_type)

                generated_sql_phrase = None

                if primary_sql_operator == self.BETWEEN_OPERATOR:
                    generated_sql_phrase = '`{table}`.`{attribute}` {operator} {primary_value} AND {secondary_value}'.format(
                        table=table, attribute=attribute, 
                        primary_operator=primary_sql_operator, 
                        primary_value=primary_sql_value, 
                        secondary_value=secondary_sql_value
                    )
                else:
                    generated_sql_phrase = '`{table}`.`{attribute}` {operator} {primary_value}'.format(
                        operator=primary_sql_operator, table=table, 
                        attribute=attribute, primary_value=primary_sql_value, 
                    )
            return generated_sql_phrase

    def _parse_and_in_where(self, data):
        if not isinstance(data, list):
            raise ValueError(
                u'AND clause can only contain a list of (nested) conditions'
            )
        # data needs to be ANDed
        return self._parse_conditions_in_where(self.AND_CONDITION, data)

    def _parse_or_in_where(self, data):
        if not isinstance(data, list):
            raise ValueError(
                u'OR clause can only contain a list of (nested) conditions'
            )
        # data needs to be ORed
        return self._parse_conditions_in_where(self.OR_CONDITION, data)

    def _parse_exists_in_where(self, data):
        if not isinstance(data, dict):
            raise ValueError(
                u'EXISTS clause can only contain a dict'
            )
        return self._parse_conditions_in_where(self.EXISTS_CONDITION, data)
   
    def _parse_not_in_where(self, data):
        if not isinstance(data, dict):
            raise ValueError(
                u'NOT clause can only contain a dict of nested conditions'
            )
        # data needs to be NOTed
        return u'NOT {}'.format(
            self._parse_conditions_in_where(self.NOT_CONDITION, data)
        )

    def _parse_subquery_in_where(self, data):
        if not isinstance(data, dict):
            raise ValueError(
                u'SUBQUERY clause can only contain a dict'
            )
        # Create a new instance of DynamicSQLGenerator to parse a subquery
        sql_generator = DynamicSQLGenerator()
        return '( {} )'.format(sql_generator.generate_sql(data))
   
    def _parse_conditions_in_where(self, condition, data):
        if isinstance(data, list):
            # Call _parse_where for all dicts inside the list and return ANDed sring
            return u'( {} )'.format(
                u' {} '.format(condition).join(
                    self._parse_where(where_data) for where_data in data
                )
            )
        elif isinstance(data, dict):
            return u'( {} )'.format(self._parse_where(data))
        else:
            raise ValueError(
                u'[{}] inside where clause condition - [{}] is not supported'.format(
                    type(data), condition.upper()
                )
            )

    def _convert_values(self, values, data_type):
        if data_type in [self.STRING, self.DATE_TIME, self.DATE]:
            wrapper = '\'{value}\''
        else:
            wrapper = '{value}'

        return (wrapper.format(value=value) for value in values)

    def _santize_attribute(self, table, attribute, data_type):
        if table in self._all_tables:
            table_columns = self._get_all_columns(table)
            if attribute in table_columns and table_columns[attribute] == data_type:
                return True
        return False

    def _sanitize_value(self, primary_value, data_type):
        if data_type == self.INTEGER:
            try:
                int(primary_value)
            except ValueError:
                raise ValueError(
                    'Invalid value -[{}] for data_type - [{}]'.format(
                        primary_value, data_type
                    )
                )
        elif data_type == self.DATE:
            try:
                datetime.datetime.strptime(primary_value, '%Y-%m-%d')
            except ValueError as e:
                raise e            
        elif data_type == self.DATE_TIME:
            try:
                datetime.datetime.strptime(primary_value, '%Y-%m-%dT%H:%M:%S')
            except ValueError as e:
                raise e
        return True

    def _validate_table(self, table_name):
        # Check if table exists in the DB
        return True

    def _get_all_columns(self, table):
        return {}


test = DynamicSQLGenerator()

test._parse_where({
    "where_condition": {
        "primary_value": "abc",
        "data_type": "string",
        "primary_operator": "equals",
        "table": "table_name",
        "attribute": "col_name"
    }
})


test._parse_where({
        "and": [
            {
                "where_condition": {
                    "primary_value": "abc",
                    "data_type": "string",
                    "primary_operator": "equals",
                    "table": "table_name",
                    "attribute": "col_name"
                }
            },
            {
                "where_condition": {
                    "primary_value": "abc",
                    "data_type": "string",
                    "primary_operator": "equals",
                    "table": "table_name",
                    "attribute": "col_name"
                }
            }

        ]
})

# print test._parse_where({
#         "and": [
#             {
#                 "where_condition": {
#                     "primary_value": "abc",
#                     "data_type": "string",
#                     "primary_operator": "equals",
#                     "table": "table_name",
#                     "attribute": "col_name"
#                 }
#             },
#             {
#                 "not": {
#                     "or":[
#                                 {
#                                     "where_condition": {
#                                         "primary_value": "abc",
#                                         "data_type": "string",
#                                         "primary_operator": "equals",
#                                         "table": "table_name",
#                                         "attribute": "col_name"
#                                     }
#                                 },
#                                 {
#                                     "where_condition": {
#                                         "primary_value": "abc",
#                                         "data_type": "string",
#                                         "primary_operator": "equals",
#                                         "table": "table_name",
#                                         "attribute": "col_name"
#                                     }
#                                 }

#                         ]
#                 }
#             }

#         ]
# })

# print test._parse_where({
#         "and": [
#             {
#                 "where_condition": {
#                     "primary_value": "abc",
#                     "data_type": "string",
#                     "primary_operator": "equals",
#                     "table": "table_name",
#                     "attribute": "col_name"
#                 }
#             },
#             {
#                 "not": {
#                     "or":[
#                                 {
#                                     "not": {
#                                                 "where_condition": {
#                                                     "primary_value": "abc",
#                                                     "data_type": "string",
#                                                     "primary_operator": "equals",
#                                                     "table": "table_name",
#                                                     "attribute": "col_name"
#                                                 }
#                                             }
#                                 },
#                                 {
#                                     "where_condition": {
#                                         "primary_value": "abc",
#                                         "data_type": "string",
#                                         "primary_operator": "equals",
#                                         "table": "table_name",
#                                         "attribute": "col_name"
#                                     }
#                                 }

#                         ]
#                 }
#             }

#         ]
# })


print test._parse_where({
        "and": [
            {
                "where_condition": {
                    "primary_value": "abc",
                    "data_type": "string",
                    "primary_operator": "equals",
                    "table": "table_name",
                    "attribute": "col_name"
                }
            },
            {
                "not": {
                    "or":[
                                {
                                    "not": {
                                                "where_condition": {
                                                    "primary_value": "abc",
                                                    "data_type": "string",
                                                    "primary_operator": "equals",
                                                    "table": "table_name",
                                                    "attribute": "col_name"
                                                }
                                            }
                                },
                                {
                                    "where_condition": {
                                        "primary_value": "abc",
                                        "data_type": "string",
                                        "primary_operator": "equals",
                                        "table": "table_name",
                                        "attribute": "col_name"
                                    }
                                }

                        ]
                }
            }

        ]
})

