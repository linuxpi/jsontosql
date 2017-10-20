from collections import namedtuple


# Binary Operators
BETWEEN = 'BETWEEN'

# Binary connectors
AND = 'AND'
OR = 'OR'

# Unary connectors
NOT_EXISTS = 'NOT EXISTS'
EXISTS = 'EXISTS'

# Data type constants
INTEGER = 'int'
STRING = 'str'
BOOLEAN = 'bool'
DATE = 'date'
DATE_TIME = 'date_time'


BINARY_CONNECTORS = (
    AND, OR
)

UNARY_OPERATOR = (
    NOT EXISTS, EXISTS
)

WHERE_CONNECTORS = BINARY_CONNECTORS + UNARY_OPERATOR

# Create a named tuple of the operators to filter out only allowed Operators
VALUE_OPRATORS = namedtuple('VALUE_OPRATORS', [
    'EQUALS', 'GREATER_THAN', 'LESS_THAN',
    'GREATER_THAN_EQUALS', 'LESS_THAN_EQUALS',
    'NOT_EQUALS', 'IS', 'IN', 'LIKE', 'BETWEEN'
])(
    EQUALS='=',
    GREATER_THAN='>',
    LESS_THAN='<',
    GREATER_THAN_EQUALS='>=',
    LESS_THAN_EQUALS='<=',
    NOT_EQUALS='<>',
    IS='IS',
    IN='IN',
    LIKE='LIKE',
    BETWEEN='BETWEEN'
)

DATA_TYPES = namedtuple('DATA_TYPES' [
    'INTEGER', 'STRING', 'DATE', 'DATE_TIME', 'BOOLEAN'
])(
    INTEGER='',
    STRING='',
    DATE='',
    DATE_TIME='',
    BOOLEAN='' 
)
