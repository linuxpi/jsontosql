import json
from json2sql import convert_operator
from json2sql import constants
from json2sql import sanitizer

# TODO - Implement table name attribute.  Must be here to do joins
# TODO - Implement comparison2, value2
def process_where(where):

    # Get all required keys if present, else raise error
    try:
        data_type = where.pop('data_type').lower()
        primary_operator = where.pop('primary_operator').upper()
        primary_value = where.pop('primary_value')
        attribute = where.pop('attribute')
        table = where.pop('table')
    except KeyError as e:
        raise KeyError(u'Missing key - [{}] in where JSON'.format(e.args[0]))
    else:
        # Check if secondary_value is present for binary operators
        if primary_operator == constants.BETWEEN and 'secondary_value' not in where:
            raise ValueError(
                u'Missing key - [secondary_value] for operator - [{}]'.format(
                    primary_operator
                )
            )

        # Get optional secondary value
        secondary_value = where.get('secondary_value')

        # Validate primary operator and date type
        try:
            primary_sql_operator = getattr(constants.VALUE_OPRATORS, primary_operator)
            data_type = getattr(constants.DATA_TYPES, data_type)
        except AttributeError as e:
            raise e
        else:
            # Check if the attribute and data_type are in sync
            if not sanitizer.santize_attribute(attribute, data_type):
                raise ValueError(
                    u'Invalid data_type selected for attribute - [{}]'.format(
                        attribute
                    )
                )

            # Check if the primary_value and data_type are in sync
            if not sanitizer.sanitize_value(primary_value, data_type):
                raise ValueError(
                    u'Invalid data_type selected for primary_value - [{}]'.format(
                        value
                    )
                )

            # Check if the secondary_value and data_type are in sync
            if secondary_value and not sanitizer.sanitize_value(
                secondary_value, data_type
            ):
                raise ValueError(
                    u'Invalid data_type selected for secondary_value - [{}]'.format(
                        value
                    )
                )     

            generated_sql_phrase = None

            if primary_operator == constants.BETWEEN:
                generated_sql_phrase = '`{}`.`{}` {} {} AND {}'.format(
                    table, attribute, primary_operator, 
                    primary_value, secondary_value
                )
            else:
                generated_sql_phrase = '`{}`.`{}` {} {}'.format(
                    table, attribute, primary_operator, primary_value
                )

        return generated_sql_phrase
