#
# __str__()-related functions
#

def basic_str(obj):
    """
    Handy for writing quick and dirty __str__() implementations.
    """
    return obj.__class__.__name__ + ': ' + obj.__repr__()


#
# date formats and utilities
#

YYYY_MM_DD_DATE_FORMAT = '%Y-%m-%d'  # e.g., '2017-01-17'


#
# SQL utilities
#

# "chunk" size of rows to fetch. used by batched_rows(cursor). value from `chunk_size=2000`:
# https://docs.djangoproject.com/en/2.2/ref/models/querysets/#iterator
SQL_ROWS_BATCH_SIZE = 2000


def batched_rows(cursor):
    """
    Generator that retrieves rows from `cursor` in batches of size SQL_ROWS_BATCH_SIZE.

    :param cursor: a cursor
    :return: next row from cursor
    """
    while True:
        rows = cursor.fetchmany(SQL_ROWS_BATCH_SIZE)
        if not rows:
            break

        for row in rows:
            yield row
