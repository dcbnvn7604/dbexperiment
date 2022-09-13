import random
import psycopg

from settings import PG_URI, TOTAL_RECORD, ROUND


def make_query_parameters(explain):
    _round = ROUND if not explain else 1
    sql = '''
        select id, text_field from entry where id in (%s)
    '''
    record_ids = []
    for i in range(_round):
        record_ids.append(random.randint(1, TOTAL_RECORD))
    sql_in = ', '.join(["%s"] * _round)
    sql = sql % sql_in

    with psycopg.connect(PG_URI) as conn:
        with conn.cursor() as cur:
            result = cur.execute(sql, record_ids).fetchall()

    parameters = []
    for (id, text_field, ) in result:
        space_count = text_field.count(' ')
        start_space = random.randint(1, space_count)
        start_pos = find_nth(text_field, ' ', start_space)
        end_pos = find_nth(text_field, ' ', start_space + 8)
        if end_pos > 0:
            parameters.append((id, text_field[start_pos:end_pos]))
        else:
            parameters.append((id, text_field[start_pos:]))
    return parameters


def find_nth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start
