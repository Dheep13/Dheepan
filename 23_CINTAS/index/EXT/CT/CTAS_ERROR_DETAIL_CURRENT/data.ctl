import data
into table "EXT"."CTAS_ERROR_DETAIL_CURRENT"
from 'data.csv'
    record delimited by '\n'
    field delimited by ','
    optionally enclosed by '"'
error log 'data.err'
