import data
into table "EXT"."STEL_RPT_CFG_LOOKUP"
from 'data.csv'
    record delimited by '\n'
    field delimited by ','
    optionally enclosed by '"'
error log 'data.err'
