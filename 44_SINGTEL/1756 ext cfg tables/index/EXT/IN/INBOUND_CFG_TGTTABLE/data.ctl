import data
into table "EXT"."INBOUND_CFG_TGTTABLE"
from 'data.csv'
    record delimited by '\n'
    field delimited by ','
    optionally enclosed by '"'
error log 'data.err'
