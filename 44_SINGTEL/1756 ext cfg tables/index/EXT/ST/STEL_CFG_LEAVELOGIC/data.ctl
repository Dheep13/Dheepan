import data
into table "EXT"."STEL_CFG_LEAVELOGIC"
from 'data.csv'
    record delimited by '\n'
    field delimited by ','
    optionally enclosed by '"'
error log 'data.err'
