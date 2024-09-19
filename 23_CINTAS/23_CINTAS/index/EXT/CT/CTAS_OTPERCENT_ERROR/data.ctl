import data
into table "EXT"."CTAS_OTPERCENT_ERROR"
from 'data.csv'
    record delimited by '\n'
    field delimited by ','
    optionally enclosed by '"'
error log 'data.err'
