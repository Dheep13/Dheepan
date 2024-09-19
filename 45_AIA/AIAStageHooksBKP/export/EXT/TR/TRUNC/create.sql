CREATE function ext.trunc(in in_date date, in in_periodtype varchar(255)) 
returns x date 
language sqlscript sql security invoker as begin 
DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN x := null;

END;

select
    case
        when :in_periodtype = 'MONTH' then to_date(to_varchar(:in_date, 'MMYYYY'), 'MMYYYY')
        when :in_periodtype = 'YEAR' then to_date(to_varchar(:in_date, 'YYYY'), 'YYYY')
        when :in_periodtype = 'DAY' then to_date(to_varchar(:in_date, 'DDMMYYYY'), 'DDMMYYYY')
        WHEN :in_periodtype = 'QUARTER'
        and SUBSTR_AFTER(QUARTER(TO_DATE(:IN_DATE, 'YYYY-MM-DD'), 4), '-') = 'Q1' THEN to_date(to_varchar(:IN_DATE, 'YYYY') || '04', 'YYYYMM')
        WHEN :in_periodtype = 'QUARTER'
        and SUBSTR_AFTER(QUARTER(TO_DATE(:IN_DATE, 'YYYY-MM-DD'), 4), '-') = 'Q2' THEN to_date(to_varchar(:IN_DATE, 'YYYY') || '07', 'YYYYMM')
        WHEN :in_periodtype = 'QUARTER'
        and SUBSTR_AFTER(QUARTER(TO_DATE(:IN_DATE, 'YYYY-MM-DD'), 4), '-') = 'Q3' THEN to_date(to_varchar(:IN_DATE, 'YYYY') || '10', 'YYYYMM')
        WHEN :in_periodtype = 'QUARTER'
        and SUBSTR_AFTER(QUARTER(TO_DATE(:IN_DATE, 'YYYY-MM-DD'), 4), '-') = 'Q4' THEN to_date(to_varchar(:IN_DATE, 'YYYY') || '01', 'YYYYMM')
        else null
    end into x
from
    dummy;

end