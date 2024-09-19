CREATE function ext.fn_quarter_period_label(in in_date date) 
returns x nvarchar(50) 
language sqlscript sql security invoker as begin 
declare v_periodlabel nvarchar(50);
DECLARE v_periodlabel_uc NVARCHAR(50);
DECLARE to_position INT;
DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN x := null;

END;


select MONTHNAME(ext.trunc(to_date(:in_date),'QUARTER'))  --get month name of start date of quarter
||' '|| EXTRACT (YEAR FROM TO_DATE (:in_date, 'YYYY-MM-DD'))  ||--extract year
' to '  ||CASE 
          WHEN SUBSTR_AFTER(QUARTER(TO_DATE(:in_date, 'YYYY-MM-DD'), 4), '-') = 'Q1'
          THEN MONTHNAME(to_date(to_varchar(:in_date, 'YYYY') || '06', 'YYYYMM')) || ' '  ||
          EXTRACT (YEAR FROM TO_DATE (:in_date, 'YYYY-MM-DD'))
          
          WHEN SUBSTR_AFTER(QUARTER(TO_DATE(:in_date, 'YYYY-MM-DD'), 4), '-') = 'Q2'
          THEN MONTHNAME(to_date(to_varchar(:in_date, 'YYYY') || '09', 'YYYYMM')) || ' '  ||
          EXTRACT (YEAR FROM TO_DATE (:in_date, 'YYYY-MM-DD'))
          
          WHEN SUBSTR_AFTER(QUARTER(TO_DATE(:in_date, 'YYYY-MM-DD'), 4), '-') = 'Q3'
          THEN MONTHNAME(to_date(to_varchar(:in_date, 'YYYY') || '12', 'YYYYMM')) || ' '  ||
          EXTRACT (YEAR FROM TO_DATE (:in_date, 'YYYY-MM-DD'))
          
          WHEN SUBSTR_AFTER(QUARTER(TO_DATE(:in_date, 'YYYY-MM-DD'),4), '-') = 'Q4'
          THEN MONTHNAME(to_date(to_varchar(:in_date, 'YYYY') || '04', 'YYYYMM')) || ' '  ||
          EXTRACT (YEAR FROM TO_DATE (:in_date, 'YYYY-MM-DD'))
          
          ELSE null
          END  into v_periodlabel_uc
from dummy;

    v_periodlabel := LOWER(:v_periodlabel_uc);
    v_periodlabel := CONCAT(UPPER(SUBSTRING(:v_periodlabel_uc, 1, 1)), SUBSTRING(:v_periodlabel, 2));

    to_position := INSTR(:v_periodlabel, 'to ');

    IF :to_position > 0 THEN
        x := 
            SUBSTRING(:v_periodlabel, 1, to_position + 2)||
            UPPER(SUBSTRING(:v_periodlabel, to_position + 3, 1))||
            SUBSTRING(:v_periodlabel, to_position + 4);
    END IF;
    
    


END