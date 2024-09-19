CREATE function ext.trunc_bkp_31May2024( in in_date date,in in_periodtype varchar(255))
returns x date 
language sqlscript
sql security invoker 
as
begin 
DECLARE EXIT HANDLER FOR SQLEXCEPTION 
BEGIN 
x:= null;	
END;

select case when  :in_periodtype = 'MONTH' then to_date(to_varchar(:in_date,'MMYYYY'),'MMYYYY')
			when :in_periodtype = 'YEAR' then to_date(to_varchar(:in_date,'YYYY'),'YYYY') 	
			when :in_periodtype = 'DAY'	then to_date(to_varchar(:in_date,'DDMMYYYY'),'DDMMYYYY')
			else null end 
		into x 
		from dummy;
	
end