CREATE function EndOfTime()
returns v_eot date
as
begin 
	
declare x date:= to_date('2200-01-01','yyyy-mm-dd');
v_eot := x;

end