CREATE function gettenantid()
returns v_tenantid varchar(20)
as
begin 
	
declare x varchar(20);
select top 1 tenantid 
into x
from cs_tenant;	
	
v_tenantid := x;

end