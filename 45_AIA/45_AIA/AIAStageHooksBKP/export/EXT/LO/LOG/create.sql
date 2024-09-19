CREATE procedure Log(in inText clob) 
sql security invoker 
as 
begin 
	using sqlscript_print as dbms_output;	
 	declare   vText varchar2(4000);
	declare exit handler for sqlexception
        begin rollback;
            resignal;
        end;	

  	begin autonomous transaction

	  	vText = substr(inText, 1, 4000);

    	insert into cs_debug_custom (text, value) values ('STAGEHOOK_' || vText, 1);
    	commit;
   
   	end;
 dbms_output:print_line('STAGEHOOK_' || vText);
   end; -- Log