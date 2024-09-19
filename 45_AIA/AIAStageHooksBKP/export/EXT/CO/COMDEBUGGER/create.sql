
CREATE  procedure comDebugger(in i_objName varchar2(1000),in  i_objContent varchar2(4000)) 
as
begin 
begin autonomous transaction

  

    insert into sh_debugger values (i_objName, current_timestamp, i_objContent);
    commit;

end;
end --comDebugger;

    