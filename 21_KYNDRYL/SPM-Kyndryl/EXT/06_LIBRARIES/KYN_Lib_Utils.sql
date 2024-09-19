--liquibase formatted sql

--changeset jcadby:KYN_Lib_Utils splitStatements:false stripComments:false
--comment: Create library
--ignoreLines:1
set schema ext;

create or replace library EXT.KYN_Lib_Utils default schema ext as
begin

  private variable c_eot constant date := to_Date('22000101','YYYYMMDD');
  
  public function get_unittype(in i_unittypeseq bigint) returns v_ret varchar(40) as
  begin
    DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299
    begin
      -- return null
    END;  
    if :i_unittypeseq is not null then
      select name into v_ret from cs_unittype where removedate = :c_eot and unittypeseq = :i_unittypeseq;
    end if;
  end;
  
  public function get_config(in i_name varchar(255), in i_date timestamp default null) returns v_ret nvarchar(255) as
  begin
      
    DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299
    begin
      -- return null
    END;

    select cell.stringvalue into v_ret
    from cs_relationalmdlt mdlt
    join cs_mdltdimension dim0 on 
      mdlt.ruleelementseq = dim0.ruleelementseq 
      and dim0.removedate = :c_eot 
      and dim0.dimensionslot = 0
    join cs_mdltindex ind0 on
      ind0.ruleelementseq = dim0.ruleelementseq 
      and ind0.removedate = :c_eot 
      and ind0.dimensionseq = dim0.dimensionseq
    join cs_mdltcell cell on
      cell.mdltseq = mdlt.ruleelementseq
      and cell.removedate = :c_eot
      and cell.dim0index = ind0.ordinal
    where mdlt.name = 'LT_DI_Config'
    and mdlt.removedate = :c_eot
    and cell.effectivestartdate <= ifnull(:i_date, current_timestamp)
    and cell.effectiveenddate > ifnull(:i_date, current_timestamp)
    and upper(ind0.minstring) = upper(:i_name);

  end;
  
  public function get_tenant_id returns v_ret varchar(4) as
  begin
    select tenantid into v_ret from cs_period limit 1;
  end;

  -- sets varchar field to null if it is a string of length 0
  public procedure null_empty_string(IN i_table_name varchar(255)) as
  begin
    declare v_sql clob;
    declare v_debug varchar(4000);
    declare cursor c_cols for 
      select * from table_columns 
      where upper(table_name) = upper(:i_table_name)
      and schema_name = 'EXT'
      and data_type_name = 'VARCHAR'
      order by position;
    
    v_sql := 'update '||'ext.'||:i_table_name||' set ';
    
    for x as c_cols
    do
      v_sql := :v_sql ||:x.column_name||' = case when length('||x.column_name||')=0 then null else '||x.column_name||' end,';
    end for;
    
    v_sql := substr(:v_sql, 1, length(:v_sql)-1);
    v_debug := substr(:v_sql, 1, 4000);
    
    ext.kyn_prc_debug(:v_debug);
    
    execute immediate :v_sql;
    commit;
  end;

  public procedure copy_data(
    IN i_schema_name varchar(255) default null,
    IN i_source_table varchar(255),
    IN i_target_table varchar(255)
  ) as
  begin
    declare v_schema_name varchar(255) := ifnull(:i_schema_name, current_schema);
    declare v_sql clob;
    declare v_count integer;
    declare cursor c_cols for
      select ifnull(src.column_name, 'NULL')||' as '||tgt.column_name||',' as sql_text
      from table_columns tgt
      left outer join table_columns src on
        lower(src.table_name) = lower(:i_source_table)
        and lower(src.schema_name) = lower(:v_schema_name)
        and src.column_name = tgt.column_name
      where lower(tgt.table_name) = lower(:i_target_table)
      and lower(tgt.schema_name) = lower(:v_schema_name)
      order by tgt.position;
      
    select count(*) into v_count 
    from tables 
    where upper(table_name) = upper(:i_source_table) 
    and upper(schema_name) = upper(:v_schema_name);
    
    if :v_count != 1 then
      SIGNAL SQL_ERROR_CODE 10000 SET MESSAGE_TEXT = 'Invalid table name "'||:i_source_table||'"';
    end if;

    select count(*) into v_count 
    from tables 
    where upper(table_name) = upper(:i_target_table)
      and upper(schema_name) = upper(:v_schema_name);
    if :v_count != 1 then
      SIGNAL SQL_ERROR_CODE 10000 SET MESSAGE_TEXT = 'Invalid table name "'||:i_target_table||'"';
    end if;
    
    v_sql := 'insert into '||:v_schema_name||'.' || :i_target_table || ' select ';
    for x as c_cols
    do
      v_sql := :v_sql ||' ' || :x.sql_text;
    end for;
    
    v_sql := substr(v_sql,1, length(v_sql)-1);
    
    v_sql := :v_sql ||' from '||:v_schema_name||'.'||:i_source_table;
    
    execute immediate :v_sql;
    commit;
  
  end;

  public procedure drop_table_if_exists (
    IN i_schema_name varchar(255) default null,
    IN i_table_name varchar(255)
  )
  as
  begin
    declare v_schema_name varchar(255) := ifnull(:i_schema_name, current_schema);
    declare v_table_name varchar(255);
    declare v_sql clob;
    declare v_count integer;
 
    select count(*), max(table_name) into v_count, v_table_name
    from table_columns
    where upper(table_name) = upper(:i_table_name)
    and schema_name = :v_schema_name;
    
    if :v_count > 1 then
      SIGNAL SQL_ERROR_CODE 10000 SET MESSAGE_TEXT = 'Cannot uniquely identify "'||:i_table_name||'"';
    end if;
    v_sql := 'drop table ' || :v_schema_name || '."' || :v_table_name ||'"';
    ext.kyn_prc_debug(:v_sql);
    execute immediate :v_sql;
  end;

end;