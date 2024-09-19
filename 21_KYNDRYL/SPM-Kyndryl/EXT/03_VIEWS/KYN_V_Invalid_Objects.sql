--liquibase formatted sql

--changeset jcadby:KYN_V_Invalid_Objects splitStatements:false stripComments:false
--comment: Create view
create or replace view EXT.KYN_V_Invalid_Objects as
select schema_name, object_name, object_type, oid, create_time
from (
select schema_name, function_name as object_name, 'FUNCTION' as object_type, function_oid as oid, create_time, is_valid from FUNCTIONS
union all
select schema_name, library_name as object_name, 'LIBRARY' as object_type, library_oid as oid, create_time, is_valid from LIBRARIES
union all
select schema_name, procedure_name as object_name, 'PROCEDURE' as object_type, procedure_oid as oid, create_time, is_valid from PROCEDURES
union all
select schema_name, synonym_name as object_name, 'SYNONYM' as object_type, synonym_oid as oid, create_time, is_valid from SYNONYMS
union all
select schema_name, trigger_name as object_name, 'TRIGGER' as object_type, trigger_oid as oid, create_time, is_valid from TRIGGERS
union all
select schema_name, view_name as object_name, 'VIEW' as object_type, view_oid as oid, create_time, is_valid from views 
)
where is_valid != 'TRUE' 
and schema_name = 'EXT';