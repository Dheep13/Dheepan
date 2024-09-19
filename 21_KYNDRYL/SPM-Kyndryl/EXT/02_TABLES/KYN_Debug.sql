--liquibase formatted sql

--changeset jcadby:KYN_Debug1 splitStatements:false stripComments:false
--comment: rename table
rename table EXT.KYN_DEBUG to EXT.KYN_DEBUG_old;

--changeset jcadby:KYN_Debug2 splitStatements:false stripComments:false
--comment: Create table
CREATE COLUMN TABLE EXT.KYN_DEBUG (
  datetime      timestamp default current_timestamp,
  text          varchar(4000),
  value         decimal(25,10),
  connection_id integer,  
  UUID          varchar(100)
);

--changeset jcadby:KYN_Debug3 splitStatements:false stripComments:false
--comment: Comment on table
comment on table EXT.KYN_DEBUG is 'Debug table';

--changeset jcadby:KYN_Debug4 splitStatements:false stripComments:false
--comment: Copy data
call ext.kyn_lib_utils:copy_data('ext', 'KYN_DEBUG_old','KYN_DEBUG');

--changeset jcadby:KYN_Debug5 splitStatements:false stripComments:false
--comment: drop old
drop table EXT.KYN_DEBUG_old;