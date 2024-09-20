--liquibase formatted sql

--changeset jcadby:KYN_Config1 splitStatements:false stripComments:false
--comment: Create table
create table EXT.KYN_Config (
  name varchar(255),
  value varchar(4000)
);

--changeset jcadby:KYN_Config2 splitStatements:false stripComments:false
--comment: Comment on table
comment on table EXT.KYN_Config is 'Config table';

--changeset jcadby:KYN_Config3 splitStatements:false stripComments:false
--comment: Create index
create unique index EXT.KYN_Config_idx on EXT.KYN_Config (name);

--changeset jcadby:KYN_Config4 splitStatements:false stripComments:false
--comment: Add entries
do begin
delete from ext.KYN_Config;
-- pipeline schedule time
insert into ext.KYN_Config values ('pipeline_schedule_sec', '10');
commit;
end;