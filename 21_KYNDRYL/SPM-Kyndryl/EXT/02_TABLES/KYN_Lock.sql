--liquibase formatted sql

--changeset jcadby:KYN_Lock1 splitStatements:false stripComments:false
--comment: Create table
create column table EXT.KYN_Lock
(
  PROCESS_NAME   VARCHAR(255),
  CONNECTION_ID  INTEGER,
  CLIENT_HOST    NVARCHAR(256),
  CLIENT_IP      VARCHAR(45),
  CLIENT_PID     BIGINT,
  USER_NAME      NVARCHAR(256),
  CREATE_DATE    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  REMOVE_DATE    TIMESTAMP DEFAULT '2200-01-01',
  MESSAGE        VARCHAR(4000)
);

--changeset jcadby:KYN_Lock2 splitStatements:false stripComments:false
--comment: Comment table
comment on table EXT.KYN_Lock is 'Lock info';

--changeset jcadby:KYN_Lock3 splitStatements:false stripComments:false
--comment: Create index
create unique index ext.KYN_Lock_idx on EXT.KYN_Lock(process_name, remove_date);