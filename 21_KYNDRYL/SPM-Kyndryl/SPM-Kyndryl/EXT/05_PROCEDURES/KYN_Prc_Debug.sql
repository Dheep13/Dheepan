--liquibase formatted sql

--changeset jcadby:KYN_Prc_Debug splitStatements:false stripComments:false
--comment: Create procedure
--ignoreLines:1
set schema ext;

CREATE OR REPLACE PROCEDURE EXT.KYN_Prc_Debug(
  IN i_text      varchar(4000) default null,
  IN i_value     decimal(25,10) default null,
  IN i_uuid      varchar(100) default null
)
LANGUAGE SQLSCRIPT default schema ext
AS
BEGIN
  declare v_current_connection integer := current_connection;
  begin autonomous transaction
    insert into ext.kyn_debug (connection_id, text, value, uuid) values (:v_current_connection, :i_text, :i_value, :i_uuid);
    commit;
  end;
END;
