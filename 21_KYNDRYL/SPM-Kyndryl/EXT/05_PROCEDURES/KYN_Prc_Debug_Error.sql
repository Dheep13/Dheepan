--liquibase formatted sql

--changeset jcadby:KYN_Prc_Debug_Error splitStatements:false stripComments:false
--comment: Create procedure
--ignoreLines:1
set schema ext;

CREATE OR REPLACE PROCEDURE EXT.KYN_Prc_Debug_Error (
  IN i_error_code      integer,
  IN i_error_message   varchar(4000) default null,
  IN i_uuid            varchar(100) default null
)
LANGUAGE SQLSCRIPT default schema ext
AS
BEGIN
  declare v_current_connection integer := current_connection;
  begin autonomous transaction
    insert into ext.kyn_debug (connection_id, text, value, uuid) values (:v_current_connection, '['||:i_error_code || ']: ' || :i_error_message, :i_error_code, :i_uuid);
    commit;
  end;
END;
