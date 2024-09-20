--liquibase formatted sql

--changeset jcadby:KYN_Prc_TQ2Com_Run splitStatements:false stripComments:false
--comment: Create procedure
--ignoreLines:1
set schema ext;

CREATE OR REPLACE PROCEDURE EXT.KYN_Prc_TQ2Com_Run(
  out o_filename varchar(255), 
  in i_pipelinerunseq bigint default null
)
LANGUAGE SQLSCRIPT default schema ext
AS
BEGIN
  ext.kyn_lib_tq2com:run();
  o_filename = ::CURRENT_OBJECT_NAME||'_'||to_char(current_timestamp, 'YYYYMMDD_HH24MISS')||'.txt';
END;
