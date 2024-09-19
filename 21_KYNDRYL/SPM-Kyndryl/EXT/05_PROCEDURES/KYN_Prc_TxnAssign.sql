--liquibase formatted sql

--changeset jcadby:KYN_Prc_TxnAssign splitStatements:false stripComments:false
--comment: Create procedure
--ignoreLines:1
set schema ext;

CREATE OR REPLACE PROCEDURE EXT.KYN_Prc_TxnAssign(
  in i_periodSeq bigint default null,
  in i_calendarSeq bigint default null,
  in i_processingUnitSeq bigint default null,
  in i_tentantId varchar(4) default null,
  in i_pipelineRunSeq bigint default null, 
  in i_runMode varchar(255) default null
)
LANGUAGE SQLSCRIPT default schema ext
AS
BEGIN
  kyn_lib_txnassign:run(i_periodSeq, i_calendarSeq, i_processingUnitSeq, i_tentantId, i_pipelineRunSeq, i_runMode);
END;
