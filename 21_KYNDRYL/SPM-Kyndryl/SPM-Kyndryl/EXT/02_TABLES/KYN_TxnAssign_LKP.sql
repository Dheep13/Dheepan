--liquibase formatted sql

--changeset jcadby:KYN_TxnAssign_LKP1 splitStatements:false stripComments:false
--comment: Create table
create column table ext.KYN_TxnAssign_LKP(
  processingunitSeq   bigint not null,
  periodseq           bigint not null,
  period_name         varchar(50) not null,
  variableseq         bigint not null,
  variable_name       varchar(255) not null,
  eventtypeseq        bigint,
  eventtypeid         varchar(40),
  territoryseq        bigint not null,
  territory_name      varchar(127) not null,
  categoryseq         bigint,
  category_name       varchar(127),
  classifierseq       bigint,
  classifierid        nvarchar(127),
  planning_accountseq bigint not null,
  planning_accountid  nvarchar(255) not null,
  bp_accountseq       bigint not null,
  bp_accountid        nvarchar(255) not null,
  positionseq         bigint not null,
  position_name       nvarchar(127) not null
);

--changeset jcadby:KYN_TxnAssign_LKP2 splitStatements:false stripComments:false
--comment: Create synonym
create public synonym KYN_TXNASSIGN_LKP for ext.KYN_TXNASSIGN_LKP;

--changeset jcadby:KYN_TxnAssign_LKP3 splitStatements:false stripComments:false
--comment: grant for ext
grant select on ext.KYN_TXNASSIGN_LKP to ext;

--changeset jcadby:KYN_TxnAssign_LKP4 splitStatements:false stripComments:false
--comment: grant for tcmp
grant select on ext.KYN_TXNASSIGN_LKP to tcmp;