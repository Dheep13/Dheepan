--liquibase formatted sql

--changeset jcadby:KYN_TxnAssign_Variable splitStatements:false stripComments:false
--comment: Create table
create column table EXT.KYN_TxnAssign_Variable(
  processingunitseq bigint not null,
	variableseq 	  bigint not null,
	variable_name 	varchar(255) not null,
	territoryseq 	  bigint not null,
	territory_name 	varchar(127) not null,
	titleseq 		    bigint,
	title_name 		  varchar(127),
	positionseq 	  bigint,
	position_name 	nvarchar(127),
  default_flag    tinyint default 0,
  eventtypeseq    bigint,
  eventtypeid     varchar(40)
);
