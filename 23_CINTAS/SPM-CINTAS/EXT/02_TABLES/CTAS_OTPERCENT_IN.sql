--liquibase formatted sql

--changeset deepan:CTAS_SP_OTPERCENT splitStatements:false stripComments:false
--comment: Create procedure
--ignoreLines:1

CREATE COLUMN TABLE EXT.CTAS_OTPERCENT_IN(
	LOCATION VARCHAR(255),
	MVGR2 VARCHAR(255),
	CONFIRMEDSERV VARCHAR(255),
	TOTALSERV VARCHAR(255),
	COMPENSATIONDATE VARCHAR(255)
)
UNLOAD PRIORITY 5 AUTO MERGE;
