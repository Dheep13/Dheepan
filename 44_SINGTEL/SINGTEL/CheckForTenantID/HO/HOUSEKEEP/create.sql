CREATE PROCEDURE EXT.HOUSEKEEP
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* ORIGSQL: SP_LOGGER ('HOUSEKEEP', 'Start Housekeeping-1', 0, null, 'Execution Completed') */
    CALL EXT.STEL_SP_LOGGER('HOUSEKEEP', 'Start Housekeeping-1', 0, NULL, 'Execution Completed');

    --delete from INBOUND_DATA_STAGING_ARCH where importdatetime <sysdate-45;
    --delete from lp_logger where datetime<sysdate-60;
    --delete from INBOUND_DATA_clpr where filedate <sysdate-90 ;
    --delete from INBOUND_DATA_ogpo where filedate <sysdate-90 ;
    --delete from INBOUND_DATA_ogpt where filedate <sysdate-90 ;
    --delete from INBOUND_DATA_gatxn where filedate <sysdate-90 ;--8 mins
    --delete from INBOUND_DATA_assignment where filedate <sysdate-90 ; -- 30 mins
    --delete from INBOUND_DATA_txn where filedate <sysdate-90 ;
    --delete from STEL_DATA_TXN_MOBILE where filedate <sysdate-90 ;
    --execute immediate 'ALTER TABLE INBOUND_DATA_STAGING_ARCH ';
    --execute immediate 'alter table INBOUND_DATA_STAGING_ARCH shrink space';
    --execute immediate 'ALTER TABLE lp_logger ';
    --execute immediate 'alter table lp_logger shrink space';
    --execute immediate 'ALTER TABLE INBOUND_DATA_clpr ';
    --execute immediate 'alter table INBOUND_DATA_clpr shrink space';
    --execute immediate 'ALTER TABLE INBOUND_DATA_ogpo ';
    --execute immediate 'alter table INBOUND_DATA_ogpo shrink space';

    --execute immediate 'ALTER TABLE INBOUND_DATA_ogpt ';
    --execute immediate 'alter table INBOUND_DATA_ogpt shrink space';

    --execute immediate 'ALTER TABLE INBOUND_DATA_gatxn ';
    --execute immediate 'alter table  INBOUND_DATA_gatxn shrink space';

    --execute immediate 'ALTER TABLE  INBOUND_DATA_assignment ';
    --execute immediate 'alter table   INBOUND_DATA_assignment shrink space';

    --execute immediate 'ALTER TABLE INBOUND_DATA_txn ';
    --execute immediate 'alter table INBOUND_DATA_txn shrink space';

    --execute immediate 'ALTER TABLE STEL_DATA_TXN_MOBILE ';
    --execute immediate 'alter table STEL_DATA_TXN_MOBILE shrink space';
    --execute immediate 'alter index INBOUND_DATA_ASSIGNMENT_INDEX3 rebuild online';
    --execute immediate 'alter index INBOUND_DATA_ASSIGNMENT_INDEX1 rebuild online';
    --execute immediate 'alter index INBOUND_DATA_TXN_INDEX2 rebuild online';
    --execute immediate 'alter index INBOUND_DATA_TXN_INDEX1 rebuild online';
    --execute immediate 'alter index INBOUND_DATA_TXN_INDEX3 rebuild online';
    --execute immediate 'alter index INBOUND_DATA_ASSIGNMENT_INDEX2 rebuild online';
    --commit;

    /* ORIGSQL: SP_LOGGER ('HOUSEKEEP', 'End Housekeeping-1', 0, null, 'Execution Completed') */
    CALL EXT.STEL_SP_LOGGER('HOUSEKEEP', 'End Housekeeping-1', 0, NULL, 'Execution Completed');
END