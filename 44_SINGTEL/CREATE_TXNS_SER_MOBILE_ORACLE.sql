--------------------------------------------------------
--  DDL for Procedure OUTBOUND_LOG_STATS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "OUTBOUND_LOG_STATS" 
AS
   v_inbound_cfg_parameter   inbound_cfg_parameter%ROWTYPE;
   Message1                  VARCHAR (255) := 'Stage 1: File to Staging';
   Message2 VARCHAR (255)
         := 'Stage 3: Validation on Staging';
   Message3 VARCHAR (255)
         := 'Stage 4: Staging to ODS Table :';
   Message4 VARCHAR (255)
         := 'Stage 5: ODS Table to Callidus Final File : ';
   v_success_rec             NUMBER := 0;
   v_failure_rec             NUMBER := 0;
   v_sql                     VARCHAR2 (4000);
   v_statusflag              NUMBER;
BEGIN
 --  EXECUTE IMMEDIATE 'truncate table Outbound_log_details drop storage';




   SELECT * INTO v_inbound_cfg_parameter FROM inbound_cfg_parameter;


--delete from Outbound_log_details
--where (FILE_TYPE,FILE_NAME,FILE_DATE) not in (select FILE_TYPE,
--FILE_NAME,
--FILE_DATE from inbound_cfg_parameter);
--
--commit;



dbms_output.put_line (v_inbound_cfg_parameter.file_date );

   -- Get Count for Stage 1

   SELECT COUNT ( * )
     INTO v_success_rec
     FROM inbound_data_staging
    WHERE (filetype, filename, filedate) IN
                (SELECT v_inbound_cfg_parameter.file_type,
                        v_inbound_cfg_parameter.file_name,
                        v_inbound_cfg_parameter.file_date
                   FROM DUAL);
/*
   INSERT INTO Outbound_log_details (FILE_TYPE,FILE_NAME,FILE_DATE,STEPS_PROCESSED,RECORDS_PROCESSED,RECORDS_REJECTED) 
       VALUES (v_inbound_cfg_parameter.file_type,
               v_inbound_cfg_parameter.file_name,
               v_inbound_cfg_parameter.file_date,
               Message1,
               v_success_rec,
               0);
*/
   COMMIT;

   -- Get count for Stage 2

   SELECT COUNT ( * )
     INTO v_failure_rec
     FROM inbound_data_staging
    WHERE (filetype, filename, filedate) IN
                (SELECT v_inbound_cfg_parameter.file_type,
                        v_inbound_cfg_parameter.file_name,
                        v_inbound_cfg_parameter.file_date
                   FROM DUAL)
          AND NVL (error_flag, 0) <> 0;

   v_success_rec := v_success_rec - v_failure_rec;

   INSERT INTO Outbound_log_details (FILE_TYPE,FILE_NAME,FILE_DATE,STEPS_PROCESSED,RECORDS_PROCESSED,RECORDS_REJECTED) 
       VALUES (v_inbound_cfg_parameter.file_type,
               v_inbound_cfg_parameter.file_name,
               v_inbound_cfg_parameter.file_date,
               Message2,
               v_success_rec,
               v_failure_rec);

   COMMIT;

   -- Get count for Stage 3
   v_failure_rec := 0;
   v_statusflag := 0;

   SELECT VALUE
     INTO v_statusflag
     FROM INBOUND_CFG_GENERICPARAMETER
    WHERE key = 'VALIDRECORDSTATUS';

   FOR i IN (SELECT DISTINCT nvl(b.tablename,a.tgttable) tgttable
               FROM inbound_cfg_txnfield a
               left outer join inbound_cfg_tgttable b
                on a.tgttable=b.tgttable
              WHERE filetype = v_inbound_cfg_parameter.file_type)
   LOOP
      v_sql := 'select count(*) from ';
      v_sql := v_sql || i.tgttable;
      v_sql := v_sql || ' where (filename,filedate) in ';
      v_sql :=
         v_sql
         || ' (select file_name,file_date from inbound_cfg_parameter) ';
      v_sql := v_sql || ' and nvl(recordstatus,0)<> ';
      v_sql := v_sql || v_statusflag;

      DBMS_OUTPUT.put_line (v_sql);

      EXECUTE IMMEDIATE v_sql INTO v_failure_rec;


      v_sql := 'select count(*) from ';
      v_sql := v_sql || i.tgttable;
      v_sql := v_sql || ' where (filename,filedate) in ';
      v_sql :=
         v_sql
         || ' (select file_name,file_date from inbound_cfg_parameter) ';
      v_sql := v_sql || ' and nvl(recordstatus,0)= ';
      v_sql := v_sql || v_statusflag;


      DBMS_OUTPUT.put_line (v_sql);

      EXECUTE IMMEDIATE v_sql INTO v_success_rec;

      Message3 := Message3 || ' ' || i.tgttable;

      INSERT INTO Outbound_log_details (FILE_TYPE,FILE_NAME,FILE_DATE,STEPS_PROCESSED,RECORDS_PROCESSED,RECORDS_REJECTED) 
          VALUES (v_inbound_cfg_parameter.file_type,
                  v_inbound_cfg_parameter.file_name,
                  v_inbound_cfg_parameter.file_date,
                  Message3,
                  v_success_rec,
                  v_failure_rec);

      COMMIT;
   END LOOP;


   -- Get count for Stage 4
   v_failure_rec := 0;
   v_statusflag := 0;



   SELECT VALUE
     INTO v_statusflag
     FROM INBOUND_CFG_GENERICPARAMETER
    WHERE key = 'VALIDRECORDSTATUS';

   FOR i IN (

   select rownum rn,tgttable from (SELECT DISTINCT nvl(b.tablename,a.tgttable) tgttable
               FROM inbound_cfg_txnfield a
               left outer join inbound_cfg_tgttable b
                on a.tgttable=b.tgttable
              WHERE filetype = v_inbound_cfg_parameter.file_type))
   LOOP
     v_sql := 'select count(*) from ';
      v_sql := v_sql || i.tgttable;
      v_sql := v_sql || ' where (filename,filedate) in ';
      v_sql :=
         v_sql
         || ' (select file_name,file_date from inbound_cfg_parameter) ';
      v_sql := v_sql || ' and nvl(recordstatus,0)<> ';
      v_sql := v_sql || v_statusflag;

      DBMS_OUTPUT.put_line (v_sql);

      EXECUTE IMMEDIATE v_sql INTO v_failure_rec;


      v_sql := 'select count(*) from ';
      v_sql := v_sql || i.tgttable;
      v_sql := v_sql || ' where (filename,filedate) in ';
      v_sql :=
         v_sql
         || ' (select file_name,file_date from inbound_cfg_parameter) ';
      v_sql := v_sql || ' and nvl(recordstatus,0)= ';
      v_sql := v_sql || v_statusflag;


      DBMS_OUTPUT.put_line (v_sql);

      EXECUTE IMMEDIATE v_sql INTO v_success_rec;


  --    Message4 := Message4 || ' ' || i.rn;

      INSERT INTO Outbound_log_details (FILE_TYPE,FILE_NAME,FILE_DATE,STEPS_PROCESSED,RECORDS_PROCESSED,RECORDS_REJECTED) 
          VALUES (v_inbound_cfg_parameter.file_type,
                  v_inbound_cfg_parameter.file_name,
                  v_inbound_cfg_parameter.file_date,
                  Message4 || ' ' || i.rn ,
                  v_success_rec,
                  v_failure_rec);

      COMMIT;
   END LOOP;
END;
