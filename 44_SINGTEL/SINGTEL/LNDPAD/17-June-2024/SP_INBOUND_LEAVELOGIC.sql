CREATE
OR REPLACE PROCEDURE EXT.SP_INBOUND_LEAVELOGIC () SQL SECURITY DEFINER
/*READS SQL DATA*/
-- this procedure cannot be read-only
AS BEGIN -- select TO_DATE(TO_VARCHAR(current_timestamp,'YYYY-MM-DD')) from dummy;
/* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

DECLARE v_validLeaveStatus VARCHAR (200) = 'V';
/* ORIGSQL: v_validLeaveStatus VARCHAR2(200):= 'V'; */
DECLARE v_invalidLeaveStatus VARCHAR (200) = 'X';
/* ORIGSQL: v_invalidLeaveStatus VARCHAR2(200):= 'X'; */
DECLARE v_Eot TIMESTAMP = TO_DATE ('22000101', 'yyyymmdd');
/* ORIGSQL: v_Eot DATE := TO_DATE('21990101','yyyymmdd') ; */
DECLARE v_Filedate timestamp = to_timestamp (TO_VARCHAR (current_timestamp, 'YYYY-MM-DD'));
/* ORIGSQL: v_Filedate date:=trunc(sysdate) ; */
DECLARE v_proc_name VARCHAR (127) = 'SP_INBOUND_LEAVELOGIC';


/* ORIGSQL: v_proc_name varchar2(127):='SP_INBOUND_LEAVELOGIC'; */
DECLARE v_parameter ROW LIKE ext.inbound_cfg_Parameter;


/* NOT CONVERTED! */
/* RESOLVE: Identifier not found: Table 'EXT.Inbound_cfg_Parameter' not found (for %ROWTYPE declaration) */
DECLARE v_rowcount BIGINT;


/* ORIGSQL: v_rowcount integer; */
/* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_PARAMETER' not found */
SELECT
  DISTINCT * INTO v_parameter
FROM
  EXT.Inbound_cfg_Parameter;


/* ORIGSQL: truncate table EXT.stel_TEMP_PERIODHIERARCHY ; */
EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.stel_TEMP_PERIODHIERARCHY';


/* ORIGSQL: execute immediate 'truncate table EXT.stel_TEMP_LOOKUP'; */
/* RESOLVE: Identifier not found: Table/view 'EXT.STEL_TEMP_LOOKUP' not found */
/* ORIGSQL: truncate table EXT.stel_TEMP_LOOKUP ; */
EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.stel_TEMP_LOOKUP';


/* ORIGSQL: execute immediate 'truncate table EXT.stel_TEMP_NONWORKINGDAYS'; */
/* RESOLVE: Identifier not found: Table/view 'EXT.STEL_TEMP_NONWORKINGDAYS' not found */
/* ORIGSQL: truncate table EXT.stel_TEMP_NONWORKINGDAYS ; */
EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.stel_TEMP_NONWORKINGDAYS';


/* ORIGSQL: insert into EXT.stel_TEMP_PERIODHIERARCHY(CALENDARNAME, PERIODTYPELEVEL, PERIODTYPENAME, PERIODNAME, PERIODSEQ, STARTDATE, ENDDATE, MONTHPERIODSEQ, MONTHNAME, MONTHSTARTDATE, MONTHENDDATE) SELECT CALENDAR(...) */
INSERT INTO
  EXT.STEL_TEMP_PERIODHIERARCHY (
    CALENDARNAME,
    PERIODTYPELEVEL,
    PERIODTYPENAME,
    PERIODNAME,
    PERIODSEQ,
    STARTDATE,
    ENDDATE,
    MONTHPERIODSEQ,
    MONTHNAME,
    MONTHSTARTDATE,
    MONTHENDDATE
  )
SELECT
  /* ORIGSQL: SELECT CALENDARNAME, PERIODTYPELEVEL, PERIODTYPENAME, PERIODNAME, PERIODSEQ, STARTDATE, ENDDATE, MONTHPERIODSEQ, MONTHNAME, MONTHSTARTDATE, MONTHENDDATE FROM EXT.stel_PERIODHIERARCHY@stelext; */
  CALENDARNAME,
  PERIODTYPELEVEL,
  PERIODTYPENAME,
  PERIODNAME,
  PERIODSEQ,
  STARTDATE,
  ENDDATE,
  MONTHPERIODSEQ,
  MONTHNAME,
  MONTHSTARTDATE,
  MONTHENDDATE
FROM
  EXT.STEL_PERIODHIERARCHY;


/* RESOLVE: Oracle Database link: Remote table/view 'EXT.STEL_PERIODHIERARCHY@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.STEL_PERIODHIERARCHY'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert into EXT.stel_TEMP_PERIODHIERARCHY :' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), 'INSERT into(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | 'Insert into EXT.stel_TEMP_PERIODHIERARCHY   :' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  'INSERT into EXT.stel_TEMP_PERIODHIERARCHY  Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || 'Insert into EXT.stel_TEMP_PERIODHIERARCHY   :' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/* ORIGSQL: insert into EXT.stel_Temp_lookup(NAME, MDLTCELLSEQ, EFFECTIVESTARTDATE, EFFECTIVEENDDATE, VALUE, STRINGVALUE, DATEVALUE, DIM0, DIM1, DIM2, DIM3, DIM4, DIM5, DIM6, DIM7, DIM8, DIM9, DIM10) SELECT NAME, MDL(...) */
INSERT INTO
  ext.stel_Temp_lookup (
    NAME,
    MDLTCELLSEQ,
    EFFECTIVESTARTDATE,
    EFFECTIVEENDDATE,
    VALUE,
    STRINGVALUE,
    DATEVALUE,
    DIM0,
    DIM1,
    DIM2,
    DIM3,
    DIM4,
    DIM5,
    DIM6,
    DIM7,
    DIM8,
    DIM9,
    DIM10
  )
SELECT
  /* ORIGSQL: SELECT NAME, MDLTCELLSEQ, EFFECTIVESTARTDATE, EFFECTIVEENDDATE, VALUE, STRINGVALUE, DATEVALUE, DIM0, DIM1, DIM2, DIM3, DIM4, DIM5, DIM6, DIM7, DIM8, DIM9, DIM10 FROM EXT.stel_LOOKUP@stelext; */
  NAME,
  MDLTCELLSEQ,
  EFFECTIVESTARTDATE,
  EFFECTIVEENDDATE,
  VALUE,
  STRINGVALUE,
  DATEVALUE,
  DIM0,
  DIM1,
  DIM2,
  DIM3,
  DIM4,
  DIM5,
  DIM6,
  DIM7,
  DIM8,
  DIM9,
  DIM10
FROM
  EXT.STEL_LOOKUP;


/* RESOLVE: Oracle Database link: Remote table/view 'EXT.STEL_LOOKUP@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.STEL_LOOKUP'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert into EXT.stel_TEMP_LOOKUP :' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), 'INSERT into EXT.stel_TEM(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | 'Insert into EXT.stel_TEMP_LOOKUP  :' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  'INSERT into EXT.stel_TEMP_LOOKUP Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || 'Insert into EXT.stel_TEMP_LOOKUP  :' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
select
  *
from
  ext.STEL_TEMP_NONWORKINGDAYS
  /* ORIGSQL: commit; */
  COMMIT;


/* ORIGSQL: insert into EXT.stel_TEMP_NONWORKINGDAYS (channel, nonworkdate) select to_char(channel), nonworkdate from EXT.stel_nonworkingdays@stelext union select to_char(lt.dim0), prd.startdate+rn-1 as nonwdate from (SE(...) */
/* Deepan : Replacing this insert into STEL_TEMP_NONWORKINGDAYS with a new sql below as this does not work in HANA*/
--     INSERT INTO ext.STEL_TEMP_NONWORKINGDAYS
--         (
--             channel, nonworkdate
--         )
--         SELECT   /* ORIGSQL: select to_char(channel), nonworkdate from EXT.stel_nonworkingdays@stelext */
--             TO_VARCHAR(channel),
--             nonworkdate
--         FROM
--             EXT.stel_nonworkingdays
--             /* RESOLVE: Oracle Database link: Remote table/view 'EXT.stel_nonworkingdays@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'EXT.stel_nonworkingdays'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
-- UNION
--     SELECT   /* ORIGSQL: select to_char(lt.dim0), prd.startdate+rn-1 as nonwdate from (SELECT ROW_NUMBER() OVER (ORDER BY 0*0) AS rn FROM SYS.OBJECTS where ROWNUM <=31) AS rn join EXT.stel_temp_periodhierarchy prd on prd.calendar(...) */
--         TO_VARCHAR(lt.dim0),
--         TO_DATE(ADD_SECONDS(TO_DATE(ADD_SECONDS(prd.startdate,(86400*(rn)))),(86400*-1))) AS nonwdate  /* ORIGSQL: prd.startdate+rn */
--                                                                                                       /* ORIGSQL: TO_DATE(ADD_SECONDS(prd.startdate,(86400*(rn)))) -1 */
--     FROM
--         (
--             SELECT   /* ORIGSQL: (select ROW_NUMBER() OVER (ORDER BY 0*0) rn from all_objects where ROWNUM <=31) rownum */
--                 ROW_NUMBER() OVER (ORDER BY 0*0) AS rn  
--             FROM
--                 SYS.OBJECTS  /* RESOLVE: Catalog reference(partly converted): Oracle catalog 'ALL_OBJECTS': verify conversion */
--                              /* ORIGSQL: all_objects (Oracle catalog) */
--             WHERE
--                 ROWNUM  <= 31  /* RESOLVE: ROWNUM pseudo-column(not converted): Cannot convert ROWNUM in INSERT statement, convert manually */
--         ) AS rn
--     INNER JOIN
--         EXT.stel_temp_periodhierarchy prd
--         ON prd.calendarname LIKE 'Singte%Mont%'
--         AND prd.periodtypename = 'month'
--     INNER JOIN
--         EXT.stel_temp_lookup lt
--         ON lt.name = 'LT_Working_Days_Channel'
--         AND IFNULL(lt.value,0) = 0  /* ORIGSQL: nvl(lt.value,0) */
--         AND lt.dim1 = TO_VARCHAR(TO_DATE(ADD_SECONDS(TO_DATE(ADD_SECONDS(prd.startdate,(86400*-1))),(86400*(rn.rn)))),'D');  /* ORIGSQL: to_char(prd.startdate-1+rn.rn,'D') */
--                                                                                                                              /* ORIGSQL: TO_DATE(ADD_SECONDS(prd.startdate,(86400*-1))) +rn.rn */
/* Deepan :The new logic for inserting into STEL_TEMP_NONWORKINGDAYS*/
INSERT INTO
  ext.STEL_TEMP_NONWORKINGDAYS (channel, nonworkdate)
SELECT
  TO_NVARCHAR(channel) AS channel,
  nonworkdate
FROM
  ext.stel_nonworkingdays
UNION
SELECT
  TO_NVARCHAR(lt.dim0) AS channel,
  add_days(prd.startdate, rn - 1) AS nonwdate
FROM
  (
    SELECT
      ROW_NUMBER() OVER () AS rn
    FROM
      cs_salestransaction
    LIMIT
      31
  ) AS rn
  JOIN ext.stel_temp_periodhierarchy prd ON prd.calendarname LIKE 'Singtel%Mont%'
  AND prd.periodtypename = 'month'
  JOIN ext.stel_temp_lookup lt ON lt.name = 'LT_Working_Days_Channel'
  AND ifnull(lt.value, 0) = 0
  AND lt.dim1 = TO_NVARCHAR(
    DAYOFWEEK(add_days(add_days(prd.startdate, - 1), rn.rn))
  );


-- select DAYOFWEEK(add_days(add_days(current_timestamp, - 1), 2)) from dummy;
-- select add_days(add_days(current_timestamp, - 1),2) from d
v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert into EXT.stel_TEMP_NONWORKINGDAYS :' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), 'INSERT into (...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | 'Insert into EXT.stel_TEMP_NONWORKINGDAYS   :' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  'INSERT into EXT.stel_TEMP_NONWORKINGDAYS  Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || 'Insert into EXT.stel_TEMP_NONWORKINGDAYS   :' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/* ORIGSQL: commit; */
COMMIT;


/* ORIGSQL: execute immediate 'truncate table EXT.stel_temp_workingdays'; */
/* RESOLVE: Identifier not found: Table/view 'EXT.STEL_TEMP_WORKINGDAYS' not found */
/* ORIGSQL: truncate table EXT.stel_temp_workingdays ; */
EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.stel_temp_workingdays';


/* ORIGSQL: insert into EXT.stel_temp_workingdays(CHANNEL, CALDATE, TABLENAME, WORKINGDAYSINENDMONTH, PERIODTYPENAME, PERIODNAME, PERIODSEQ) SELECT CHANNEL, CALDATE, TABLENAME, WORKINGDAYSINENDMONTH, PERIODTYPENAME, (...) */
INSERT INTO
  EXT.stel_temp_workingdays (
    CHANNEL,
    CALDATE,
    TABLENAME,
    WORKINGDAYSINENDMONTH,
    PERIODTYPENAME,
    PERIODNAME,
    PERIODSEQ
  )
SELECT
  /* ORIGSQL: SELECT CHANNEL, CALDATE, TABLENAME, WORKINGDAYSINENDMONTH, PERIODTYPENAME, PERIODNAME, PERIODSEQ FROM EXT.stel_WORKINGDAYS; */
  CHANNEL,
  CALDATE,
  TABLENAME,
  WORKINGDAYSINENDMONTH,
  PERIODTYPENAME,
  PERIODNAME,
  PERIODSEQ
FROM
  EXT.stel_WORKINGDAYS;


v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert into EXT.stel_TEMP_WORKINGDAYS:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), 'Insert into  STE(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | 'Insert into  EXT.stel_TEMP_WORKINGDAYS:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  'Insert into  EXT.stel_TEMP_WORKINGDAYS Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || 'Insert into  EXT.stel_TEMP_WORKINGDAYS:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/* ORIGSQL: commit; */
COMMIT;


--inbound_Data _leave should only have the latest file data
--stel_Work_leavedates persistes historical data. Can be housekep separately for a year ago 
/* ORIGSQL: DELETE FROM EXT.stel_WORK_LEAVEDATES WHERE elasid IN (SELECT elasid FROM ext.inbound_data_leave) ; */
DELETE FROM
  EXT.stel_WORK_LEAVEDATES
WHERE
  elasid
  /* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_DATA_LEAVE' not found */
  IN (
    SELECT
      /* ORIGSQL: (SELECT elasid FROM ext.inbound_data_leave) */
      elasid
    FROM
      ext.inbound_data_leave
  );


v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'DELETE existing records EXT.stel_WORK_LEAVEDATES :' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), 'DELE(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | 'DELETE existing records EXT.stel_WORK_LEAVEDATES   :' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  'DELETE existing records EXT.stel_WORK_LEAVEDATES Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || 'DELETE existing records EXT.stel_WORK_LEAVEDATES   :' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/* ORIGSQL: COMMIT; */
COMMIT;


/* ORIGSQL: INSERT INTO EXT.stel_WORK_LEAVEDATES SELECT geid, leavetype, leavetiming, TO_CHAR(startdate+cnt,'yyyymm') LeaveMonth, startdate + cnt LeaveDate, nw.nonworkdate AS nonworkdateFlag, elasid, pos.genericattri(...) */
INSERT INTO
  EXT.stel_WORK_LEAVEDATES
SELECT
  /* ORIGSQL: SELECT geid, leavetype, leavetiming, TO_CHAR(startdate+cnt,'yyyymm') LeaveMonth, startdate + cnt LeaveDate, nw.nonworkdate AS nonworkdateFlag, elasid, pos.genericattribute3 as channel FROM ext.inbound_Dat(...) */
  geid,
  leavetype,
  leavetiming,
  -- TO_VARCHAR(TO_DATE(ADD_SECONDS(startdate,(86400*(cnt)))),'yyyymm') AS LeaveMonth,  /* ORIGSQL: TO_CHAR(startdate+cnt,'yyyymm') */
  TO_VARCHAR(ADD_DAYS(startdate, cnt), 'yyyymm') AS LeaveMonth,
  /*Deepan : simplified version for LeaveMonth*/
  -- TO_DATE(ADD_SECONDS(startdate,(86400*(cnt)))) AS LeaveDate,  /* ORIGSQL: startdate + cnt */
  ADD_DAYS(startdate, cnt) AS LeaveDate,
  /*Deepan : simplified version for LeaveDate*/
  nw.nonworkdate AS nonworkdateFlag,
  elasid,
  pos.genericattribute3 AS channel
FROM
  ext.inbound_Data_leave le
  INNER JOIN (
    SELECT
      /* ORIGSQL: (SELECT ROW_NUMBER() OVER (ORDER BY 0*0)-1 cnt FROM all_objects) rownum */
      ROW_NUMBER() OVER (
        ORDER BY
          0 * 0
      ) -1 AS cnt
    FROM
      SYS.OBJECTS
      /* RESOLVE: Catalog reference(partly converted): Oracle catalog 'ALL_OBJECTS': verify conversion */
      /* ORIGSQL: all_objects (Oracle catalog) */
  ) AS run -- ON run.cnt <= (SECONDS_BETWEEN(le.startdate,le.enddate)/86400)   /* ORIGSQL: le.enddate-le.startdate */
  ON run.cnt <= DAYS_BETWEEN(le.startdate, le.enddate)
  /*Deepan : simplified version*/
  INNER JOIN cs_position pos ON pos.name = le.geid
  AND pos.removedate > CURRENT_TIMESTAMP
  /* ORIGSQL: sysdate */
  AND startdate BETWEEN pos.effectivestartdate
  AND add_days(pos.effectiveenddate, -1)
  LEFT OUTER JOIN EXT.stel_temp_NONWORKINGDAYS nw -- ON TO_DATE(ADD_SECONDS(startdate,(86400*(cnt)))) = nw.nonworkdate  /* ORIGSQL: startdate+cnt */
  ON ADD_DAYS(startdate, cnt) = nw.nonworkdate
  /*Deepan : simplified version*/
  AND nw.channel = pos.genericattribute3
  /* RESOLVE: Oracle Database link: Remote table/view 'cs_position@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'cs_position'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
WHERE
  IFNULL(le.leavestatus,: v_validLeaveStatus) <>: v_invalidLeaveStatus;


/* ORIGSQL: NVL(le.leavestatus,v_validleavestatus) */
v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '1. Insert into EXT.stel_WORK_LEAVEDATES:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), '1.Insert into (...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | '1. Insert into  EXT.stel_WORK_LEAVEDATES:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  '1.Insert into  EXT.stel_WORK_LEAVEDATES Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || '1. Insert into  EXT.stel_WORK_LEAVEDATES:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
--and lavetiming<>'Full';
--1 filter out non work days, factor in leavrtimining to get total leave days
-- aggregate by quarter and year
--  first remove ones that don't meet the threshold
--then group across leave type
--2. convert this back to ranges to determine consec days of leave for specific situation
--but still within a month
--assuming that diff types of leave dfo not count as consec leaves
/* ORIGSQL: EXECUTE immediate 'truncate table EXT.stel_Work_leaveconsec'; */
/* RESOLVE: Identifier not found: Table/view 'EXT.STEL_WORK_LEAVECONSEC' not found */
/* ORIGSQL: truncate table EXT.stel_Work_leaveconsec ; */
EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.stel_Work_leaveconsec';


/*
 
 #	Scenario	Proration	Note:
 1	5 days Annual 	(Expected Working Days \x96 5) /(Expected Working Days)	 Babu: Correct
 2	4 days Annual	100%	Babu:100% of the target, no proration
 3	4 days Annual + 2 days MC 	100%	 Babu: 100% of the target , no proration
 4	5 days Annual, followed immediately by 5 days MC	(Expected Working Days \x96 10) /(Expected Working Days)	5 and 5 are consecutive
 Babu: consider 10 days to prorate.
 5	5 days Annual, followed immediately by 4 days MC	(Expected Working Days \x96 9) /(Expected Working Days)	5 and 4 are consecutive
 Babu: 9 days to prorate, if it is consecutive
 6	5 days Annual, followed by 1 working day, followed by 4 days MC	(Expected Working Days \x96 5) /(Expected Working Days)	5 and 4 are NOT consecutive \x96 should we prorate using 9 or 5?
 Babu: Prorate for the 5 days of leave
 7	5 days Annual, followed by 1 working day, followed by 7 days MC	(Expected Working Days \x96 7) /(Expected Working Days)	5 and 7 are NOT consecutive \x96 should we prorate using 7 or 12?
 Babu: This is for the 12 days to prorate.
 
 
 */
--find consec days range regardless of leave type group by startdate of consec range
---consec days to consider
-----filter on leave types - no separation
----include holidays/non work days
--substract # of holidays/weekends from the number
--if there are more than 1 in a month with diff start dates, take the max, after filtering out anything <threshold
/* RESOLVE: Identifier not found: Table/view 'EXT.STEL_WORK_LEAVEDATES' not found */
/* RESOLVE: Identifier not found: Table/view 'EXT.STEL_CFG_LEAVELOGIC' not found */
/* RESOLVE: Identifier not found: Table/view 'EXT.STEL_CFG_LEAVECHANNEL' not found */
/* ORIGSQL: INSERT INTO EXT.stel_Work_leaveconsec (conseclogic, geid, leavetype, firstleavedate, channel, lastleavedate, leavedateexholidays, consecleavedays, rnk, periodname) select 'ONETYPE', geid, 'ONETYPE', MIN(l(...) */
INSERT INTO
  EXT.stel_Work_leaveconsec (
    conseclogic,
    geid,
    leavetype,
    firstleavedate,
    channel,
    lastleavedate,
    leavedateexholidays,
    consecleavedays,
    rnk,
    periodname
  )
SELECT
  /* ORIGSQL: select 'ONETYPE', geid, 'ONETYPE', MIN(leavedate), channel, MAX(leavedate), SUM(cnt), MAX(cnt) - SUM(CASE WHEN nonworkdateflag IS NULL THEN 0 ELSE 1 END) ConsecLeaveDays, RANK() OVER (PARTITION BY gei(...) */
  'ONETYPE',
  geid,
  'ONETYPE',
  MIN(leavedate),
  channel,
  MAX(leavedate),
  SUM(cnt),
  MAX(cnt) - SUM(
    CASE
      WHEN nonworkdateflag IS NULL THEN 0
      ELSE 1
    END
  ) AS ConsecLeaveDays,
  RANK() OVER (
    PARTITION BY geid,
    grp
    ORDER BY
      grp
  ),
  periodname
FROM
  (
    SELECT
      /* ORIGSQL: (select t.*, COUNT(*) OVER (PARTITION BY geid, grp) as cnt from (SELECT x.*, (x.leavedate - ROW_NUMBER() OVER (PARTITION BY geid ORDER BY leavedate)) AS grp FROM (SELECT * FROM EXT.stel_work_leavedates t)(...) */
      t.*,
      COUNT(*) OVER (PARTITION BY geid, grp) AS cnt
    FROM
      (
        SELECT
          /* ORIGSQL: (select x.*, (x.leavedate - ROW_NUMBER() OVER (PARTITION BY geid ORDER BY leavedate)) as grp from (SELECT * FROM EXT.stel_work_leavedates t) (select * from EXT.stel_work_leavedates t) */
          x.*,
          add_days(
            x.leavedate,
            - ROW_NUMBER() OVER (
              PARTITION BY geid
              ORDER BY
                leavedate
            )
          ) AS grp
          /*  ,(x.leavedate -
           row_number() over (partition by geid,leavetype order by leavedate)
           ) as grp2*/
        FROM
          (
            SELECT
              /* ORIGSQL: (select * from EXT.stel_work_leavedates t) */
              *
            FROM
              EXT.stel_work_leavedates t -- where geid='7a_6251874'
            UNION
            SELECT
              /* ORIGSQL: select distinct y.geid, y.leavetype, y.leavetiming, y.leavemonth, x.nonworkdate, x.nonworkdate, y.elasid, y.channel from EXT.stel_temp_NONWORKINGDAYS x INNER join EXT.stel_work_leavedates y on y.channel=x.cha(...) */
              DISTINCT y.geid,
              y.leavetype,
              y.leavetiming,
              y.leavemonth,
              x.nonworkdate,
              x.nonworkdate,
              y.elasid,
              y.channel
            FROM
              EXT.stel_temp_NONWORKINGDAYS x
              INNER JOIN EXT.stel_work_leavedates y ON y.channel = x.channel
              AND y.leavemonth = TO_VARCHAR(x.nonworkdate, 'YYYYMM')
              /* ORIGSQL: to_Char(x.nonworkdate,'YYYYMM') */
              INNER JOIN EXT.stel_work_leavedates z ON z.channel = x.channel
              AND z.geid = y.geid
              AND z.leavemonth = TO_VARCHAR(x.nonworkdate, 'YYYYMM')
              /* ORIGSQL: to_Char(x.nonworkdate,'YYYYMM') */
              AND z.leavedate > y.leavedate
            WHERE
              --y.geid='7a_6251874' and
              x.nonworkdate BETWEEN y.leavedate
              AND z.leavedate
          ) AS x
        WHERE
          x.leavetype IN (
            SELECT
              /* ORIGSQL: (select ll.leavetype from EXT.stel_cfg_leavelogic ll join EXT.stel_Cfg_leavechannel lc on lc.leavesetting=ll.leavesetting where consethreshold_reduction IS NOT NULL and lc.saleschannel=x.channel) */
              ll.leavetype
            FROM
              EXT.stel_cfg_leavelogic ll
              INNER JOIN EXT.stel_Cfg_leavechannel lc ON lc.leavesetting = ll.leavesetting
            WHERE
              consethreshold_reduction IS NOT NULL
              AND lc.saleschannel = x.channel
          )
      ) AS t
  ) AS t
  INNER JOIN EXT.STEL_PERIODHIERARCHY PRD ON LAST_DAY(PRD.Monthstartdate) = LAST_DAY(t.leavedate)
  AND prd.periodtypename = 'month'
  AND PRD.calendarname = 'Singtel Monthly Calendar'
  /* RESOLVE: Oracle Database link: Remote table/view 'EXT.STEL_PERIODHIERARCHY@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'EXT.STEL_PERIODHIERARCHY'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
GROUP BY
  geid,
  leavemonth,
  grp,
  channel,
  periodname
HAVING
  MAX(cnt) - SUM(
    CASE
      WHEN nonworkdateflag IS NULL THEN 0
      ELSE 1
    END
  ) >= 5;


--select distinct geid from EXT.stel_work_leavedates
/*
 
 SELECT COnsecLogic, geid, z.leavetype, leavedate as firstleavedate, channel
 , lastleavedate, leavedateexholidays, consecleavedays, rnk, periodname
 FROM
 (
 SELECT 'ONETYPE' COnsecLogic , x.geid, x.leavetype, x.leavedate, x.channel ,
 MAX(y.leavedate) lastleavedate,
 SUM( CASE WHEN y.nonworkdateflag IS NULL THEN 1 ELSE 0 END) +1 leavedateexholidays ,
 MAX(y.leavedate)- x.leavedate - SUM( CASE WHEN y.nonworkdateflag IS NULL THEN 0 ELSE 1 END) +1 consecleavedays ,
 row_number() over(partition BY x.geid, x.leavetype, MAX(y.leavedate)
 order by MAX(y.leavedate)- x.leavedate - SUM( CASE WHEN y.nonworkdateflag IS NULL THEN 0 ELSE 1 END) DESC) AS rnk
 FROM EXT.stel_work_leavedates x
 JOIN EXT.stel_work_leavedates y
 ON x.geid               =y.geid
 AND x.leavetype         =y.leavetype
 AND x.leavedate         <y.leavedate
 AND x.leavemonth        =y.leavemonth
 WHERE ( ( x.leavetiming = 'AM' AND y.leavetiming ='PM')
 OR (upper(x.leavetiming)       = 'FULL' AND upper(y.leavetiming) ='AM')
 OR( upper(x.leavetiming)       = 'FULL' AND upper(y.leavetiming) ='FULL') )
 GROUP BY x.geid, x.leavetype, x.leavedate, x.channel
 
 UNION
 SELECT 'DIFFTYPES' COnsecLogic , x.geid, max(x.leavetype) leavetype, x.leavedate, x.channel ,
 MAX(y.leavedate),
 SUM( CASE WHEN y.nonworkdateflag IS NULL THEN 1 ELSE 0 END) +1  ,
 MAX(y.leavedate)-x.leavedate - SUM( CASE WHEN y.nonworkdateflag IS NULL THEN 0 ELSE 1 END) +1 consecleavedays ,
 row_number() over(partition BY x.geid,  MAX(y.leavedate)
 order by MAX(y.leavedate)-x.leavedate - SUM( CASE WHEN y.nonworkdateflag IS NULL THEN 0 ELSE 1 END) DESC) AS rnk
 FROM EXT.stel_work_leavedates x
 JOIN EXT.stel_work_leavedates y
 ON x.geid               =y.geid
 AND x.leavedate         <y.leavedate
 AND x.leavemonth        =y.leavemonth
 
 WHERE ( ( x.leavetiming = 'AM' AND y.leavetiming ='PM')
 OR (upper(x.leavetiming)       = 'FULL' AND upper(y.leavetiming) ='AM')
 OR( upper(x.leavetiming)       = 'FULL' AND upper(y.leavetiming) ='FULL') )
 and x.leavetype in
 (
 select ll.leavetype
 from EXT.stel_cfg_leavelogic ll
 join EXT.stel_Cfg_leavechannel lc
 on lc.leavesetting=ll.leavesetting
 where consethreshold_reduction IS NOT NULL
 and lc.saleschannel=x.channel
 
 
 )
 GROUP BY x.geid,  x.leavedate, x.channel
 
 ) Z
 JOIN (Select lc.saleschannel, lc.frequency, ll.consethreshold_reduction
 , ll.reduceworkingdays, ll.nonconsecthreshold_reduction, ll.leavetype
 from EXT.stel_cfg_leavelogic ll
 join EXT.stel_Cfg_leavechannel lc
 on lc.leavesetting=ll.leavesetting
 ) L
 on L.saleschannel=Z.channel --and Z.consecleavedays>= nvl(consethreshold_reduction,0)
 and  L.leavetype=Z.leavetype
 JOIN EXT.stel_PERIODHIERARCHY@STELEXT PRD
 ON last_day(PRD.Monthstartdate) =  last_Day(Z.leavedate)
 and prd.periodtypename=L.frequency
 and PRD.calendarname                  ='Singtel Monthly Calendar'
 WHERE Z.rnk = 1
 -- and X.consecleavedays > (select consethreshold from EXT.stel_cfg_leavelogic s where s.leavetype=X.leavetype and
 ;
 */
v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '3. Insert into EXT.stel_WORK_LEAVECONSEC:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), '3. Insert int(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | '3. Insert into EXT.stel_WORK_LEAVECONSEC:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  '3. Insert into EXT.stel_WORK_LEAVECONSEC Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || '3. Insert into EXT.stel_WORK_LEAVECONSEC:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
--for the consec leaves
-- have to hit min threhsold on at least one leave type
--where this is hit, include the days around it for qualifying leavr types
--based on proration loic per channel, get the ratio.
/* LEave days
 Lave deductive days
 Target days
 Consec leave days (with 1 leave type)
 Consec leve days with multi leave types
 is available per geid and month
 */
-- get working days for each emp and popuylate fv
---get # of working days, factoring in hire date and termination date
--start from postion table, to make sure everyone has a value
/*
 Workingdays per month
 hiredate to end date
 */
/* ORIGSQL: EXECUTE immediate 'truncate table EXT.stel_work_LeaveCount'; */
/* RESOLVE: Identifier not found: Table/view 'EXT.STEL_WORK_LEAVECOUNT' not found */
/* ORIGSQL: truncate table EXT.stel_work_LeaveCount ; */
EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.stel_work_LeaveCount';


/* ORIGSQL: INSERT INTO EXT.stel_work_leavecount SELECT le.geid, le.leavecounttype, SUM(le.leavedays) LeaveDays, prd.periodname, prd.periodseq, MAX(le.leavemonth) LAstLEavemonth, channel FROM (SELECT geid, SUM(CASE W(...) */
INSERT INTO
  EXT.stel_work_leavecount
SELECT
  /* ORIGSQL: SELECT le.geid, le.leavecounttype, SUM(le.leavedays)LeaveDays, prd.periodname, prd.periodseq, MAX(le.leavemonth) LAstLEavemonth, channel FROM (SELECT geid, SUM(CASE WHEN leavetiming='FULL' THEN 1 ELSE(...) */
  le.geid,
  le.leavecounttype,
  SUM(le.leavedays) LeaveDays,
  prd.periodname,
  prd.periodseq,
  MAX(le.leavemonth) AS LAstLEavemonth,
  channel
FROM
  (
    SELECT
      /* ORIGSQL: (SELECT geid, SUM(CASE WHEN leavetiming='FULL' THEN 1 ELSE 0.5 END) AS leavedays, LeaveMonth, 'ALL' LeaveCountType, channel FROM EXT.stel_Work_leavedates WHERE nonworkdateflag IS NULL GROUP BY geid, Leave(...) */
      geid,
      SUM(
        CASE
          WHEN leavetiming = 'FULL' THEN 1
          ELSE 0.5
        END
      ) AS leavedays,
      LeaveMonth,
      'ALL' AS LeaveCountType,
      channel
    FROM
      EXT.stel_Work_leavedates
    WHERE
      nonworkdateflag IS NULL
    GROUP BY
      geid,
      LeaveMonth,
      channel
    UNION
    SELECT
      /* ORIGSQL: select geid, MAX(consecleavedays), to_Char(firstleavedate,'YYYYMM'), 'DEDUCTABLE', channel from EXT.stel_Work_leaveconsec group by geid, channel, to_Char(firstleavedate,'YYYYMM') */
      geid,
      MAX(consecleavedays),
      TO_VARCHAR(firstleavedate, 'YYYYMM'),
      /* ORIGSQL: to_Char(firstleavedate,'YYYYMM') */
      'DEDUCTABLE',
      channel
    FROM
      EXT.stel_Work_leaveconsec
    GROUP BY
      geid,
      channel,
      TO_VARCHAR(firstleavedate, 'YYYYMM')
      /* ORIGSQL: to_Char(firstleavedate,'YYYYMM') */
    UNION
    SELECT
      /* ORIGSQL: SELECT geid, SUM(CASE WHEN leavetiming='FULL' THEN 1 ELSE 0.5 END) AS leavedays, LeaveMonth, 'DEDUCTABLE' LeaveCountType, channel FROM EXT.stel_Work_leavedates ld INNER join EXT.stel_cfg_leavechannel lc on lc(...) */
      geid,
      SUM(
        CASE
          WHEN leavetiming = 'FULL' THEN 1
          ELSE 0.5
        END
      ) AS leavedays,
      LeaveMonth,
      'DEDUCTABLE' AS LeaveCountType,
      channel
    FROM
      EXT.stel_Work_leavedates ld
      INNER JOIN EXT.stel_cfg_leavechannel lc ON lc.saleschannel = ld.channel
      INNER JOIN EXT.stel_temp_periodhierarchy pd ON pd.periodtypename = lc.frequency -- AND ld.leavedate BETWEEN pd.startdate AND TO_DATE(ADD_SECONDS(pd.enddate,(86400*-1)))   /* ORIGSQL: pd.enddate-1 */
      AND ld.leavedate BETWEEN pd.startdate
      AND ADD_DAYS(pd.enddate, -1)
      /*Deepan : Simplified version*/
      AND pd.calendarname LIKE 'Singtel%Mon%'
      AND pd.periodtypename = 'month' -- this is extended to qtr and yr later
      INNER JOIN EXT.stel_Cfg_leavelogic ll ON ll.leavesetting = lc.leavesetting
      AND ll.reduceworkingdays = 'Y'
      AND ll.leavetype = ld.leavetype
      AND ll.consethreshold_reduction IS NULL
      AND IFNULL(ll.nonconsecthreshold_reduction, 0)
      /* ORIGSQL: nvl(ll.nonconsecthreshold_reduction,0) */
      <= (
        SELECT
          /* ORIGSQL: (Select COUNT(*) from EXT.stel_Work_leavedates s where s.elasid = ld.elasid group by s.elasid) */
          COUNT(*)
        FROM
          EXT.stel_Work_leavedates s
        WHERE
          s.elasid = ld.elasid
        GROUP BY
          s.elasid
      )
    WHERE
      ld.nonworkdateflag IS NULL --leave type is a type that is deductable
      --working days only
      ------------------------------
      -----------------------------
    GROUP BY
      geid,
      LEavemonth,
      channel
  ) AS le
  INNER JOIN EXT.STEL_PERIODHIERARCHY PRD ON TO_VARCHAR(PRD.Monthstartdate, 'YYYYMM') = le.LeaveMonth
  /* ORIGSQL: TO_CHAR(PRD.Monthstartdate,'YYYYMM') */
  /* RESOLVE: Oracle Database link: Remote table/view 'EXT.STEL_PERIODHIERARCHY@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'EXT.STEL_PERIODHIERARCHY'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
WHERE
  PRD.calendarname = 'Singtel Monthly Calendar'
GROUP BY
  le.geid,
  le.leavecounttype,
  prd.periodname,
  prd.periodseq,
  channel;


v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '2. Insert into EXT.stel_WORK_LEAVECOUNT:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), '2. Insert into(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | '2. Insert into EXT.stel_WORK_LEAVECOUNT:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  '2. Insert into EXT.stel_WORK_LEAVECOUN Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || '2. Insert into EXT.stel_WORK_LEAVECOUNT:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/* ORIGSQL: COMMIT; */
COMMIT;


/*
 
 
 1. Check if threhsold was crossed. set deductiable to 0 if it wasn't in the leavecount table
 2. For the ones where we set the FV value to 0 or 100, add the code at the end
 */
/* ORIGSQL: UPDATE ext.inbound_cfg_genericparameter SET value = (SELECT TO_CHAR(startdate,'YYYYMMDD') FROM cs_period WHERE periodseq=EXT.FN_GETCURRENTPERIOD() AND removedate >sysdate) WHERE KEY='PROCES(...) */
UPDATE
  ext.inbound_cfg_genericparameter
SET
  /* ORIGSQL: value = */
  value = (
    SELECT
      /* ORIGSQL: (SELECT TO_CHAR(startdate,'YYYYMMDD') FROM cs_period@STELEXT WHERE periodseq=EXT.FN_GETCURRENTPERIOD() AND removedate >sysdate) */
      TO_VARCHAR(startdate, 'YYYYMMDD')
    FROM
      cs_period
      /* RESOLVE: Oracle Database link: Remote table/view 'cs_period@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'cs_period'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
    WHERE
      periodseq = EXT.FN_GETCURRENTPERIOD()
      /* ORIGSQL: fn_getcurrentperiod() */
      AND removedate > CURRENT_TIMESTAMP
      /* ORIGSQL: sysdate */
  )
FROM
  ext.inbound_cfg_genericparameter
WHERE
  KEY = 'PROCESSMONTH';


v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '4. Update value in ext.inbound_CFG_GENERICPARAMETER:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), '3.(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | '4. Update value in ext.inbound_CFG_GENERICPARAMETER:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  '3. Update value in ext.inbound_CFG_GENERICPARAMETER Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || '4. Update value in ext.inbound_CFG_GENERICPARAMETER:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/* ORIGSQL: commit; */
COMMIT;


-- working days for new hires and terminated people. for Advertising and other channles with no proration
/* ORIGSQL: execute immediate 'truncate table EXT.stel_DATA_finalfv_DAYS'; */
/* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_FINALFV_DAYS' not found */
/* ORIGSQL: truncate table EXT.stel_DATA_finalfv_DAYS ; */
EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.stel_DATA_finalfv_DAYS';


/* ORIGSQL: execute immediate 'truncate table EXT.stel_DATA_finalfv_DAYS_ann'; */
/* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_FINALFV_DAYS_ANN' not found */
/* ORIGSQL: truncate table EXT.stel_DATA_finalfv_DAYS_ann ; */
EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.stel_DATA_finalfv_DAYS_ann';


/* ORIGSQL: execute immediate 'truncate table EXT.stel_DATA_finalfv_RATIO'; */
/* RESOLVE: Identifier not found: Table/view 'EXT.STEL_DATA_FINALFV_RATIO' not found */
/* ORIGSQL: truncate table EXT.stel_DATA_finalfv_RATIO ; */
EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.stel_DATA_finalfv_RATIO';


/* ORIGSQL: execute immediate 'truncate table EXT.stel_TeMP_PROCESSMONTHS'; */
/* RESOLVE: Identifier not found: Table/view 'EXT.STEL_TEMP_PROCESSMONTHS' not found */
/* ORIGSQL: truncate table EXT.stel_TeMP_PROCESSMONTHS ; */
EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.stel_TeMP_PROCESSMONTHS';


/* ORIGSQL: execute immediate 'truncate table EXT.stel_TeMP_POSITION'; */
/* RESOLVE: Identifier not found: Table/view 'EXT.STEL_TEMP_POSITION' not found */
/* ORIGSQL: truncate table EXT.stel_TeMP_POSITION ; */
EXECUTE IMMEDIATE 'TRUNCATE TABLE EXT.stel_TeMP_POSITION';


/* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_CFG_GENERICPARAMETER' not found */
/* ORIGSQL: insert into EXT.stel_TeMP_PROCESSMONTHS (caldate, periodname, periodseq, startdate, enddate, periodtypename) select distinct a.caldate, b.periodname, b.periodseq, b.startdate, b.enddate, b.periodtypename (...) */
INSERT INTO
  EXT.stel_TeMP_PROCESSMONTHS (
    caldate,
    periodname,
    periodseq,
    startdate,
    enddate,
    periodtypename
  )
SELECT
  /* ORIGSQL: select distinct a.caldate, b.periodname, b.periodseq, b.startdate, b.enddate, b.periodtypename from (SELECT distinct yy.monthstartdate AS caldate FROM ext.inbound_cfg_genericparameter xx INNER join EXT.ST(...) */
  DISTINCT a.caldate,
  b.periodname,
  b.periodseq,
  b.startdate,
  b.enddate,
  b.periodtypename
FROM
  -- , b.monthperiodseq, b.monthstartdate, b.monthenddate
  (
    SELECT
      /* ORIGSQL: (SELECT distinct yy.monthstartdate caldate FROM ext.inbound_cfg_genericparameter xx join EXT.stel_PERIODHIERARCHY@STELEXT yy on yy.calendarname='Singtel Monthly Calendar' and yy.periodtypename='year' join STE(...) */
      DISTINCT yy.monthstartdate AS caldate
    FROM
      ext.inbound_cfg_genericparameter xx
      INNER JOIN EXT.STEL_PERIODHIERARCHY yy ON yy.calendarname = 'Singtel Monthly Calendar'
      AND yy.periodtypename = 'year'
      INNER JOIN EXT.STEL_PERIODHIERARCHY zz ON zz.calendarname = 'Singtel Monthly Calendar'
      AND zz.periodtypename = 'month'
      AND TO_VARCHAR(zz.startdate, 'YYYYMMDD') = xx.value
      /* ORIGSQL: to_char(zz.startdate, 'YYYYMMDD') */
      /* RESOLVE: Oracle Database link: Remote table/view 'EXT.STEL_PERIODHIERARCHY@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'EXT.STEL_PERIODHIERARCHY'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
      /* RESOLVE: Oracle Database link: Remote table/view 'EXT.STEL_PERIODHIERARCHY@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'EXT.STEL_PERIODHIERARCHY'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
    WHERE
      xx.value = TO_VARCHAR(zz.startdate, 'YYYYMMDD')
      /* ORIGSQL: to_char(zz.startdate, 'YYYYMMDD') */
      --yy.periodname=substr(xx.value,1,4)
      -- AND (yy.startdate, TO_DATE(ADD_DAYS(yy.enddate,(86400*-1)))) overlaps (zz.startdate, TO_DATE(ADD_SECONDS(zz.enddate,(86400*-1))))  /* ORIGSQL: zz.enddate-1 */
      AND (
        (
          yy.startdate between zz.startdate
          and ADD_DAYS(zz.enddate, -1)
        )
        or (
          yy.enddate between zz.startdate
          and ADD_DAYS(zz.enddate, -1)
        )
      )
      /* ORIGSQL: yy.enddate-1 */
      AND xx.KEY = 'PROCESSMONTH'
  ) AS a
  INNER JOIN EXT.STEL_PERIODHIERARCHY b ON b.calendarname = 'Singtel Monthly Calendar'
  AND a.caldate BETWEEN b.monthstartdate
  AND add_days(b.monthenddate, -1)
  /* RESOLVE: Oracle Database link: Remote table/view 'EXT.STEL_PERIODHIERARCHY@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'EXT.STEL_PERIODHIERARCHY'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
WHERE
  b.periodtypename = 'month';


--to avoid duplication for qtr and year, the rollup is done later
v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert into EXT.stel_TEMP_PROCESSMONTHS:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), 'Insert into ST(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | 'Insert into EXT.stel_TEMP_PROCESSMONTHS:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  'Insert into EXT.stel_TEMP_PROCESSMONTHS Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || 'Insert into EXT.stel_TEMP_PROCESSMONTHS:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/* ORIGSQL: insert into EXT.stel_tEmp_position (payeeseq, name, positionseq, genericattribute3, hiredate, terminationdate,caldate, periodtypename, startdate, enddate, periodseq, periodname) select distinct pos.payees(...) */
INSERT INTO
  EXT.stel_tEmp_position (
    payeeseq,
    name,
    positionseq,
    genericattribute3,
    hiredate,
    terminationdate,
    caldate,
    periodtypename,
    startdate,
    enddate,
    periodseq,
    periodname
  )
SELECT
  /* ORIGSQL: select distinct pos.payeeseq, pos.name, pos.ruleelementownerseq, pos.genericattribute3, MAX(CASE WHEN pos.genericattribute3 in (SELECT saleschannel FROM EXT.stel_cfg_leavechannel where hireproration='Y') (...) */
  DISTINCT pos.payeeseq,
  pos.name,
  pos.ruleelementownerseq,
  pos.genericattribute3,
  MAX(
    CASE
      WHEN pos.genericattribute3 IN (
        SELECT
          /* ORIGSQL: (Select saleschannel from EXT.stel_cfg_leavechannel where hireproration='Y') */
          saleschannel
        FROM
          EXT.stel_cfg_leavechannel
        WHERE
          hireproration = 'Y'
      ) THEN pos2.effdate
      ELSE IFNULL(par.hiredate, pos2.effdate)
      /* ORIGSQL: nvl(par.hiredate,pos2.effdate) */
    END
  ) AS hiredate
  /* -- using min eff date if hire date is missing */
,
  MAX(par.terminationdate),
  m.caldate,
  m.periodtypename,
  m.startdate,
  m.enddate,
  m.periodseq,
  m.periodname
FROM
  EXT.stel_temp_processmonths m
  INNER JOIN cs_participant par ON par.removedate > CURRENT_TIMESTAMP
  /* ORIGSQL: sysdate */
  --and m.caldate between par.effectivestartdate and par.effectiveenddate-1
  INNER JOIN cs_position pos ON pos.removedate > CURRENT_TIMESTAMP
  /* ORIGSQL: sysdate */
  AND m.caldate BETWEEN pos.effectivestartdate
  AND add_days(pos.effectiveenddate, -1)
  /*uncommented by Arjun 20190819 to fix the issue with payees in BSC and Singtel Shop*/
  AND pos.payeeseq = par.payeeseq
  INNER JOIN (
    SELECT
      /* ORIGSQL: (select MIN(px.effectivestartdate) effdate, px.ruleelementownerseq from cs_position@stelext px where px.removedate>sysdate group by px.ruleelementownerseq) */
      MIN(px.effectivestartdate) AS effdate,
      px.ruleelementownerseq
    FROM
      cs_position px
      /* RESOLVE: Oracle Database link: Remote table/view 'cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_position'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
    WHERE
      px.removedate > CURRENT_TIMESTAMP --and px.effectiveenddate>sysdate
      /* ORIGSQL: sysdate */
    GROUP BY
      px.ruleelementownerseq
  ) AS pos2 ON pos2.ruleelementownerseq = pos.ruleelementownerseq
  /* RESOLVE: Oracle Database link: Remote table/view 'cs_position@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_position'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
  /* RESOLVE: Oracle Database link: Remote table/view 'cs_participant@stelext' at Oracle DBlink 'stelext' is converted to HANA virtual table 'cs_participant'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
WHERE
  pos.genericattribute3 IS NOT NULL
GROUP BY
  pos.payeeseq,
  pos.name,
  pos.ruleelementownerseq,
  pos.genericattribute3,
  m.caldate,
  m.periodtypename,
  m.startdate,
  m.enddate,
  m.periodseq,
  m.periodname;


v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert into EXT.stel_TEMP_POSITIONS:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), 'Insert into EXT.stel_T(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | 'Insert into EXT.stel_TEMP_POSITIONS:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  'Insert into EXT.stel_TEMP_POSITIONS Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || 'Insert into EXT.stel_TEMP_POSITIONS:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/* ORIGSQL: commit; */
COMMIT;


/*Workied days, excluding hore and termination dates.*/
/* ORIGSQL: INSERT INTO EXT.stel_data_finalfv_days (Geid, channel, val, periodname, periodseq, periodtype, valtype, perioddate) select name, genericattribute3,SUM(WorkingdaysInStartMonth),periodname,periodseq,periodt(...) */
INSERT INTO
  EXT.stel_data_finalfv_days (
    Geid,
    channel,
    val,
    periodname,
    periodseq,
    periodtype,
    valtype,
    perioddate
  )
  /*In hire month*/
SELECT
  /* ORIGSQL: select name, genericattribute3,SUM(WorkingdaysInStartMonth),periodname,periodseq,periodtypename,valtype, startdate from (SELECT pos.name, pos.genericattribute3, (LAST_DAY(pos.hiredate) - (pos.hiredate(...) */
  name,
  genericattribute3,
  SUM(WorkingdaysInStartMonth),
  periodname,
  periodseq,
  periodtypename,
  valtype,
  startdate
FROM
  (
    SELECT
      /* ORIGSQL: (SELECT pos.name, pos.genericattribute3, (LAST_DAY(pos.hiredate) - (pos.hiredate)+1 - NVL(COUNT (DISTINCT x.nonworkdate),0)) -nvl(max(lcount.leavedays),0) AS WorkingdaysInStartMonth, pos.periodname, p(...) */
      pos.name,
      pos.genericattribute3,
      --  SELECT
      --     DAYOFMONTH(ADD_DAYS(pos.effectivestartdate, 1)) AS day_after_effectivestartdate,
      --     DAYOFMONTH(LAST_DAY(pos.effectivestartdate)) AS last_day_of_month,
      --     DAYOFMONTH(LAST_DAY(pos.hiredate)) - DAYOFMONTH(ADD_DAYS(pos.hiredate, 1))
      --     - 
      -- FROM
      --     cs_position pos;
      /*Deepan : replacing WorkingdaysInStartMonth logic with a simpler code */
      DAYOFMONTH(LAST_DAY(pos.hiredate)) - DAYOFMONTH(ADD_DAYS(pos.hiredate, 1)) - IFNULL(COUNT(DISTINCT x.nonworkdate), 0) - IFNULL(MAX(lcount.leavedays), 0) AS WorkingdaysInStartMonth,
      -- TO_DATE(ADD_SECONDS((TO_DATE(ADD_SECONDS(TO_DATE(ADD_SECONDS(ADD_SECONDS(LAST_DAY(pos.hiredate),(86400*(-1*(pos.hiredate)))),(86400*1)))   /* ORIGSQL: LAST_DAY(pos.hiredate) - (pos.hiredate) */
      --                                                                                                                                           /* ORIGSQL: ADD_SECONDS(LAST_DAY(pos.hiredate),(86400*(-1*(pos.hiredate)))) +1 */
      --                     ,(86400*(-1*IFNULL(COUNT(DISTINCT x.nonworkdate),0)))))
      --             ),(86400*(-1*IFNULL(MAX(lcount.leavedays),0)))))  /* ORIGSQL: TO_DATE(ADD_SECONDS(ADD_SECONDS(LAST_DAY(pos.hiredate),(86400*(-1*(pos.hiredate)))),(86400*1))) - IFNULL(COUNT(DISTINCT x.nonworkdate),0) */
      /* ORIGSQL: NVL(COUNT (DISTINCT x.nonworkdate),0) */
      -- AS WorkingdaysInStartMonth,  /* ORIGSQL: nvl(max(lcount.leavedays),0) */
      /* ORIGSQL: (TO_DATE(ADD_SECONDS(TO_DATE(ADD_SECONDS(ADD_SECONDS(LAST_DAY(pos.hiredate),(86400*(-1*(pos.hiredate)))),(86400*1))),(86400*(-1*IFNULL(COUNT(DISTINCT x.nonworkdate),0)))))) -IFNULL(MAX(lcount.leaveday(...) */
      /* -- Total work days  - (Non working days (Sat Sun PH) + the leave days that are not non working days) */
      pos.periodname,
      pos.periodseq,
      pos.periodtypename,
      'DAYS' AS valtype,
      POS.startdate
    FROM
      EXT.stel_temp_position pos
      INNER JOIN EXT.stel_cfg_leavechannel lc ON lc.saleschannel = pos.genericattribute3
      AND lc.frequency = pos.periodtypename
      LEFT OUTER JOIN EXT.stel_Work_leavecount lcount ON lcount.channel = lc.saleschannel
      AND lcount.geid = pos.name
      AND lcount.leavecounttype = 'DEDUCTABLE'
      AND lcount.periodseq = pos.periodseq
      LEFT OUTER JOIN EXT.stel_temp_NONWORKINGDAYS x ON x.channel = pos.genericattribute3
      AND x.nonworkdate BETWEEN pos.hiredate
      AND LAST_DAY(pos.hiredate)
    WHERE
      pos.hiredate BETWEEN pos.startdate
      AND ADD_DAYS(pos.enddate, - 1)
      /*Deepan : replaced TO_DATE(ADD_SECONDS(pos.enddate,(86400*-1)) with ADD_DAYS(pos.enddate, - 1)*/
      -- TO_DATE(ADD_SECONDS(pos.enddate,(86400*-1)))   /* ORIGSQL: pos.enddate-1 */
      AND lc.saleschannel IN (
        SELECT
          /* ORIGSQL: (Select saleschannel from EXT.stel_cfg_leavechannel where hireproration='Y') */
          saleschannel
        FROM
          EXT.stel_cfg_leavechannel
        WHERE
          hireproration = 'Y'
      )
    GROUP BY
      pos.name,
      pos.genericattribute3,
      hiredate,
      pos.periodname,
      pos.periodseq,
      pos.periodtypename,
      pos.startdate --where par.hiredate   < add_months(last_day(Caldate),    -1)+1
    UNION ALL
    /*in termination month*/
    SELECT
      /* ORIGSQL: SELECT pos.name, pos.genericattribute3, (terminationdate - (ADD_MONTHS(LAST_DAY(terminationdate),-1)) - COUNT(DISTINCT x.nonworkdate) -nvl(max(lcount.leavedays),0)) AS WorkingdaysInEndMonth, pos.perio(...) */
      pos.name,
      pos.genericattribute3,
      /*Deepan : replacing WorkingdaysInEndMonth logic with a simpler code */
      DAYOFMONTH(LAST_DAY(pos.terminationdate)) - DAYOFMONTH(ADD_MONTHS(pos.terminationdate, -1)) - IFNULL(COUNT(DISTINCT x.nonworkdate), 0) - IFNULL(MAX(lcount.leavedays), 0) AS WorkingdaysInEndMonth,
      -- (terminationdate - TO_DATE(ADD_SECONDS(TO_DATE(ADD_SECONDS((ADD_MONTHS(LAST_DAY(terminationdate),-1))
      --                     ,(86400*(-1*COUNT(DISTINCT x.nonworkdate))))),(86400*(-1*IFNULL(MAX(lcount.leavedays),0)))))  /* ORIGSQL: (ADD_MONTHS(LAST_DAY(terminationdate),-1)) - COUNT(DISTINCT x.nonworkdate) */
      --     ) AS WorkingdaysInEndMonth,  /* ORIGSQL: nvl(max(lcount.leavedays),0) */
      --                                  /* ORIGSQL: TO_DATE(ADD_SECONDS((ADD_MONTHS(LAST_DAY(terminationdate),-1)),(86400*(-1*COUNT(DISTINCT x.nonworkdate))))) -IFNULL(MAX(lcount.leavedays),0) */
      pos.periodname,
      pos.periodseq,
      pos.periodtypename,
      'DAYS',
      pos.startdate
    FROM
      EXT.stel_temp_position pos -- AND par.terminationdate between  pos.effectivestartdate and pos.effectiveenddate-1 --BETWEEN par.effectivestartdate AND par.effectiveenddate-1
      -- AND pos.removedate>sysdate
      INNER JOIN EXT.stel_cfg_leavechannel lc ON lc.saleschannel = pos.genericattribute3
      AND lc.frequency = pos.periodtypename
      LEFT OUTER JOIN EXT.stel_Work_leavecount lcount ON lcount.channel = lc.saleschannel
      AND lcount.geid = pos.name
      AND lcount.leavecounttype = 'DEDUCTABLE'
      AND lcount.periodseq = pos.periodseq
      LEFT OUTER JOIN EXT.stel_temp_NONWORKINGDAYS x ON x.channel = pos.genericattribute3 -- AND x.nonworkdate BETWEEN TO_DATE(ADD_SECONDS(ADD_MONTHS(LAST_DAY(terminationdate),-1),(86400*1))) AND pos.terminationdate  /* ORIGSQL: ADD_MONTHS(LAST_DAY(terminationdate),-1) +1 */
      AND x.nonworkdate BETWEEN ADD_DAYS(ADD_MONTHS(LAST_DAY(terminationdate), -1), 1)
      AND pos.terminationdate
      /*Deepan : Simplified version*/
    WHERE
      pos.terminationdate IS NOT NULL -- AND pos.terminationdate BETWEEN pos.startdate AND TO_DATE(ADD_SECONDS(pos.enddate,(86400*-1)))   /* ORIGSQL: pos.enddate-1 */
      AND pos.terminationdate BETWEEN pos.startdate
      AND ADD_DAYS(pos.enddate, -1)
      /*Deepan : Simplified version*/
      AND lc.saleschannel IN (
        SELECT
          /* ORIGSQL: (Select saleschannel from EXT.stel_cfg_leavechannel where hireproration='Y') */
          saleschannel
        FROM
          EXT.stel_cfg_leavechannel
        WHERE
          hireproration = 'Y'
      )
    GROUP BY
      pos.name,
      pos.genericattribute3,
      terminationdate,
      pos.periodname,
      pos.periodseq,
      pos.periodtypename,
      pos.startdate
    UNION ALL
    /*between hore and termination*/
    SELECT
      /* ORIGSQL: SELECT pos.name, pos.genericattribute3, (LAST_DAY(caldate) - (ADD_MONTHS(LAST_DAY(caldate),-1)) - COUNT(DISTINCT x.nonworkdate) -nvl(max(lcount.leavedays),0)) AS WorkingdaysInMonth, pos.periodname, po(...) */
      pos.name,
      pos.genericattribute3,
      (
        (
          SECONDS_BETWEEN(
            (ADD_MONTHS(LAST_DAY(caldate), -1)),
            LAST_DAY(caldate)
          ) / 86400
        )
        /* ORIGSQL: LAST_DAY(caldate) - (ADD_MONTHS(LAST_DAY(caldate),-1)) */
        - COUNT(DISTINCT x.nonworkdate) - IFNULL(MAX(lcount.leavedays), 0)
      ) AS WorkingdaysInMonth,
      /* ORIGSQL: nvl(max(lcount.leavedays),0) */
      pos.periodname,
      pos.periodseq,
      pos.periodtypename,
      'DAYS',
      pos.startdate
    FROM
      EXT.stel_TEMP_Position pos
      INNER JOIN EXT.stel_cfg_leavechannel lc ON lc.saleschannel = pos.genericattribute3
      AND lc.frequency = pos.periodtypename
      LEFT OUTER JOIN EXT.stel_Work_leavecount lcount ON lcount.channel = lc.saleschannel
      AND lcount.geid = pos.name
      AND lcount.leavecounttype = 'DEDUCTABLE'
      AND lcount.periodseq = pos.periodseq
      LEFT OUTER JOIN EXT.stel_temp_NONWORKINGDAYS x ON x.channel = pos.genericattribute3 -- AND x.nonworkdate BETWEEN TO_DATE(ADD_SECONDS(ADD_MONTHS(LAST_DAY(caldate),-1),(86400*1))) AND LAST_DAY(caldate)  /* ORIGSQL: ADD_MONTHS(LAST_DAY(caldate),-1) +1 */
      AND x.nonworkdate BETWEEN ADD_DAYS(ADD_MONTHS(LAST_DAY(caldate), -1), 1)
      AND LAST_DAY(caldate)
      /*Deepan : Simplified version */
    WHERE
      (
        (
          -- (IFNULL(pos.hiredate,TO_DATE('20150401','YYYYMMDD')) < TO_DATE(ADD_SECONDS(ADD_MONTHS(LAST_DAY(Caldate), -1),(86400*1)))   /* ORIGSQL: nvl(pos.hiredate,to_Date('20150401','YYYYMMDD')) */
          --                                                                                                                            /* ORIGSQL: ADD_MONTHS(LAST_DAY(Caldate), -1) +1 */
          --     AND IFNULL(pos.terminationdate,TO_DATE('22000101','YYYYMMDD')) > LAST_DAY(caldate))  /* ORIGSQL: nvl(pos.terminationdate,to_Date('22000101','YYYYMMDD')) */
          (
            IFNULL(pos.hiredate, TO_DATE('20150401', 'YYYYMMDD')) < ADD_DAYS(ADD_MONTHS(LAST_DAY(Caldate), -1), 1)
            AND IFNULL(
              pos.terminationdate,
              TO_DATE('22000101', 'YYYYMMDD')
            ) > LAST_DAY(Caldate)
          )
          /*Deepan : Simplified form*/
          AND (
            lc.saleschannel IN (
              SELECT
                /* ORIGSQL: (Select saleschannel from EXT.stel_cfg_leavechannel where hireproration='Y') */
                saleschannel
              FROM
                EXT.stel_cfg_leavechannel
              WHERE
                hireproration = 'Y'
            )
          )
        )
        OR (
          lc.saleschannel NOT IN (
            SELECT
              /* ORIGSQL: (Select saleschannel from EXT.stel_cfg_leavechannel where hireproration='Y') */
              saleschannel
            FROM
              EXT.stel_cfg_leavechannel
            WHERE
              hireproration = 'Y'
          )
        )
      )
    GROUP BY
      pos.name,
      pos.genericattribute3,
      caldate,
      pos.periodname,
      pos.periodseq,
      pos.periodtypename,
      pos.startdate
  ) AS a
GROUP BY
  name,
  genericattribute3,
  periodname,
  periodseq,
  periodtypename,
  valtype,
  startdate;


v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert into EXT.stel_DATA_FINALFV_DAYS:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), 'Insert into STE(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | 'Insert into EXT.stel_DATA_FINALFV_DAYS:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  'Insert into EXT.stel_DATA_FINALFV_DAYS Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || 'Insert into EXT.stel_DATA_FINALFV_DAYS:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/* ORIGSQL: commit; */
COMMIT;


/* ORIGSQL: INSERT INTO EXT.stel_data_finalfv_days_ann (Geid, channel, val, periodname, periodseq, periodtype, valtype, perioddate) select name, genericattribute3,SUM(WorkingdaysInStartMonth),periodname,periodseq,per(...) */
INSERT INTO
  EXT.stel_data_finalfv_days_ann (
    Geid,
    channel,
    val,
    periodname,
    periodseq,
    periodtype,
    valtype,
    perioddate
  )
  /*In hire month*/
SELECT
  /* ORIGSQL: select name, genericattribute3,SUM(WorkingdaysInStartMonth),periodname,periodseq,periodtypename,valtype, startdate from (SELECT pos.name, pos.genericattribute3, (LAST_DAY(pos.hiredate) - (pos.hiredate(...) */
  name,
  genericattribute3,
  SUM(WorkingdaysInStartMonth),
  periodname,
  periodseq,
  periodtypename,
  valtype,
  startdate
FROM
  (
    SELECT
      /* ORIGSQL: (SELECT pos.name, pos.genericattribute3, (LAST_DAY(pos.hiredate) - (pos.hiredate)+1 - NVL(COUNT (DISTINCT x.nonworkdate),0)) -nvl(max(lcount.leavedays),0) AS WorkingdaysInStartMonth, pos.periodname, p(...) */
      pos.name,
      pos.genericattribute3,
      /*Deepan : replacing WorkingdaysInStartMonth logic with a simpler code */
      DAYOFMONTH(LAST_DAY(pos.hiredate)) - DAYOFMONTH(ADD_DAYS(pos.hiredate, 1)) - IFNULL(COUNT(DISTINCT x.nonworkdate), 0) - IFNULL(MAX(lcount.leavedays), 0) AS WorkingdaysInStartMonth,
      -- TO_DATE(ADD_SECONDS((TO_DATE(ADD_SECONDS(TO_DATE(ADD_SECONDS(ADD_SECONDS(LAST_DAY(pos.hiredate),(86400*(-1*(pos.hiredate)))),(86400*1)))   /* ORIGSQL: LAST_DAY(pos.hiredate) - (pos.hiredate) */
      --                                                                                                                                           /* ORIGSQL: ADD_SECONDS(LAST_DAY(pos.hiredate),(86400*(-1*(pos.hiredate)))) +1 */
      --                     ,(86400*(-1*IFNULL(COUNT(DISTINCT x.nonworkdate),0)))))
      --             ),(86400*(-1*IFNULL(MAX(lcount.leavedays),0)))))  /* ORIGSQL: TO_DATE(ADD_SECONDS(ADD_SECONDS(LAST_DAY(pos.hiredate),(86400*(-1*(pos.hiredate)))),(86400*1))) - IFNULL(COUNT(DISTINCT x.nonworkdate),0) */
      --                                                               /* ORIGSQL: NVL(COUNT (DISTINCT x.nonworkdate),0) */
      -- AS WorkingdaysInStartMonth,  /* ORIGSQL: nvl(max(lcount.leavedays),0) */
      /* ORIGSQL: (TO_DATE(ADD_SECONDS(TO_DATE(ADD_SECONDS(ADD_SECONDS(LAST_DAY(pos.hiredate),(86400*(-1*(pos.hiredate)))),(86400*1))),(86400*(-1*IFNULL(COUNT(DISTINCT x.nonworkdate),0)))))) -IFNULL(MAX(lcount.leaveday(...) */
      /* -- Total work days  - (Non working days (Sat Sun PH) + the leave days that are not non working days) */
      pos.periodname,
      pos.periodseq,
      pos.periodtypename,
      'DAYS' AS valtype,
      POS.startdate
    FROM
      EXT.stel_temp_position pos
      INNER JOIN EXT.stel_cfg_leavechannel lc ON lc.saleschannel = pos.genericattribute3
      AND lc.frequency = pos.periodtypename
      LEFT OUTER JOIN EXT.stel_Work_leavecount lcount ON lcount.channel = lc.saleschannel
      AND lcount.geid = pos.name
      AND lcount.leavecounttype = 'DEDUCTABLE'
      AND lcount.periodseq = pos.periodseq
      LEFT OUTER JOIN EXT.stel_temp_NONWORKINGDAYS x ON x.channel = pos.genericattribute3
      AND x.nonworkdate BETWEEN pos.hiredate
      AND LAST_DAY(pos.hiredate)
    WHERE
      -- pos.hiredate BETWEEN pos.startdate AND TO_DATE(ADD_SECONDS(pos.enddate,(86400*-1))) --disregard hireproration check for SAA
      pos.hiredate BETWEEN pos.startdate
      AND ADD_DAYS(pos.enddate, -1)
      /*Deepan : Simplified version*/
      /* ORIGSQL: pos.enddate-1 */
    GROUP BY
      pos.name,
      pos.genericattribute3,
      hiredate,
      pos.periodname,
      pos.periodseq,
      pos.periodtypename,
      pos.startdate --where par.hiredate   < add_months(last_day(Caldate),    -1)+1
    UNION ALL
    /*in termination month*/
    SELECT
      /* ORIGSQL: SELECT pos.name, pos.genericattribute3, (terminationdate - (ADD_MONTHS(LAST_DAY(terminationdate),-1)) - COUNT(DISTINCT x.nonworkdate) -nvl(max(lcount.leavedays),0)) AS WorkingdaysInEndMonth, pos.perio(...) */
      pos.name,
      pos.genericattribute3,
      /*Deepan : replacing WorkingdaysInEndMonth logic with a simpler code */
      DAYOFMONTH(LAST_DAY(pos.terminationdate)) - DAYOFMONTH(ADD_MONTHS(pos.terminationdate, -1)) - IFNULL(COUNT(DISTINCT x.nonworkdate), 0) - IFNULL(MAX(lcount.leavedays), 0) AS WorkingdaysInEndMonth,
      -- (terminationdate - TO_DATE(ADD_SECONDS(TO_DATE(ADD_SECONDS((ADD_MONTHS(LAST_DAY(terminationdate),-1))
      --                     ,(86400*(-1*COUNT(DISTINCT x.nonworkdate))))),(86400*(-1*IFNULL(MAX(lcount.leavedays),0)))))  /* ORIGSQL: (ADD_MONTHS(LAST_DAY(terminationdate),-1)) - COUNT(DISTINCT x.nonworkdate) */
      --     ) AS WorkingdaysInEndMonth,  /* ORIGSQL: nvl(max(lcount.leavedays),0) */
      --                                  /* ORIGSQL: TO_DATE(ADD_SECONDS((ADD_MONTHS(LAST_DAY(terminationdate),-1)),(86400*(-1*COUNT(DISTINCT x.nonworkdate))))) -IFNULL(MAX(lcount.leavedays),0) */
      pos.periodname,
      pos.periodseq,
      pos.periodtypename,
      'DAYS',
      pos.startdate
    FROM
      EXT.stel_temp_position pos -- AND par.terminationdate between  pos.effectivestartdate and pos.effectiveenddate-1 --BETWEEN par.effectivestartdate AND par.effectiveenddate-1
      -- AND pos.removedate>sysdate
      INNER JOIN EXT.stel_cfg_leavechannel lc ON lc.saleschannel = pos.genericattribute3
      AND lc.frequency = pos.periodtypename
      LEFT OUTER JOIN EXT.stel_Work_leavecount lcount ON lcount.channel = lc.saleschannel
      AND lcount.geid = pos.name
      AND lcount.leavecounttype = 'DEDUCTABLE'
      AND lcount.periodseq = pos.periodseq
      LEFT OUTER JOIN EXT.stel_temp_NONWORKINGDAYS x ON x.channel = pos.genericattribute3 -- AND x.nonworkdate BETWEEN TO_DATE(ADD_SECONDS(ADD_MONTHS(LAST_DAY(terminationdate),-1),(86400*1))) AND pos.terminationdate  /* ORIGSQL: ADD_MONTHS(LAST_DAY(terminationdate),-1) +1 */
      AND x.nonworkdate BETWEEN ADD_DAYS(ADD_MONTHS(LAST_DAY(terminationdate), -1), 1)
      AND pos.terminationdate
      /*Deepan : Simplified version*/
    WHERE
      pos.terminationdate IS NOT NULL -- AND pos.terminationdate BETWEEN pos.startdate AND TO_DATE(ADD_SECONDS(pos.enddate,(86400*-1)))   /* ORIGSQL: pos.enddate-1 */
      AND pos.terminationdate BETWEEN pos.startdate
      AND ADD_DAYS(pos.enddate, -1)
      /*Deepan : simplified version*/
    GROUP BY
      pos.name,
      pos.genericattribute3,
      terminationdate,
      pos.periodname,
      pos.periodseq,
      pos.periodtypename,
      pos.startdate
    UNION ALL
    /*between hore and termination*/
    SELECT
      /* ORIGSQL: SELECT pos.name, pos.genericattribute3, (LAST_DAY(caldate) - (ADD_MONTHS(LAST_DAY(caldate),-1)) - COUNT(DISTINCT x.nonworkdate) -nvl(max(lcount.leavedays),0)) AS WorkingdaysInMonth, pos.periodname, po(...) */
      pos.name,
      pos.genericattribute3,
      -- ((SECONDS_BETWEEN((ADD_MONTHS(LAST_DAY(caldate),-1)),LAST_DAY(caldate))/86400)   /* ORIGSQL: LAST_DAY(caldate) - (ADD_MONTHS(LAST_DAY(caldate),-1)) */
      --     - COUNT(DISTINCT x.nonworkdate) -IFNULL(MAX(lcount.leavedays),0)) AS WorkingdaysInMonth,  /* ORIGSQL: nvl(max(lcount.leavedays),0) */
      (
        (
          DAYS_BETWEEN(
            ADD_MONTHS(LAST_DAY(caldate), -1),
            LAST_DAY(caldate)
          )
        ) - COUNT(DISTINCT x.nonworkdate) - IFNULL(MAX(lcount.leavedays), 0)
      ) AS WorkingdaysInMonth,
      /*Deepan :Simplified form*/
      pos.periodname,
      pos.periodseq,
      pos.periodtypename,
      'DAYS',
      pos.startdate
    FROM
      EXT.stel_TEMP_Position pos
      INNER JOIN EXT.stel_cfg_leavechannel lc ON lc.saleschannel = pos.genericattribute3
      AND lc.frequency = pos.periodtypename
      LEFT OUTER JOIN EXT.stel_Work_leavecount lcount ON lcount.channel = lc.saleschannel
      AND lcount.geid = pos.name
      AND lcount.leavecounttype = 'DEDUCTABLE'
      AND lcount.periodseq = pos.periodseq
      LEFT OUTER JOIN EXT.stel_temp_NONWORKINGDAYS x ON x.channel = pos.genericattribute3 -- AND x.nonworkdate BETWEEN TO_DATE(ADD_SECONDS(ADD_MONTHS(LAST_DAY(caldate),-1),(86400*1))) AND LAST_DAY(caldate)  /* ORIGSQL: ADD_MONTHS(LAST_DAY(caldate),-1) +1 */
      AND x.nonworkdate BETWEEN ADD_DAYS(ADD_MONTHS(LAST_DAY(caldate), -1), 1)
      AND LAST_DAY(caldate)
      /*Deepan : Simplified form*/
    WHERE
      (
        (
          -- (IFNULL(pos.hiredate,TO_DATE('20150401','YYYYMMDD')) < TO_DATE(ADD_SECONDS(ADD_MONTHS(LAST_DAY(Caldate), -1),(86400*1)))   /* ORIGSQL: nvl(pos.hiredate,to_Date('20150401','YYYYMMDD')) */
          --                                                                                                                            /* ORIGSQL: ADD_MONTHS(LAST_DAY(Caldate), -1) +1 */
          --     AND IFNULL(pos.terminationdate,TO_DATE('22000101','YYYYMMDD')) > LAST_DAY(caldate))  /* ORIGSQL: nvl(pos.terminationdate,to_Date('22000101','YYYYMMDD')) */
          (
            IFNULL(pos.hiredate, TO_DATE('20150401', 'YYYYMMDD')) < ADD_DAYS(ADD_MONTHS(LAST_DAY(caldate), -1), 1)
            AND IFNULL(
              pos.terminationdate,
              TO_DATE('22000101', 'YYYYMMDD')
            ) > LAST_DAY(caldate)
          )
          /*Deepan : Simplified form*/
        )
      )
    GROUP BY
      pos.name,
      pos.genericattribute3,
      caldate,
      pos.periodname,
      pos.periodseq,
      pos.periodtypename,
      pos.startdate
  ) AS a
GROUP BY
  name,
  genericattribute3,
  periodname,
  periodseq,
  periodtypename,
  valtype,
  startdate;


v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert into EXT.stel_DATA_FINALFV_DAYS_ann:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), 'Insert into(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | 'Insert into EXT.stel_DATA_FINALFV_DAYS_ann:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  'Insert into EXT.stel_DATA_FINALFV_DAYS Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || 'Insert into EXT.stel_DATA_FINALFV_DAYS_ann:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/* ORIGSQL: commit; */
COMMIT;


/******************/
-- Qtr and Yr Rollup   
/* ORIGSQL: insert into EXT.stel_DATA_FINALFV_DAYS (geid, channel, val, periodname, periodseq, periodtype, valtype, perioddate) select a.geid, a.channel, SUM(val) val, prd.periodname, prd.periodseq, prd.periodtypenam(...) */
INSERT INTO
  EXT.stel_DATA_FINALFV_DAYS (
    geid,
    channel,
    val,
    periodname,
    periodseq,
    periodtype,
    valtype,
    perioddate
  )
SELECT
  /* ORIGSQL: select a.geid, a.channel, SUM(val) val, prd.periodname, prd.periodseq, prd.periodtypename, a.valtype, prd.startdate perioddate from EXT.stel_DATA_FINALFV_DAYS a join EXT.stel_temp_periodhierarchy prd on a.per(...) */
  a.geid,
  a.channel,
  SUM(val) AS val,
  prd.periodname,
  prd.periodseq,
  prd.periodtypename,
  a.valtype,
  prd.startdate AS perioddate
FROM
  EXT.stel_DATA_FINALFV_DAYS a
  INNER JOIN EXT.stel_temp_periodhierarchy prd ON a.perioddate BETWEEN prd.monthstartdate
  AND add_days(prd.monthenddate, -1)
  AND prd.calendarname LIKE 'Singtel%Mon%'
  AND prd.periodtypename = 'quarter'
GROUP BY
  a.geid,
  a.channel,
  prd.periodname,
  prd.periodseq,
  prd.periodtypename,
  a.valtype,
  prd.startdate
UNION
SELECT
  /* ORIGSQL: select a.geid, xy.genericattribute3 channel, SUM(val) val, prd.periodname, prd.periodseq, prd.periodtypename, a.valtype, prd.startdate perioddate from EXT.stel_DATA_FINALFV_DAYS_ann a join EXT.stel_temp_perio(...) */
  a.geid,
  xy.genericattribute3 AS channel,
  SUM(val) AS val,
  prd.periodname,
  prd.periodseq,
  prd.periodtypename,
  a.valtype,
  prd.startdate AS perioddate
FROM
  EXT.stel_DATA_FINALFV_DAYS_ann a
  INNER JOIN EXT.stel_temp_periodhierarchy prd ON a.perioddate BETWEEN prd.monthstartdate
  AND ADD_DAYS(prd.monthenddate, -1)
  AND prd.calendarname LIKE 'Singtel%Mon%'
  AND prd.periodtypename = 'year'
  INNER JOIN (
    SELECT
      /* ORIGSQL: (select a1.name, a1.genericattribute3 from EXT.stel_Temp_position a1 where a1.enddate= (SELECT MAX(b1.enddate) FROM EXT.stel_temp_position b1 where b1.name= a1.name)) (Select MAX(b1.enddate) from EXT.stel_temp_po(...) */
      a1.name,
      a1.genericattribute3
    FROM
      EXT.stel_Temp_position a1
    WHERE
      a1.enddate = (
        SELECT
          /* ORIGSQL: (Select MAX(b1.enddate) from EXT.stel_temp_position b1 where b1.name= a1.name) */
          MAX(b1.enddate)
        FROM
          EXT.stel_temp_position b1
        WHERE
          b1.name = a1.name
      )
  ) AS xy ON xy.name = a.geid
GROUP BY
  a.geid,
  xy.genericattribute3,
  prd.periodname,
  prd.periodseq,
  prd.periodtypename,
  a.valtype,
  prd.startdate;


v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Insert Qtry and Yr rollup in EXT.stel_DATA_FINALFV_DAYS:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255),(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | 'Insert Qtry and Yr rollup in  EXT.stel_DATA_FINALFV_DAYS:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  'Insert Qtry and Yr rollup in EXT.stel_DATA_FINALFV_DAYS Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || 'Insert Qtry and Yr rollup in  EXT.stel_DATA_FINALFV_DAYS:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
/* ORIGSQL: merge into EXT.stel_DATA_FINALFV_DAYS tgt using (SELECT CASE WHEN to_number(to_char(perioddate,'MM')) <=3 THEN to_number(to_char(perioddate,'MM')) +9 WHEN to_number(to_char(perioddate,'MM')) >3 THEN to_nu(...) */
MERGE INTO EXT.stel_DATA_FINALFV_DAYS AS tgt USING (
  SELECT
    /* ORIGSQL: (select CASE WHEN to_number(to_char(perioddate,'MM')) <=3 THEN to_number(to_char(perioddate,'MM')) +9 WHEN to_number(to_char(perioddate,'MM')) >3 THEN to_number(to_char(perioddate,'MM')) -3 END rnk,pe(...) */
    CASE
      WHEN TO_DECIMAL(TO_VARCHAR(perioddate, 'MM'), 38, 18) <= 3
      /* ORIGSQL: to_number(to_char(perioddate,'MM')) */
      THEN TO_DECIMAL(TO_VARCHAR(perioddate, 'MM'), 38, 18) + 9
      /* ORIGSQL: to_number(to_char(perioddate,'MM')) */
      WHEN TO_DECIMAL(TO_VARCHAR(perioddate, 'MM'), 38, 18) > 3
      /* ORIGSQL: to_number(to_char(perioddate,'MM')) */
      THEN TO_DECIMAL(TO_VARCHAR(perioddate, 'MM'), 38, 18) -3
      /* ORIGSQL: to_number(to_char(perioddate,'MM')) */
    END AS rnk,
    periodtype,
    geid,
    valtype,
    perioddate,
    periodname,
    channel
  FROM
    EXT.stel_DATA_FINALFV_DAYS
  WHERE
    periodtype = 'month'
  UNION
  SELECT
    /* ORIGSQL: select to_number(substr(periodname,2,1)) rnk,periodtype, geid, valtype, perioddate, periodname, channel from EXT.stel_DATA_FINALFV_DAYS where periodtype='quarter') AS src on (src.periodtype=tgt.periodtype(...) */
    TO_DECIMAL(SUBSTRING(periodname, 2, 1), 38, 18) AS rnk,
    periodtype,
    geid,
    valtype,
    perioddate,
    periodname,
    channel
  FROM
    EXT.stel_DATA_FINALFV_DAYS
  WHERE
    periodtype = 'quarter'
) AS src ON (
  src.periodtype = tgt.periodtype
  AND src.geid = tgt.geid
  AND src.valtype = tgt.valtype
  AND src.periodname = tgt.periodname
  AND src.channel = tgt.channel
)
WHEN MATCHED THEN
UPDATE
SET
  tgt.periodnumber = src.rnk;


v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Update periodnumber in EXT.stel_DATA_FINALFV_DAYS:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), 'Upda(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | 'Update periodnumber in  EXT.stel_DATA_FINALFV_DAYS:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  'Update periodnumber in  EXT.stel_DATA_FINALFV_DAYS Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || 'Update periodnumber in  EXT.stel_DATA_FINALFV_DAYS:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/* ORIGSQL: commit; */
COMMIT;


/* ORIGSQL: INSERT INTO EXT.stel_data_finalfv_ratio (Geid, channel, val, periodname, periodseq, periodtype, valtype, perioddate, periodnumber) select act.Geid, act.channel, act.val/exp.workingdaysinendmonth, act.peri(...) */
INSERT INTO
  EXT.stel_data_finalfv_ratio (
    Geid,
    channel,
    val,
    periodname,
    periodseq,
    periodtype,
    valtype,
    perioddate,
    periodnumber
  )
SELECT
  /* ORIGSQL: select act.Geid, act.channel, act.val/exp.workingdaysinendmonth, act.periodname, act.periodseq, act.periodtype, 'RATIO' valtype, act.perioddate, act.periodnumber from EXT.stel_data_finalfv_days act join s(...) */
  act.Geid,
  act.channel,
  act.val / exp.workingdaysinendmonth,
  act.periodname,
  act.periodseq,
  act.periodtype,
  'RATIO' AS valtype,
  act.perioddate,
  act.periodnumber
FROM
  EXT.stel_data_finalfv_days act
  INNER JOIN EXT.stel_temp_workingdays exp ON act.periodname = exp.periodname
  AND act.channel = exp.channel;


v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'insert into EXT.stel_DATA_FINALFV_RATIO:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), 'insert into ST(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | 'insert into EXT.stel_DATA_FINALFV_RATIO:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  'insert into EXT.stel_DATA_FINALFV_RATIO Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || 'insert into EXT.stel_DATA_FINALFV_RATIO:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
--there should be no situation where a left join is required here
/*
 SalesChannel, Frequency, Setting
 Advertising, M , NO_LEAVE (only hire and term date)
 DirectSales, , LEAVESETTING1
 LEAVESETTING1, HL,
 select * from ext.inbound_Data_leave
 M,
 Q,
 Y,
 */
/* ORIGSQL: commit; */
COMMIT;


/*
 merge into EXT.stel_DATA_FINALFV_ratio  tgt
 using (select  row_number() over(partition by periodtype, geid, valtype, channel order by perioddate) rnk
 ,periodtype, geid, valtype, perioddate, periodname, channel
 from EXT.stel_DATA_FINALFV_ratio
 ) src
 on (src.periodtype=tgt.periodtype and src.geid=tgt.geid and src.valtype=tgt.valtype
 and src.periodname=tgt.periodname and src.channel=tgt.channel)
 when matched then update set tgt.periodnumber=src.rnk;
 */
/* ORIGSQL: commit; */
COMMIT;


/* ORIGSQL: delete from ext.inbound_Data_Staging where trunc(filedate) =trunc(v_filedate) and filetype like 'FixedValue%General%Work%'; */
DELETE FROM
  ext.inbound_Data_Staging
WHERE
  -- sapdbmtk.sp_f_dbmtk_truncate_datetime(filedate, 'DD') = sapdbmtk.sp_f_dbmtk_truncate_datetime(:v_Filedate, 'DD')  /* ORIGSQL: trunc(v_filedate) */
  to_date(filedate) = to_date(: v_Filedate)
  /* ORIGSQL: trunc(filedate) */
  AND filetype LIKE 'FixedValue%General%Work%';


v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Delete FVGeneralWork in ext.inbound_DATA_STAGING:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), 'Delet(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | 'Delete FVGeneralWork in ext.inbound_DATA_STAGING:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  'Delete FVGeneralWork in ext.inbound_DATA_STAGING Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || 'Delete FVGeneralWork in ext.inbound_DATA_STAGING:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/* ORIGSQL: delete from ext.inbound_Data_Staging where trunc(filedate) =trunc(v_filedate) and filetype like 'FixedValue%Gener%Ratio%'; */
DELETE FROM
  ext.inbound_Data_Staging
WHERE
  -- sapdbmtk.sp_f_dbmtk_truncate_datetime(filedate, 'DD') = sapdbmtk.sp_f_dbmtk_truncate_datetime(:v_Filedate, 'DD')  /* ORIGSQL: trunc(v_filedate) */
  TO_DATE(filedate) = TO_DATE(: v_filedate)
  /* ORIGSQL: trunc(filedate) */
  AND filetype LIKE 'FixedValue%Gener%Ratio%';


/* ORIGSQL: commit; */
COMMIT;


v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || 'Delete FVGeneralRatio in ext.inbound_DATA_STAGING:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), 'Dele(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | 'Delete FVGeneralRatio in ext.inbound_DATA_STAGING:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  'Delete FVGeneralRatio in ext.inbound_DATA_STAGING Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || 'Delete FVGeneralRatio in ext.inbound_DATA_STAGING:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/**************/
--Monthly  
/* RESOLVE: Identifier not found: Table/view 'EXT.INBOUND_DATA_STAGING' not found */
/* ORIGSQL: insert into ext.inbound_Data_Staging (filename, filetype, filedate,FIELD1,FIELD2,FIELD3,FIELD4,FIELD5,FIELD6,FIELD7,FIELD8,FIELD9,FIELD10,FIELD11,FIELD12,FIELD13,FIELD14,FIELD15,FIELD16,FIELD17,FIELD18,FI(...) */
INSERT INTO
  ext.inbound_Data_Staging (
    filename,
    filetype,
    filedate,
    FIELD1,
    FIELD2,
    FIELD3,
    FIELD4,
    FIELD5,
    FIELD6,
    FIELD7,
    FIELD8,
    FIELD9,
    FIELD10,
    FIELD11,
    FIELD12,
    FIELD13,
    FIELD14,
    FIELD15,
    FIELD16,
    FIELD17,
    FIELD18,
    FIELD19
  )
SELECT
  /* ORIGSQL: select Workdays||'_'||to_Char(v_filedate,'YYYYMMDD'), Workdays, trunc(v_filedate), GEID,CHANNEL,to_char(trunc(yrstartdate),'YYYYMMDD'),0,null,null,trunc(yrstartdate),SUM(nvl(VAL1,0)),SUM(nvl(VAL2,0)),(...) */
  IFNULL(Workdays, '') | | '_' | | IFNULL(TO_VARCHAR(: v_Filedate, 'YYYYMMDD'), '') as filename,
  Workdays as filetype,
  -- sapdbmtk.sp_f_dbmtk_truncate_datetime(:v_Filedate, 'DD'),  /* ORIGSQL: trunc(v_filedate) */
  TO_VARCHAR(: v_Filedate, 'YYYY-MM-DD HH24:MI:SS') AS filedate,
  GEID as FIELD1,
  CHANNEL as FIELD2,
  -- TO_VARCHAR(sapdbmtk.sp_f_dbmtk_truncate_datetime(yrstartdate, 'DD'),'YYYYMMDD'),  /* ORIGSQL: to_char(trunc(yrstartdate),'YYYYMMDD') */
  TO_VARCHAR(yrstartdate, 'YYYYMMDD') as FIELD3,
  0 as FIELD4,
  NULL,
  NULL,
  -- sapdbmtk.sp_f_dbmtk_truncate_datetime(yrstartdate, 'DD'),  /* ORIGSQL: trunc(yrstartdate) */
  to_date(yrstartdate, 'YYYYMMDD'),
  -- SUM(IFNULL(VAL1,0)),  /* ORIGSQL: nvl(VAL1,0) */
  -- SUM(IFNULL(VAL2,0)),  /* ORIGSQL: nvl(VAL2,0) */
  -- SUM(IFNULL(VAL3,0)),  /* ORIGSQL: nvl(VAL3,0) */
  -- SUM(IFNULL(VAL4,0)),  /* ORIGSQL: nvl(VAL4,0) */
  -- SUM(IFNULL(VAL5,0)),  /* ORIGSQL: nvl(VAL5,0) */
  -- SUM(IFNULL(VAL6,0)),  /* ORIGSQL: nvl(VAL6,0) */
  -- SUM(IFNULL(VAL7,0)),  /* ORIGSQL: nvl(VAL7,0) */
  -- SUM(IFNULL(VAL8,0)),  /* ORIGSQL: nvl(VAL8,0) */
  -- SUM(IFNULL(VAL9,0)),  /* ORIGSQL: nvl(VAL9,0) */
  -- SUM(IFNULL(VAL10,0)),  /* ORIGSQL: nvl(VAL10,0) */
  -- SUM(IFNULL(VAL11,0)),  /* ORIGSQL: nvl(VAL11,0) */
  -- SUM(IFNULL(VAL12,0))  /* ORIGSQL: nvl(VAL12,0) */
  SUM(
    CASE
      WHEN periodnumber = 1 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val1,
  SUM(
    CASE
      WHEN periodnumber = 2 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val2,
  SUM(
    CASE
      WHEN periodnumber = 3 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val3,
  SUM(
    CASE
      WHEN periodnumber = 4 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val4,
  SUM(
    CASE
      WHEN periodnumber = 5 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val5,
  SUM(
    CASE
      WHEN periodnumber = 6 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val6,
  SUM(
    CASE
      WHEN periodnumber = 7 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val7,
  SUM(
    CASE
      WHEN periodnumber = 8 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val8,
  SUM(
    CASE
      WHEN periodnumber = 9 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val9,
  SUM(
    CASE
      WHEN periodnumber = 10 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val10,
  SUM(
    CASE
      WHEN periodnumber = 11 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val11,
  SUM(
    CASE
      WHEN periodnumber = 12 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val12
FROM
  (
    SELECT
      /* ORIGSQL: (select a.*, b.startdate as yrstartdate, lc.workdays, lc.ratio from EXT.stel_Data_finalfv_days a join EXT.stel_periodhierarchy@STELEXT b on a.perioddate=b.monthstartdate and b.periodtypename='year' join EXT.stel_(...) */
      a.*,
      b.startdate AS yrstartdate,
      lc.workdays,
      lc.ratio
    FROM
      ext.stel_Data_finalfv_days a
      INNER JOIN EXT.stel_periodhierarchy b ON a.perioddate = b.monthstartdate
      AND b.periodtypename = 'year'
      INNER JOIN ext.stel_cfg_leavechannel lc ON lc.saleschannel = a.channel
      AND lc.frequency = a.periodtype
      /* RESOLVE: Oracle Database link: Remote table/view 'EXT.stel_periodhierarchy@STELEXT' at Oracle DBlink 'STELEXT' is converted to HANA virtual table 'EXT.stel_periodhierarchy'; see 'dbmtk_create_remote_tables.sqlscript' for creating this virtual table. */
    WHERE
      a.periodtype = 'month'
      AND b.calendarname LIKE 'Singtel Monthly Calendar'
  )
  /* RESOLVE: Feature not supported in target DBMS: HANA does not support PIVOT/UNPIVOT; rewrite with available HANA features. */
  /*Deepan: Pivot not available in HANA*/
  /*  PIVOT
   (
   MAX(val)
   FOR periodnumber IN (1 AS Val1,2 AS Val2,3 AS Val3
   ,4 AS Val4,5 AS Val5,6 AS Val6,7 AS Val7,8 AS Val8,9 AS Val9
   ,10 AS Val10,11 AS Val11,12 AS Val12)
   )
   */
GROUP BY
  Workdays,
  geid,
  channel,
  yrstartdate;


/* ORIGSQL: trunc(yrstartdate) */
v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '1. insert into ext.inbound_DATA_STAGING:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), '1. insert into(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | '1. insert into ext.inbound_DATA_STAGING:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  '1. insert into ext.inbound_DATA_STAGING Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || '1. insert into ext.inbound_DATA_STAGING:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/* ORIGSQL: insert into ext.inbound_Data_Staging (filename, filetype, filedate,FIELD1,FIELD2,FIELD3,FIELD4,FIELD5,FIELD6,FIELD7,FIELD8,FIELD9,FIELD10,FIELD11,FIELD12,FIELD13,FIELD14,FIELD15,FIELD16,FIELD17,FIELD18,FI(...) */
INSERT INTO
  ext.inbound_Data_Staging (
    filename,
    filetype,
    filedate,
    FIELD1,
    FIELD2,
    FIELD3,
    FIELD4,
    FIELD5,
    FIELD6,
    FIELD7,
    FIELD8,
    FIELD9,
    FIELD10,
    FIELD11,
    FIELD12,
    FIELD13,
    FIELD14,
    FIELD15,
    FIELD16,
    FIELD17,
    FIELD18,
    FIELD19
  )
SELECT
  IFNULL(ratio, '') | | '_' | | IFNULL(TO_VARCHAR(: v_Filedate, 'YYYYMMDD'), ''),
  ratio,
  TO_VARCHAR(: v_Filedate, 'YYYY-MM-DD HH24:MI:SS') AS filedate,
  GEID,
  CHANNEL,
  TO_VARCHAR(yrstartdate, 'YYYYMMDD'),
  0,
  NULL,
  NULL,
  yrstartdate,
  SUM(
    CASE
      WHEN periodnumber = 1 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val1,
  SUM(
    CASE
      WHEN periodnumber = 2 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val2,
  SUM(
    CASE
      WHEN periodnumber = 3 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val3,
  SUM(
    CASE
      WHEN periodnumber = 4 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val4,
  SUM(
    CASE
      WHEN periodnumber = 5 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val5,
  SUM(
    CASE
      WHEN periodnumber = 6 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val6,
  SUM(
    CASE
      WHEN periodnumber = 7 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val7,
  SUM(
    CASE
      WHEN periodnumber = 8 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val8,
  SUM(
    CASE
      WHEN periodnumber = 9 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val9,
  SUM(
    CASE
      WHEN periodnumber = 10 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val10,
  SUM(
    CASE
      WHEN periodnumber = 11 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val11,
  SUM(
    CASE
      WHEN periodnumber = 12 THEN IFNULL(VAL, 0)
      ELSE 0
    END
  ) AS Val12
FROM
  (
    SELECT
      a.*,
      b.startdate AS yrstartdate,
      lc.workdays,
      lc.ratio
    FROM
      ext.stel_Data_finalfv_ratio a
      INNER JOIN EXT.stel_periodhierarchy b ON a.perioddate = b.monthstartdate
      AND b.periodtypename = 'year'
      INNER JOIN ext.stel_cfg_leavechannel lc ON lc.saleschannel = a.channel
      AND lc.frequency = a.periodtype
    WHERE
      a.periodtype = 'month'
      AND b.calendarname LIKE 'Singtel Monthly Calendar'
  ) AS PivotedData
GROUP BY
  ratio,
  geid,
  channel,
  yrstartdate;


v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '2. insert into ext.inbound_DATA_STAGING:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), '2. insert into(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | '2. insert into ext.inbound_DATA_STAGING:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  '2. insert into ext.inbound_DATA_STAGING Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || '2. insert into ext.inbound_DATA_STAGING:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/**************/
--Quarterly     
/* ORIGSQL: insert into ext.inbound_Data_Staging (filename, filetype, filedate,FIELD1,FIELD2,FIELD3,FIELD4,FIELD5,FIELD6,FIELD7,FIELD8,FIELD9,FIELD10,FIELD11) select Workdays||'_'||to_Char(v_filedate,'YYYYMMDD'), Wor(...) */
INSERT INTO
  ext.inbound_Data_Staging (
    filename,
    filetype,
    filedate,
    FIELD1,
    FIELD2,
    FIELD3,
    FIELD4,
    FIELD5,
    FIELD6,
    FIELD7,
    FIELD8,
    FIELD9,
    FIELD10,
    FIELD11
  )
SELECT
  IFNULL(Workdays, '') | | '_' | | IFNULL(TO_VARCHAR(: v_Filedate, 'YYYYMMDD'), ''),
  Workdays,
  -- sapdbmtk.sp_f_dbmtk_truncate_datetime(:v_Filedate, 'DD'),  /* ORIGSQL: trunc(v_filedate) */
: v_Filedate,
  GEID,
  CHANNEL,
  TO_VARCHAR(yrstartdate, 'YYYYMMDD'),
  /* ORIGSQL: to_char(yrstartdate,'YYYYMMDD') */
  0,
  NULL,
  NULL,
  TO_VARCHAR(yrstartdate, 'YYYYMMDD'),
  /* ORIGSQL: to_char(yrstartdate,'YYYYMMDD') */
  /*Deepan: Using case instead of pivot*/
  SUM(
    CASE
      WHEN periodnumber = 1 THEN VAL
      ELSE 0
    END
  ) AS Val1,
  SUM(
    CASE
      WHEN periodnumber = 2 THEN VAL
      ELSE 0
    END
  ) AS Val2,
  SUM(
    CASE
      WHEN periodnumber = 3 THEN VAL
      ELSE 0
    END
  ) AS Val3,
  SUM(
    CASE
      WHEN periodnumber = 4 THEN VAL
      ELSE 0
    END
  ) AS Val4 -- SUM(IFNULL(VAL1,0)),  /* ORIGSQL: nvl(VAL1,0) */
  -- SUM(IFNULL(VAL2,0)),  /* ORIGSQL: nvl(VAL2,0) */
  -- SUM(IFNULL(VAL3,0)),  /* ORIGSQL: nvl(VAL3,0) */
  -- SUM(IFNULL(VAL4,0))  /* ORIGSQL: nvl(VAL4,0) */
FROM
  (
    SELECT
      /* ORIGSQL: (select a.*, b.startdate as yrstartdate, lc.workdays, lc.ratio from EXT.stel_Data_finalfv_days a join EXT.stel_periodhierarchy@STELEXT b on a.perioddate=b.monthstartdate and b.periodtypename='year' join EXT.stel_(...) */
      a.*,
      /*Deepan: Using case instead of pivot*/
      b.startdate AS yrstartdate,
      lc.workdays,
      lc.ratio
    FROM
      ext.stel_Data_finalfv_days a
      INNER JOIN EXT.stel_periodhierarchy b ON a.perioddate = b.monthstartdate
      AND b.periodtypename = 'year'
      INNER JOIN ext.stel_cfg_leavechannel lc ON lc.saleschannel = a.channel
      AND lc.frequency = a.periodtype
    WHERE
      a.periodtype = 'quarter'
      AND b.calendarname LIKE 'Singtel Monthly Calendar'
  ) t
GROUP BY
  workdays,
  geid,
  channel,
  yrstartdate
UNION ALL
SELECT
  IFNULL(ratio, '') | | '_' | | IFNULL(TO_VARCHAR(: v_Filedate, 'YYYYMMDD'), ''),
  ratio,
  to_date(: v_Filedate),
  /* ORIGSQL: trunc(v_filedate) */
  GEID,
  NULL,
  -- Placeholder for CHANNEL to match the number of columns
  TO_VARCHAR(yrstartdate, 'YYYYMMDD'),
  /* ORIGSQL: to_char(yrstartdate,'YYYYMMDD') */
  0,
  NULL,
  NULL,
  TO_VARCHAR(yrstartdate, 'YYYYMMDD'),
  /* ORIGSQL: to_char(trunc(yrstartdate),'YYYYMMDD') */
  /*Deepan: Using case instead of pivot in HANA*/
  SUM(
    CASE
      WHEN periodnumber = 1 THEN VAL
      ELSE 0
    END
  ) AS Val1,
  SUM(
    CASE
      WHEN periodnumber = 2 THEN VAL
      ELSE 0
    END
  ) AS Val2,
  SUM(
    CASE
      WHEN periodnumber = 3 THEN VAL
      ELSE 0
    END
  ) AS Val3,
  SUM(
    CASE
      WHEN periodnumber = 4 THEN VAL
      ELSE 0
    END
  ) AS Val4 -- SUM(IFNULL(VAL1,0)),  /* ORIGSQL: nvl(VAL1,0) */
  -- SUM(IFNULL(VAL2,0)),  /* ORIGSQL: nvl(VAL2,0) */
  -- SUM(IFNULL(VAL3,0)),  /* ORIGSQL: nvl(VAL3,0) */
  -- SUM(IFNULL(VAL4,0))  /* ORIGSQL: nvl(VAL4,0) */
FROM
  (
    SELECT
      /* ORIGSQL: (select a.*, b.startdate as yrstartdate, lc.workdays, lc.ratio from EXT.stel_Data_finalfv_ratio a join EXT.stel_periodhierarchy@STELEXT b on a.perioddate=b.monthstartdate and b.periodtypename='year' join stel(...) */
      a.*,
      b.startdate AS yrstartdate,
      lc.workdays,
      lc.ratio
    FROM
      ext.stel_Data_finalfv_ratio a
      INNER JOIN EXT.stel_periodhierarchy b ON a.perioddate = b.monthstartdate
      AND b.periodtypename = 'year'
      INNER JOIN ext.stel_cfg_leavechannel lc ON lc.saleschannel = a.channel
      AND lc.frequency = a.periodtype
    WHERE
      a.periodtype = 'quarter'
      AND b.calendarname LIKE 'Singtel Monthly Calendar'
  ) t
  /* RESOLVE: Feature not supported in target DBMS: HANA does not support PIVOT/UNPIVOT; rewrite with available HANA features. */
  /*Deepan: Pivot not available in HANA*/
  /* PIVOT
   (
   MAX(val)
   FOR periodnumber IN (1 AS Val1,2 AS Val2,3 AS Val3
   ,4 AS Val4)
   )
   */
  --Commented to Remove Channel Data since Proration is at Person level - Aug 25 2019
  --group by ratio, geid, channel, yrstartdate
GROUP BY
  ratio,
  geid,
  yrstartdate;


v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '3. insert into ext.inbound_DATA_STAGING:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), '3. insert into(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | '3. insert into ext.inbound_DATA_STAGING:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  '3. insert into ext.inbound_DATA_STAGING Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || '3. insert into ext.inbound_DATA_STAGING:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/* ORIGSQL: commit; */
COMMIT;


/**************/
--Annual        
/* ORIGSQL: insert into ext.inbound_Data_Staging (filename, filetype, filedate,FIELD1,FIELD2,FIELD3,FIELD4,FIELD5,FIELD6,FIELD7,FIELD8) select workdays||'_'||to_Char(v_filedate,'YYYYMMDD'), workdays, trunc(v_filedate(...) */
INSERT INTO
  ext.inbound_Data_Staging (
    filename,
    filetype,
    filedate,
    FIELD1,
    FIELD2,
    FIELD3,
    FIELD4,
    FIELD5,
    FIELD6,
    FIELD7,
    FIELD8
  )
SELECT
  /* ORIGSQL: select workdays||'_'||to_Char(v_filedate,'YYYYMMDD'), workdays, trunc(v_filedate), GEID,CHANNEL,PERIODNAME,0,null,null,trunc(add_months(PERIODDATE,-3),'YYYY'),SUM(nvl(VAL,0)) from (SELECT * FROM EXT.stel_(...) */
  IFNULL(workdays, '') | | '_' | | IFNULL(TO_VARCHAR(: v_Filedate, 'YYYYMMDD'), ''),
  workdays,
  to_date(: v_Filedate),
  /* ORIGSQL: trunc(v_filedate) */
  GEID,
  CHANNEL,
  PERIODNAME,
  0,
  NULL,
  NULL,
  ext.trunc(ADD_MONTHS(PERIODDATE, -3), 'YEAR'),
  /* ORIGSQL: trunc(add_months(PERIODDATE,-3),'YYYY') */
  SUM(IFNULL(VAL, 0))
  /* ORIGSQL: nvl(VAL,0) */
FROM
  (
    SELECT
      /* ORIGSQL: (select * from EXT.stel_Data_finalfv_days fvr join EXT.stel_cfg_leavechannel lc on lc.saleschannel=fvr.channel and lc.frequency=fvr.periodtype where periodtype='year') */
      *
    FROM
      EXT.stel_Data_finalfv_days fvr
      INNER JOIN EXT.stel_cfg_leavechannel lc ON lc.saleschannel = fvr.channel
      AND lc.frequency = fvr.periodtype
    WHERE
      periodtype = 'year'
  ) AS dbmtk_corrname_3522
GROUP BY
  workdays,
  geid,
  channel,
  ext.trunc(ADD_MONTHS(PERIODDATE, -3), 'YEAR'),
  periodname
  /* ORIGSQL: trunc(add_months(PERIODDATE,-3),'YYYY') */
UNION ALL
SELECT
  /* ORIGSQL: select ratio||'_'||to_Char(v_filedate,'YYYYMMDD'), ratio, trunc(v_filedate), GEID,CHANNEL,PERIODNAME,0,null,null,trunc(add_months(PERIODDATE,-3),'YYYY'),SUM(nvl(VAL,0)) from (SELECT * FROM EXT.stel_Data_f(...) */
  IFNULL(ratio, '') | | '_' | | IFNULL(TO_VARCHAR(: v_Filedate, 'YYYYMMDD'), ''),
  ratio,
  to_date(: v_Filedate),
  /* ORIGSQL: trunc(v_filedate) */
  GEID,
  CHANNEL,
  PERIODNAME,
  0,
  NULL,
  NULL,
  ext.trunc(ADD_MONTHS(PERIODDATE, -3), 'YEAR'),
  /* ORIGSQL: trunc(add_months(PERIODDATE,-3),'YYYY') */
  SUM(IFNULL(VAL, 0))
  /* ORIGSQL: nvl(VAL,0) */
FROM
  (
    SELECT
      /* ORIGSQL: (select * from EXT.stel_Data_finalfv_ratio fvr join EXT.stel_cfg_leavechannel lc on lc.saleschannel=fvr.channel and lc.frequency=fvr.periodtype where periodtype='year') */
      *
    FROM
      EXT.stel_Data_finalfv_ratio fvr
      INNER JOIN EXT.stel_cfg_leavechannel lc ON lc.saleschannel = fvr.channel
      AND lc.frequency = fvr.periodtype
    WHERE
      periodtype = 'year'
  ) AS dbmtk_corrname_3526
GROUP BY
  ratio,
  geid,
  channel,
  ext.trunc(ADD_MONTHS(PERIODDATE, -3), 'YEAR'),
  periodname
  /* ORIGSQL: trunc(add_months(PERIODDATE,-3),'YYYY') */
UNION ALL
SELECT
  /* ORIGSQL: select expectedworkdays||'_'||to_Char(v_filedate,'YYYYMMDD'), expectedworkdays, trunc(v_filedate), GEID,CHANNEL,PERIODNAME,0,null,null,trunc(add_months(PERIODDATE,-3),'YYYY'),MAX(nvl(workingdaysinendm(...) */
  IFNULL(expectedworkdays, '') | | '_' | | IFNULL(TO_VARCHAR(: v_Filedate, 'YYYYMMDD'), ''),
  expectedworkdays,
  to_date(: v_Filedate),
  /* ORIGSQL: trunc(v_filedate) */
  GEID,
  CHANNEL,
  PERIODNAME,
  0,
  NULL,
  NULL,
  ext.trunc(ADD_MONTHS(PERIODDATE, -3), 'YEAR'),
  /* ORIGSQL: trunc(add_months(PERIODDATE,-3),'YYYY') */
  MAX(IFNULL(workingdaysinendmonth, 0))
  /* ORIGSQL: nvl(workingdaysinendmonth,0) */
FROM
  (
    SELECT
      /* ORIGSQL: (select distinct fvr.*, lc.*, wd.workingdaysinendmonth from EXT.stel_Data_finalfv_ratio fvr join EXT.stel_cfg_leavechannel lc on lc.saleschannel=fvr.channel and lc.frequency=fvr.periodtype join EXT.stel_temp_work(...) */
      DISTINCT fvr.*,
      lc.*,
      wd.workingdaysinendmonth
    FROM
      EXT.stel_Data_finalfv_ratio fvr
      INNER JOIN EXT.stel_cfg_leavechannel lc ON lc.saleschannel = fvr.channel
      AND lc.frequency = fvr.periodtype
      INNER JOIN EXT.stel_temp_workingdays wd ON wd.channel = lc.saleschannel
      AND wd.periodtypename = 'year'
    WHERE
      periodtype = 'year'
  ) AS dbmtk_corrname_3530
GROUP BY
  expectedworkdays,
  geid,
  channel,
  ext.trunc(ADD_MONTHS(PERIODDATE, -3), 'YEAR'),
  periodname;


/* ORIGSQL: trunc(add_months(PERIODDATE,-3),'YYYY') */
v_rowcount =:: ROWCOUNT;


/* ORIGSQL: SQL%ROWCOUNT */
/* ORIGSQL: SP_LOGGER (SUBSTR(v_proc_name || '4. insert into ext.inbound_DATA_STAGING:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255), '4. insert into(...) */
CALL EXT.STEL_SP_LOGGER (
  SUBSTRING (
    IFNULL (: v_proc_name, '') | | '4. insert into ext.inbound_DATA_STAGING:' | | IFNULL (: v_parameter.file_type, '') | | '-FileName:' | | IFNULL (: v_parameter.file_name, '') | | '-Date:' | | IFNULL (: v_parameter.file_date, ''),
    1,
    255
  ),
  '4. insert into ext.inbound_DATA_STAGING Execution Completed',
: v_rowcount,
  NULL,
  NULL
);


/* ORIGSQL: SUBSTR(v_proc_name || '4. insert into ext.inbound_DATA_STAGING:' || :v_parameter.file_type || '-FileName:' || :v_parameter.file_name || '-Date:' || :v_parameter.file_date, 1, 255) */
/* ORIGSQL: commit; */
COMMIT;


/*  sp_inbound_leavetransfer(v_filedate,'FixedValue-GeneralMth-Ratio' );
 ext.inbound_trigger();
 
 sp_inbound_leavetransfer(v_filedate,'FixedValue-GeneralAnnual-Ratio' );
 ext.inbound_trigger();
 
 sp_inbound_leavetransfer(v_filedate,'FixedValue-GeneralAnnual-Workdays' );
 ext.inbound_trigger();
 
 sp_inbound_leavetransfer(v_filedate,'FixedValue-GeneralQtr-Ratio' );
 ext.inbound_trigger();
 
 sp_inbound_leavetransfer(v_filedate,'FixedValue-GeneralQtr-Workdays' );
 ext.inbound_trigger();
 
 sp_inbound_leavetransfer(v_filedate,'FixedValue-GeneralMth-Workdays' );
 ext.inbound_trigger();
 */
END;