/*
 * This file was extracted from 'C:/HANAMigrations/STELEXT/OracleObjects/PKG_REPORTING_EXTRACT_R2_1.sql' 
 * at 05-Jun-2024 10:44:42 with the 'extract_offline' command of SAP Advanced SQL Migration v.3.5.3 (64791)
 * User config setting for 'extract_offline' (id=132) was '0'.
 */




  CREATE OR REPLACE  PACKAGE BODY PKG_REPORTING_EXTRACT_R2 IS

PROCEDURE prc_setVariables(vPeriodSeq IN CS_PERIODCALENDAR.periodseq%TYPE,vProcessingUnitSeq IN CS_PROCESSINGUNIT.processingunitseq%TYPE,vCalendarSeq IN CS_PERIODCALENDAR.calendarseq%TYPE) IS
/****************************************************************************************************************
    The purpose of this procedure is to initialize all the required variables.

    Date        Author     Description
    ------------------------------------------------------------------------------------------------------------
--- 30 Nov 2017 Tharanikumar  Initial release
***************************************************************************************************************/
BEGIN
  -- Get Period.
  SELECT * INTO vPeriodRow  FROM cs_period prd  WHERE prd.removedate = cEndofTime AND prd.periodseq = vPeriodSeq;

  -- Get Processing Unit.
  SELECT * INTO vProcessingUnitRow FROM cs_processingunit pu WHERE pu.processingunitseq = vProcessingUnitSeq;

  -- Get Calendar.
  SELECT * INTO vCalendarRow FROM cs_calendar cal WHERE cal.removedate = cEndofTime AND cal.calendarseq = vCalendarSeq;

/*
  -- Get Current Year Start and End Dates  
  SELECT per.startdate,  per.enddate - 1
  INTO vCurYrStartDate, vCurYrEndDate
  FROM   cs_period per
  WHERE per.periodSeq = (SELECT per1.periodseq
    FROM cs_period  per1, cs_periodtype pt1
    WHERE per1.PeriodTypeseq = pt1.PeriodTypeseq
        AND pt1.Name = 'year'
        START WITH per1.periodseq = vperiodseq
        CONNECT BY PRIOR per1.parentseq = per1.periodseq
        );
*/
END;

PROCEDURE prc_DeleteTable(pTableName VARCHAR2,pProcessingUnitSeq IN CS_PROCESSINGUNIT.processingunitseq%TYPE) IS
/****************************************************************************************************************
    The purpose of this procedure is to truncate the supplied table.

    Date        Author     Description
    ------------------------------------------------------------------------------------------------------------
--- 30 Nov 2017 Tharanikumar  Initial release
****************************************************************************************************************/
BEGIN
   prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_DeleteTable','Delete started',''||pTableName,NULL);
   EXECUTE IMMEDIATE 'Delete from '||pTableName||' where processingunitseq = '||pProcessingUnitSeq;
      prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_DeleteTable','Delete End',''||pTableName,NULL);
   COMMIT;
   --prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_DeleteTable','START','Base Table Delete '||pTableName,NULL);
END;

PROCEDURE prc_TruncateTablePartition(pTableName VARCHAR2,vPeriodSeq IN CS_PERIODCALENDAR.periodseq%TYPE,vProcessingUnitSeq IN CS_PROCESSINGUNIT.processingunitseq%TYPE,vCalendarSeq IN CS_PERIODCALENDAR.calendarseq%TYPE) IS
/****************************************************************************************************************
    The purpose of this procedure is to truncate a set of Partitions of the supplied table.

    Date        Author     Description
    ------------------------------------------------------------------------------------------------------------

****************************************************************************************************************/
BEGIN
   prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_TruncateTablePartition','START','Truncating Table Partitions for : '||pTableName,NULL);

   -- Partition truncate code to be included here.
  -- Include a call to OD_GetPeriodSubPartitionName;
  -- Will require to raise a call for this one.

END;

FUNCTION fnc_PipelineWasRun(vPeriodSeq IN CS_PERIODCALENDAR.periodseq%TYPE,vProcessingUnitSeq IN CS_PROCESSINGUNIT.processingunitseq%TYPE,vCalendarSeq IN CS_PERIODCALENDAR.calendarseq%TYPE)
RETURN BOOLEAN IS
/****************************************************************************************************************
    The purpose of this procedure is to check whether a pipeline was run since the last Report Generation.
    If no new Pipeline are found, then don't execute the Reporting Stored Procedures.

    Date        Author     Description
    ------------------------------------------------------------------------------------------------------------

****************************************************************************************************************/
BEGIN
  -- Include a call to odsutils.istorunodsprocs (pprocessingunitseq, pperiodseq);
  -- Will require to raise a call for this one.
  RETURN TRUE;
END;

PROCEDURE prc_AnalyzeTable(pTableName VARCHAR2) IS
/****************************************************************************************************************
    The purpose of this procedure is to analyze reporting tables and to keep the statistics upto date for performance.

    Date        Author     Description
    ------------------------------------------------------------------------------------------------------------
--- 30 Nov 2017 Tharanikumar  Initial release
****************************************************************************************************************/
BEGIN
	  prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_AnalyzeTable','prc_AnalyzeTable started',''||pTableName,NULL);
    DBMS_STATS.gather_table_stats(ownname => '',tabname => pTableName,estimate_percent => DBMS_STATS.auto_sample_size);
     prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_AnalyzeTable','prc_AnalyzeTable ended',''||pTableName,NULL);
END;
PROCEDURE prc_AnalyzeTableSubpartition(pExtUser VARCHAR2, pRptTableName VARCHAR2, pSubPartitionName VARCHAR2) IS
/****************************************************************************************************************
    The purpose of this procedure is to analyze reporting tables and to keep the statistics upto date for performance.

    Date        Author     Description
    ------------------------------------------------------------------------------------------------------------
--- 30 Nov 2017 Tharanikumar  Initial release
****************************************************************************************************************/
BEGIN
	prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_AnalyzeTableSubpartition','prc_AnalyzeTableSubpartition started',''||pRptTableName,NULL);
    DBMS_STATS.gather_table_stats
        (ownname => pExtUser,
        tabname => pRptTableName,
        partname => pSubPartitionName,
        method_opt => 'FOR ALL INDEXED COLUMNS size AUTO',
        DEGREE => 1,
        CASCADE => TRUE,
        estimate_percent => 1);
       prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_AnalyzeTableSubpartition','prc_AnalyzeTableSubpartition ended',''||pRptTableName,NULL);
END;
PROCEDURE prc_AddTableSubpartition(vExtUser VARCHAR2, vTCTemplateTable VARCHAR2, vTCSchemaName VARCHAR2, vTenantId VARCHAR2, vProcessingUnitSeq IN cs_processingunit.processingunitseq%TYPE, vPeriodSeq IN cs_period.periodseq%TYPE, vRptTableName VARCHAR2) IS
/****************************************************************************************************************
    The purpose of this procedure is to create new subpartitions for the custom reporting tables as needed.

    Date        Author     Description
    ------------------------------------------------------------------------------------------------------------
--- 30 Nov 2017 Tharanikumar  Initial release
****************************************************************************************************************/

CURSOR c_partitioncheck(p_rpttable varchar2, p_tableowner varchar2, p_partition_name varchar2) IS
SELECT 'X'
           FROM all_tab_partitions
           WHERE table_name = p_rpttable
           AND table_owner=p_tableowner
           AND partition_name=p_partition_name;


vLong           long;
vVarchar2       varchar2(2000);

v_PartitionAvail CHAR := NULL;
v_FirstCheck CHAR := NULL;
v_partitionname varchar2(100) := NULL;

BEGIN

      prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_AddTableSubpartition','START','Auto create subpartitions' ,NULL);
        FOR row IN (SELECT subpartition_name, high_value
            FROM all_tab_subpartitions
            WHERE table_name = vTCTemplateTable
                AND table_owner=vTCSchemaName
                AND subpartition_name IN (
                (SELECT subpartition_name
                FROM all_tab_subpartitions
                WHERE table_name = vTCTemplateTable
                    AND table_owner=vTCSchemaName
                    AND subpartition_name = (SELECT OD_GetPeriodSubPartitionName(vTCSchemaName, vTenantId, vProcessingUnitSeq, vPeriodSeq, vTCTemplateTable) FROM DUAL)
                MINUS
                    SELECT subpartition_name
                    FROM all_tab_subpartitions
                    WHERE table_name = vrpttablename
                        AND table_owner=vTenantId||'EXT')))
        LOOP

           v_partitionname := SUBSTR(row.subpartition_name, 0, 12);
           IF v_FirstCheck IS NULL THEN
           v_FirstCheck := 'X';

           prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_AddTableSubpartition','START','Auto create partition' ,v_partitionname);

           OPEN c_partitioncheck(vRptTableName,vTenantId||'EXT',v_partitionname);
           FETCH c_partitioncheck INTO v_PartitionAvail;
           CLOSE c_partitioncheck;

           IF v_PartitionAvail IS NULL THEN

            SELECT high_value valueLong INTO vLong
            FROM all_tab_partitions
            WHERE table_name = UPPER(vTCTemplateTable)
            AND    table_owner = UPPER(vTCSchemaName)
            AND partition_name = v_partitionname;

           vVarchar2 := vLong;

           EXECUTE IMMEDIATE 'ALTER TABLE ' ||vRptTableName||
                ' ADD PARTITION '|| v_partitionname ||
                ' VALUES LESS THAN(' ||vVarchar2||') TABLESPACE TALLYDATA' ;

           END IF;

           END IF;

            EXECUTE IMMEDIATE 'ALTER TABLE ' ||vRptTableName||
                ' MODIFY PARTITION '|| SUBSTR(row.subpartition_name, 0, 12) ||
                ' ADD SUBPARTITION ' || row.subpartition_name ||
                ' VALUES (' ||row.high_value||') TABLESPACE TALLYDATA' ;

        END LOOP;

END;
PROCEDURE prc_TruncateTableSubpartition(vRptTableName VARCHAR2, vSubpartitionName VARCHAR2) IS
/****************************************************************************************************************
    The purpose of this procedure is to truncate a specified subpartition.

    Date        Author     Description
    ------------------------------------------------------------------------------------------------------------
--- 30 Nov 2017 Tharanikumar  Initial release
****************************************************************************************************************/
BEGIN
        prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_TruncateTableSubpartition','START','Start truncate table '||vSubPartitionName,NULL);
        EXECUTE IMMEDIATE 'ALTER TABLE ' ||vRptTableName || ' TRUNCATE SUBPARTITION ' || vSubPartitionName || ' DROP STORAGE' ;
END;
PROCEDURE prc_buildbasetables(vPeriodSeq IN CS_PERIODCALENDAR.periodseq%TYPE,vProcessingUnitSeq IN CS_PROCESSINGUNIT.processingunitseq%TYPE,vCalendarSeq IN CS_PERIODCALENDAR.calendarseq%TYPE) IS
/****************************************************************************************************************
    The purpose of this procedure is to populate reporting base tables.

    Date        Author     Description
    ------------------------------------------------------------------------------------------------------------
--- 30 Nov 2017 Tharanikumar  Initial release
****************************************************************************************************************/

Cursor c_pareporting is
    SELECT par.*, SYSDATE
    FROM cs_pareporting par
    WHERE par.processingunitseq = vProcessingUnitSeq
        AND par.effectivestartdate <  vPeriodRow.enddate
        AND par.effectiveenddate >= vPeriodRow.startdate
        AND effectiveenddate = (SELECT MAX(effectiveenddate)
            FROM cs_pareporting
            WHERE effectivestartdate < vPeriodRow.enddate
                AND effectiveenddate >= vPeriodRow.startdate
                AND removedate = cendoftime   
                AND par.descendantpositionseq = descendantpositionseq
                AND par.descendantuserid = descendantuserid
				AND par.Ancestorpositionseq = ancestorpositionseq) -- Siva: Added this condition to form hierachy correctly
        AND par.removedate = cendoftime;





   TYPE HierType IS TABLE OF RPT_BASE_PAREPORTING%ROWTYPE INDEX BY PLS_INTEGER;
   vPaReporting HierType;

BEGIN
    prc_setVariables(vperiodseq, vprocessingunitseq, vcalendarseq);

    prc_AnalyzeTable('rpt_base_padimension');
    prc_base_padimension(vperiodseq, vprocessingunitseq, vcalendarseq);
    prc_AnalyzeTable('rpt_base_padimension');

    prc_AnalyzeTable('rpt_base_salestransaction');
    prc_base_salestransaction(vperiodseq, vprocessingunitseq, vcalendarseq);
    prc_AnalyzeTable('rpt_base_salestransaction');


    prc_AnalyzeTable('rpt_base_pareporting');    

    OPEN c_pareporting;
    LOOP
    BEGIN

         FETCH c_pareporting BULK COLLECT INTO vPaReporting LIMIT 10000;

         FORALL i IN 1..vPaReporting.COUNT
         INSERT /*+ APPEND PARALLEL */ INTO stelext.rpt_base_pareporting NOLOGGING VALUES vPaReporting(i);                
         COMMIT;

         EXIT WHEN c_pareporting%NOTFOUND;         
    END;
    END LOOP; 
    CLOSE c_pareporting;      

    prc_AnalyzeTable('rpt_base_deposit');
    /*INSERT INTO stelext.rpt_base_deposit
(SELECT d.*, SYSDATE*/
    --added by kyap, column values as there's an error on column mismatch
    INSERT INTO stelext.rpt_base_deposit (
        TENANTID,
        DEPOSITSEQ,
        NAME,
        PAYEESEQ,
        POSITIONSEQ,
        PERIODSEQ,
        PIPELINERUNSEQ,
        ORIGINTYPEID,
        PIPELINERUNDATE,
        BUSINESSUNITMAP,
        PREADJUSTEDVALUE,
        UNITTYPEFORPREADJUSTEDVALUE,
        VALUE,
        UNITTYPEFORVALUE,
        EARNINGGROUPID,
        EARNINGCODEID,
        RULESEQ,
        ISHELD,
        RELEASEDATE,
        DEPOSITDATE,
        REASONSEQ,
        COMMENTS,
        GENERICATTRIBUTE1,
        GENERICATTRIBUTE2,
        GENERICATTRIBUTE3,
        GENERICATTRIBUTE4,
        GENERICATTRIBUTE5,
        GENERICATTRIBUTE6,
        GENERICATTRIBUTE7,
        GENERICATTRIBUTE8,
        GENERICATTRIBUTE9,
        GENERICATTRIBUTE10,
        GENERICATTRIBUTE11,
        GENERICATTRIBUTE12,
        GENERICATTRIBUTE13,
        GENERICATTRIBUTE14,
        GENERICATTRIBUTE15,
        GENERICATTRIBUTE16,
        GENERICNUMBER1,
        UNITTYPEFORGENERICNUMBER1,
        GENERICNUMBER2,
        UNITTYPEFORGENERICNUMBER2,
        GENERICNUMBER3,
        UNITTYPEFORGENERICNUMBER3,
        GENERICNUMBER4,
        UNITTYPEFORGENERICNUMBER4,
        GENERICNUMBER5,
        UNITTYPEFORGENERICNUMBER5,
        GENERICNUMBER6,
        UNITTYPEFORGENERICNUMBER6,
        GENERICDATE1,
        GENERICDATE2,
        GENERICDATE3,
        GENERICDATE4,
        GENERICDATE5,
        GENERICDATE6,
        GENERICBOOLEAN1,
        GENERICBOOLEAN2,
        GENERICBOOLEAN3,
        GENERICBOOLEAN4,
        GENERICBOOLEAN5,
        GENERICBOOLEAN6,
        PROCESSINGUNITSEQ,
        LOADDTTM)
        (select
        d.TENANTID,
        d.DEPOSITSEQ,
        d.NAME,
        d.PAYEESEQ,
        d.POSITIONSEQ,
        d.PERIODSEQ,
        d.PIPELINERUNSEQ,
        d.ORIGINTYPEID,
        d.PIPELINERUNDATE,
        d.BUSINESSUNITMAP,
        d.PREADJUSTEDVALUE,
        d.UNITTYPEFORPREADJUSTEDVALUE,
        d.VALUE,
        d.UNITTYPEFORVALUE,
        d.EARNINGGROUPID,
        d.EARNINGCODEID,
        d.RULESEQ,
        d.ISHELD,
        d.RELEASEDATE,
        d.DEPOSITDATE,
        d.REASONSEQ,
        d.COMMENTS,
        d.GENERICATTRIBUTE1,
        d.GENERICATTRIBUTE2,
        d.GENERICATTRIBUTE3,
        d.GENERICATTRIBUTE4,
        d.GENERICATTRIBUTE5,
        d.GENERICATTRIBUTE6,
        d.GENERICATTRIBUTE7,
        d.GENERICATTRIBUTE8,
        d.GENERICATTRIBUTE9,
        d.GENERICATTRIBUTE10,
        d.GENERICATTRIBUTE11,
        d.GENERICATTRIBUTE12,
        d.GENERICATTRIBUTE13,
        d.GENERICATTRIBUTE14,
        d.GENERICATTRIBUTE15,
        d.GENERICATTRIBUTE16,
        d.GENERICNUMBER1,
        d.UNITTYPEFORGENERICNUMBER1,
        d.GENERICNUMBER2,
        d.UNITTYPEFORGENERICNUMBER2,
        d.GENERICNUMBER3,
        d.UNITTYPEFORGENERICNUMBER3,
        d.GENERICNUMBER4,
        d.UNITTYPEFORGENERICNUMBER4,
        d.GENERICNUMBER5,
        d.UNITTYPEFORGENERICNUMBER5,
        d.GENERICNUMBER6,
        d.UNITTYPEFORGENERICNUMBER6,
        d.GENERICDATE1,
        d.GENERICDATE2,
        d.GENERICDATE3,
        d.GENERICDATE4,
        d.GENERICDATE5,
        d.GENERICDATE6,
        d.GENERICBOOLEAN1,
        d.GENERICBOOLEAN2,
        d.GENERICBOOLEAN3,
        d.GENERICBOOLEAN4,
        d.GENERICBOOLEAN5,
        d.GENERICBOOLEAN6,
        d.PROCESSINGUNITSEQ,
        sysdate
    FROM cs_deposit d, cs_period per
    WHERE d.periodseq = per.periodseq
        AND d.processingunitseq = vProcessingUnitSeq
        --AND per.startdate >= vCurYrStartDate 
        --AND per.enddate <= vPeriodRow.enddate
        AND per.removedate = cendoftime   
        AND (per.periodseq = vPeriodSeq OR per.shortname IN ('Mar', 'Jun', 'Sep', 'Dec'))
        AND (d.originTypeId IN ('manual', 'imported')
        OR (d.pipelinerunseq IN (SELECT pr.pipelineRunSeq
            FROM cs_pipelinerun pr, cs_stagesummary ss, cs_stagetype st
            WHERE pr.pipelineRunSeq = ss.pipelineRunSeq
            AND ss.stageTypeSeq = st.stageTypeSeq
            AND st.name = 'Reward'
            AND pr.periodSeq = vperiodseq
            AND pr.processingunitseq = vprocessingunitseq
            AND ss.isactive = 1)
            )));
    COMMIT;
    prc_AnalyzeTable('rpt_base_deposit');

    prc_AnalyzeTable('rpt_base_incentive');
    /*INSERT INTO stelext.rpt_base_incentive
(SELECT i.*, SYSDATE*/
    --added by kyap, column values as there's an error on column mismatch
    INSERT INTO stelext.rpt_base_incentive(
        TENANTID,
        INCENTIVESEQ,
        NAME,
        PAYEESEQ,
        POSITIONSEQ,
        PERIODSEQ,
        PIPELINERUNSEQ,
        PIPELINERUNDATE,
        PLANSEQ,
        RULESEQ,
        VALUE,
        UNITTYPEFORVALUE,
        ISACTIVE,
        RELEASEDATE,
        QUOTA,
        UNITTYPEFORQUOTA,
        ATTAINMENT,
        UNITTYPEFORATTAINMENT,
        BUSINESSUNITMAP,
        GENERICATTRIBUTE1,
        GENERICATTRIBUTE2,
        GENERICATTRIBUTE3,
        GENERICATTRIBUTE4,
        GENERICATTRIBUTE5,
        GENERICATTRIBUTE6,
        GENERICATTRIBUTE7,
        GENERICATTRIBUTE8,
        GENERICATTRIBUTE9,
        GENERICATTRIBUTE10,
        GENERICATTRIBUTE11,
        GENERICATTRIBUTE12,
        GENERICATTRIBUTE13,
        GENERICATTRIBUTE14,
        GENERICATTRIBUTE15,
        GENERICATTRIBUTE16,
        GENERICNUMBER1,
        UNITTYPEFORGENERICNUMBER1,
        GENERICNUMBER2,
        UNITTYPEFORGENERICNUMBER2,
        GENERICNUMBER3,
        UNITTYPEFORGENERICNUMBER3,
        GENERICNUMBER4,
        UNITTYPEFORGENERICNUMBER4,
        GENERICNUMBER5,
        UNITTYPEFORGENERICNUMBER5,
        GENERICNUMBER6,
        UNITTYPEFORGENERICNUMBER6,
        GENERICDATE1,
        GENERICDATE2,
        GENERICDATE3,
        GENERICDATE4,
        GENERICDATE5,
        GENERICDATE6,
        GENERICBOOLEAN1,
        GENERICBOOLEAN2,
        GENERICBOOLEAN3,
        GENERICBOOLEAN4,
        GENERICBOOLEAN5,
        GENERICBOOLEAN6,
        PROCESSINGUNITSEQ,
        ISREPORTABLE,
        LOADDTTM)
        (select
        i.TENANTID,
        i.INCENTIVESEQ,
        i.NAME,
        i.PAYEESEQ,
        i.POSITIONSEQ,
        i.PERIODSEQ,
        i.PIPELINERUNSEQ,
        i.PIPELINERUNDATE,
        i.PLANSEQ,
        i.RULESEQ,
        i.VALUE,
        i.UNITTYPEFORVALUE,
        i.ISACTIVE,
        i.RELEASEDATE,
        i.QUOTA,
        i.UNITTYPEFORQUOTA,
        i.ATTAINMENT,
        i.UNITTYPEFORATTAINMENT,
        i.BUSINESSUNITMAP,
        i.GENERICATTRIBUTE1,
        i.GENERICATTRIBUTE2,
        i.GENERICATTRIBUTE3,
        i.GENERICATTRIBUTE4,
        i.GENERICATTRIBUTE5,
        i.GENERICATTRIBUTE6,
        i.GENERICATTRIBUTE7,
        i.GENERICATTRIBUTE8,
        i.GENERICATTRIBUTE9,
        i.GENERICATTRIBUTE10,
        i.GENERICATTRIBUTE11,
        i.GENERICATTRIBUTE12,
        i.GENERICATTRIBUTE13,
        i.GENERICATTRIBUTE14,
        i.GENERICATTRIBUTE15,
        i.GENERICATTRIBUTE16,
        i.GENERICNUMBER1,
        i.UNITTYPEFORGENERICNUMBER1,
        i.GENERICNUMBER2,
        i.UNITTYPEFORGENERICNUMBER2,
        i.GENERICNUMBER3,
        i.UNITTYPEFORGENERICNUMBER3,
        i.GENERICNUMBER4,
        i.UNITTYPEFORGENERICNUMBER4,
        i.GENERICNUMBER5,
        i.UNITTYPEFORGENERICNUMBER5,
        i.GENERICNUMBER6,
        i.UNITTYPEFORGENERICNUMBER6,
        i.GENERICDATE1,
        i.GENERICDATE2,
        i.GENERICDATE3,
        i.GENERICDATE4,
        i.GENERICDATE5,
        i.GENERICDATE6,
        i.GENERICBOOLEAN1,
        i.GENERICBOOLEAN2,
        i.GENERICBOOLEAN3,
        i.GENERICBOOLEAN4,
        i.GENERICBOOLEAN5,
        i.GENERICBOOLEAN6,
        i.PROCESSINGUNITSEQ,
        i.ISREPORTABLE,
        sysdate
    FROM cs_incentive i, cs_period per
    WHERE i.periodseq = per.periodseq
        AND i.processingunitseq = vProcessingUnitSeq
        --AND per.startdate >= vCurYrStartDate 
        --AND per.enddate <= vPeriodRow.enddate
        AND per.removedate = cendoftime  
        AND (per.periodseq = vPeriodSeq OR per.shortname IN ('Mar', 'Jun', 'Sep', 'Dec'))
        AND (i.pipelinerunseq IN (SELECT pr.pipelineRunSeq
            FROM cs_pipelinerun pr, cs_stagesummary ss, cs_stagetype st
            WHERE pr.pipelineRunSeq = ss.pipelineRunSeq
            AND ss.stageTypeSeq = st.stageTypeSeq
            AND st.name = 'Reward'
            AND pr.periodSeq = vperiodseq
            AND pr.processingunitseq = vprocessingunitseq
            AND ss.isactive = 1)
            ));
    COMMIT;
    prc_AnalyzeTable('rpt_base_incentive');

    prc_AnalyzeTable('rpt_base_measurement');
    /*INSERT INTO stelext.rpt_base_measurement
(SELECT m.*, SYSDATE*/
    --added by kyap, column values as there's an error on column mismatch
    INSERT INTO stelext.rpt_base_measurement(
        TENANTID,
        MEASUREMENTSEQ,
        NAME,
        PAYEESEQ,
        POSITIONSEQ,
        PERIODSEQ,
        PIPELINERUNSEQ,
        PIPELINERUNDATE,
        PLANSEQ,
        RULESEQ,
        VALUE,
        UNITTYPEFORVALUE,
        NUMBEROFCREDITS,
        BUSINESSUNITMAP,
        GENERICATTRIBUTE1,
        GENERICATTRIBUTE2,
        GENERICATTRIBUTE3,
        GENERICATTRIBUTE4,
        GENERICATTRIBUTE5,
        GENERICATTRIBUTE6,
        GENERICATTRIBUTE7,
        GENERICATTRIBUTE8,
        GENERICATTRIBUTE9,
        GENERICATTRIBUTE10,
        GENERICATTRIBUTE11,
        GENERICATTRIBUTE12,
        GENERICATTRIBUTE13,
        GENERICATTRIBUTE14,
        GENERICATTRIBUTE15,
        GENERICATTRIBUTE16,
        GENERICNUMBER1,
        UNITTYPEFORGENERICNUMBER1,
        GENERICNUMBER2,
        UNITTYPEFORGENERICNUMBER2,
        GENERICNUMBER3,
        UNITTYPEFORGENERICNUMBER3,
        GENERICNUMBER4,
        UNITTYPEFORGENERICNUMBER4,
        GENERICNUMBER5,
        UNITTYPEFORGENERICNUMBER5,
        GENERICNUMBER6,
        UNITTYPEFORGENERICNUMBER6,
        GENERICDATE1,
        GENERICDATE2,
        GENERICDATE3,
        GENERICDATE4,
        GENERICDATE5,
        GENERICDATE6,
        GENERICBOOLEAN1,
        GENERICBOOLEAN2,
        GENERICBOOLEAN3,
        GENERICBOOLEAN4,
        GENERICBOOLEAN5,
        GENERICBOOLEAN6,
        PROCESSINGUNITSEQ,
        UNITTYPEFORNUMBEROFCREDITS,
        LOADDTTM)
        (SELECT
        m.TENANTID,
        m.MEASUREMENTSEQ,
        m.NAME,
        m.PAYEESEQ,
        m.POSITIONSEQ,
        m.PERIODSEQ,
        m.PIPELINERUNSEQ,
        m.PIPELINERUNDATE,
        m.PLANSEQ,
        m.RULESEQ,
        m.VALUE,
        m.UNITTYPEFORVALUE,
        m.NUMBEROFCREDITS,
        m.BUSINESSUNITMAP,
        m.GENERICATTRIBUTE1,
        m.GENERICATTRIBUTE2,
        m.GENERICATTRIBUTE3,
        m.GENERICATTRIBUTE4,
        m.GENERICATTRIBUTE5,
        m.GENERICATTRIBUTE6,
        m.GENERICATTRIBUTE7,
        m.GENERICATTRIBUTE8,
        m.GENERICATTRIBUTE9,
        m.GENERICATTRIBUTE10,
        m.GENERICATTRIBUTE11,
        m.GENERICATTRIBUTE12,
        m.GENERICATTRIBUTE13,
        m.GENERICATTRIBUTE14,
        m.GENERICATTRIBUTE15,
        m.GENERICATTRIBUTE16,
        m.GENERICNUMBER1,
        m.UNITTYPEFORGENERICNUMBER1,
        m.GENERICNUMBER2,
        m.UNITTYPEFORGENERICNUMBER2,
        m.GENERICNUMBER3,
        m.UNITTYPEFORGENERICNUMBER3,
        m.GENERICNUMBER4,
        m.UNITTYPEFORGENERICNUMBER4,
        m.GENERICNUMBER5,
        m.UNITTYPEFORGENERICNUMBER5,
        m.GENERICNUMBER6,
        m.UNITTYPEFORGENERICNUMBER6,
        m.GENERICDATE1,
        m.GENERICDATE2,
        m.GENERICDATE3,
        m.GENERICDATE4,
        m.GENERICDATE5,
        m.GENERICDATE6,
        m.GENERICBOOLEAN1,
        m.GENERICBOOLEAN2,
        m.GENERICBOOLEAN3,
        m.GENERICBOOLEAN4,
        m.GENERICBOOLEAN5,
        m.GENERICBOOLEAN6,
        m.PROCESSINGUNITSEQ,
        m.UNITTYPEFORNUMBEROFCREDITS,
        sysdate
    FROM cs_measurement m, cs_period per
    WHERE m.periodseq = per.periodseq
        AND m.processingunitseq = vProcessingUnitSeq
        --AND per.startdate >= vCurYrStartDate 
        --AND per.enddate <= vPeriodRow.enddate
        AND per.removedate = cendoftime  
        AND (per.periodseq = vPeriodSeq OR per.shortname IN ('Mar', 'Jun', 'Sep', 'Dec'))
        AND m.pipelinerunseq IN (SELECT pr.pipelineRunSeq
            FROM cs_pipelinerun pr, cs_stagesummary ss, cs_stagetype st
            WHERE pr.pipelineRunSeq = ss.pipelineRunSeq
                AND ss.stageTypeSeq = st.stageTypeSeq
                AND st.name IN ('Allocate', 'Reward', 'CreateDefaultData')
                AND pr.periodSeq = vperiodseq
                AND pr.processingunitseq = vprocessingunitseq
                AND ss.isactive = 1));
    COMMIT;
    prc_AnalyzeTable('rpt_base_measurement');

    prc_AnalyzeTable('rpt_base_credit');
    /*INSERT INTO stelext.rpt_base_credit
(SELECT cr.*, crt.credittypeid, SYSDATE*/
    --added by kyap, column values as there's an error on column mismatch
    INSERT INTO stelext.rpt_base_credit(
        TENANTID,
        CREDITSEQ,
        PAYEESEQ,
        POSITIONSEQ,
        SALESORDERSEQ,
        SALESTRANSACTIONSEQ,
        PERIODSEQ,
        CREDITTYPESEQ,
        NAME,
        PIPELINERUNSEQ,
        ORIGINTYPEID,
        COMPENSATIONDATE,
        PIPELINERUNDATE,
        BUSINESSUNITMAP,
        PREADJUSTEDVALUE,
        UNITTYPEFORPREADJUSTEDVALUE,
        VALUE,
        UNITTYPEFORVALUE,
        RELEASEDATE,
        RULESEQ,
        ISHELD,
        ISROLLABLE,
        ROLLDATE,
        REASONSEQ,
        COMMENTS,
        GENERICATTRIBUTE1,
        GENERICATTRIBUTE2,
        GENERICATTRIBUTE3,
        GENERICATTRIBUTE4,
        GENERICATTRIBUTE5,
        GENERICATTRIBUTE6,
        GENERICATTRIBUTE7,
        GENERICATTRIBUTE8,
        GENERICATTRIBUTE9,
        GENERICATTRIBUTE10,
        GENERICATTRIBUTE11,
        GENERICATTRIBUTE12,
        GENERICATTRIBUTE13,
        GENERICATTRIBUTE14,
        GENERICATTRIBUTE15,
        GENERICATTRIBUTE16,
        GENERICNUMBER1,
        UNITTYPEFORGENERICNUMBER1,
        GENERICNUMBER2,
        UNITTYPEFORGENERICNUMBER2,
        GENERICNUMBER3,
        UNITTYPEFORGENERICNUMBER3,
        GENERICNUMBER4,
        UNITTYPEFORGENERICNUMBER4,
        GENERICNUMBER5,
        UNITTYPEFORGENERICNUMBER5,
        GENERICNUMBER6,
        UNITTYPEFORGENERICNUMBER6,
        GENERICDATE1,
        GENERICDATE2,
        GENERICDATE3,
        GENERICDATE4,
        GENERICDATE5,
        GENERICDATE6,
        GENERICBOOLEAN1,
        GENERICBOOLEAN2,
        GENERICBOOLEAN3,
        GENERICBOOLEAN4,
        GENERICBOOLEAN5,
        GENERICBOOLEAN6,
        PROCESSINGUNITSEQ,
        CREDITTYPEID,
        LOADDTTM)
        (select
        cr.TENANTID,
        cr.CREDITSEQ,
        cr.PAYEESEQ,
        cr.POSITIONSEQ,
        cr.SALESORDERSEQ,
        cr.SALESTRANSACTIONSEQ,
        cr.PERIODSEQ,
        cr.CREDITTYPESEQ,
        cr.NAME,
        cr.PIPELINERUNSEQ,
        cr.ORIGINTYPEID,
        cr.COMPENSATIONDATE,
        cr.PIPELINERUNDATE,
        cr.BUSINESSUNITMAP,
        cr.PREADJUSTEDVALUE,
        cr.UNITTYPEFORPREADJUSTEDVALUE,
        cr.VALUE,
        cr.UNITTYPEFORVALUE,
        cr.RELEASEDATE,
        cr.RULESEQ,
        cr.ISHELD,
        cr.ISROLLABLE,
        cr.ROLLDATE,
        cr.REASONSEQ,
        cr.COMMENTS,
        cr.GENERICATTRIBUTE1,
        cr.GENERICATTRIBUTE2,
        cr.GENERICATTRIBUTE3,
        cr.GENERICATTRIBUTE4,
        cr.GENERICATTRIBUTE5,
        cr.GENERICATTRIBUTE6,
        cr.GENERICATTRIBUTE7,
        cr.GENERICATTRIBUTE8,
        cr.GENERICATTRIBUTE9,
        cr.GENERICATTRIBUTE10,
        cr.GENERICATTRIBUTE11,
        cr.GENERICATTRIBUTE12,
        cr.GENERICATTRIBUTE13,
        cr.GENERICATTRIBUTE14,
        cr.GENERICATTRIBUTE15,
        cr.GENERICATTRIBUTE16,
        cr.GENERICNUMBER1,
        cr.UNITTYPEFORGENERICNUMBER1,
        cr.GENERICNUMBER2,
        cr.UNITTYPEFORGENERICNUMBER2,
        cr.GENERICNUMBER3,
        cr.UNITTYPEFORGENERICNUMBER3,
        cr.GENERICNUMBER4,
        cr.UNITTYPEFORGENERICNUMBER4,
        cr.GENERICNUMBER5,
        cr.UNITTYPEFORGENERICNUMBER5,
        cr.GENERICNUMBER6,
        cr.UNITTYPEFORGENERICNUMBER6,
        cr.GENERICDATE1,
        cr.GENERICDATE2,
        cr.GENERICDATE3,
        cr.GENERICDATE4,
        cr.GENERICDATE5,
        cr.GENERICDATE6,
        cr.GENERICBOOLEAN1,
        cr.GENERICBOOLEAN2,
        cr.GENERICBOOLEAN3,
        cr.GENERICBOOLEAN4,
        cr.GENERICBOOLEAN5,
        cr.GENERICBOOLEAN6,
        cr.PROCESSINGUNITSEQ,
        crt.CREDITTYPEID,
        SYSDATE
    FROM cs_credit cr, cs_credittype crt
    WHERE ((cr.periodseq = vPeriodSeq
        AND cr.processingunitseq = vProcessingUnitSeq
        AND cr.credittypeseq = crt.datatypeseq
        AND crt.removedate = cendoftime
        AND cr.isheld = 0
        AND cr.pipelinerunseq IN (SELECT pr.pipelineRunSeq
            FROM cs_pipelinerun pr, cs_stagesummary ss, cs_stagetype st
            WHERE pr.pipelineRunSeq = ss.pipelineRunSeq
                AND ss.stageTypeSeq = st.stageTypeSeq
                AND st.name = 'Allocate'
                AND pr.periodSeq = vperiodseq
                AND pr.processingunitseq = vprocessingunitseq
                AND ss.isactive = 1
                )) or (cr.releasedate = (vPeriodRow.enddate-1)))); 
                /* or (cr.releasedate = (vPeriodRow.enddate-1) Condition 
  Add on for the issue Add on up front issues because compensation date between salestransaction and credit are not matching
  However, some of credits are held so that add the condition
*/  

    COMMIT;
    prc_AnalyzeTable('rpt_base_credit');

END;

PROCEDURE prc_buildreportingtables(vPeriodSeq IN CS_PERIODCALENDAR.periodseq%TYPE,vProcessingUnitSeq IN CS_PROCESSINGUNIT.processingunitseq%TYPE,vCalendarSeq IN CS_PERIODCALENDAR.calendarseq%TYPE,preportgroup IN VARCHAR2 DEFAULT NULL) IS
/****************************************************************************************************************
    The purpose of this procedure is to populate reporting custom tables.
    Date        Author        Description
    ------------------------------------------------------------------------------------------------------------
--- 30 Nov 2017 Tharanikumar  Initial release
****************************************************************************************************************/

  vProcessingUnitSeqext CS_PROCESSINGUNIT.processingunitseq%TYPE;
  v_procname varchar2(255):='prc_buildreportingtables';
  vpipelinerundate       cs_credit.pipelinerundate%TYPE;
  vrunmode               cs_pipelinerun.runmode%TYPE;  
  vpipelinerunseq        cs_pipelinerun.pipelinerunseq%TYPE;
  v_reportlist           VARCHAR2(30000); 




  cursor c_positiongrouplist is
    Select distinct pgp.name positiongroupname
          From              
               CS_PipelineRun_Positions PlPos, 
               CS_Position Pos,
               CS_PositionGroup Pgp,
               CS_Period Per
         Where PlPos.PipelineRunSeq  = vpipelinerunseq                          
           And PlPos.PositionSeq     = Pos.RuleElementOwnerSeq          
           And Pos.RemoveDate        = PKG_REPORTING_EXTRACT_R2.cEndofTime
           --And Pos.PositionGroupSeq  = Pgp.PositionGroupSeq   --sudhir
           And Pgp.RemoveDate        = PKG_REPORTING_EXTRACT_R2.cEndofTime
           And per.periodseq = vPeriodSeq
           And per.calendarseq = vCalendarSeq
           And per.removedate = PKG_REPORTING_EXTRACT_R2.cEndofTime
           And pos.effectivestartdate < Per.enddate
           And pos.effectiveenddate >= Per.startdate         
           And Pos.createdate <= vpipelinerundate
           And Pos.removedate > vpipelinerundate
           And Pos.effectivestartdate =
                   (SELECT /*+ index(pos cs_position_IND1) */
                           MAX (p.effectivestartdate)
                      FROM cs_position p, CS_Period per
                     WHERE p.ruleelementownerseq = pos.ruleelementownerseq
                       And per.periodseq = vPeriodSeq
                       And per.calendarseq = vCalendarSeq
                       And per.removedate = PKG_REPORTING_EXTRACT_R2.cEndofTime 
                       AND vpipelinerundate >= p.createdate
                       AND vpipelinerundate < p.removedate
                       AND p.effectivestartdate < per.enddate
                       AND p.effectiveenddate > per.startdate
                       AND vprocessingunitseq = pos.processingunitseq);

BEGIN

   prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Params : Period:'||vPeriodSeq||',Processing: ' ||vProcessingUnitSeq||', CalendarSeq :' ||vCalendarSeq||', Report group : ' || preportgroup,sqlerrm); 

    SELECT   TO_DATE (TO_CHAR (starttime, 'YYYY-MON-DD HH24:MI:SS'), 
                   'YYYY-MON-DD HH24:MI:SS'
                  ) AS pipelinerundate, runmode, pipelinerunseq
        INTO vpipelinerundate, vrunmode, vpipelinerunseq 
        FROM   tcmp.cs_pipelinerun
       WHERE   periodseq = vperiodseq
               AND processingunitseq =vprocessingunitseq
               AND pipelinerunseq = (SELECT   MAX (pipelinerunseq)
                                     FROM   tcmp.cs_pipelinerun
                                     WHERE   periodseq = vperiodseq
                                     and processingunitseq = vprocessingunitseq
                                     );
              --to be -- 
        prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Params : vpipelinerundate:'||vpipelinerundate||',vrunmode: ' ||vrunmode||', vpipelinerunseq :' ||vpipelinerunseq,sqlerrm); 

        --prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Started Full Mode',sqlerrm); 

    BEGIN   
        prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Enter Build Reporting Table BEGIN',sqlerrm); 

         select 
          RTRIM (REGEXP_SUBSTR (runparameters, '\[odsReportList\]([^\[]+)' , 1, 1, 'i', 1), ',' )  ||',' 
          into v_reportlist
        from cs_pipelinerun 
          where command ='PipelineRun' and description like '%ODS%'
			and state<>'Pending' --Added by Gopi 
            and state<>'Done' 
            and periodseq=vperiodseq
            and processingunitseq = vprocessingunitseq;
            exception when others then 
            v_reportlist := preportgroup;
            END;


           prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'v_reportlist :' ||v_reportlist,sqlerrm);   

   BEGIN
            execute immediate 'truncate table stel_classifier_Tab';
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Executing truncate table stel_classifier_Tab',sqlerrm);  
            insert into stel_classifier_Tab
            select * from stel_classifier;
            commit;
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Inserting into stel_classifier.',sqlerrm);  
            END;
            --to be added
             prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'v_reportlist :' ||v_reportlist,sqlerrm);  



        -- BSC Procedures Started 
 if v_reportlist like '%_BSC_%' THEN

          prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'START-BSC Report Group',sqlerrm);

          prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Start-PRC_BSC_PAY_MTHQTR_ACHV -'||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  

          PRC_BSC_PAY_MTHQTR_ACHV('BSCPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);  

          prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'End-PRC_BSC_PAY_MTHQTR_ACHV',sqlerrm); 


          prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Start-PRC_BSC_ADV_COMM_PAY'||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  

          PRC_BSC_ADV_COMM_PAY('BSCADVCOMMPAY',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);  

          prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'End-PRC_BSC_ADV_COMM_PAY',sqlerrm);  
        --to be added
          prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'vPeriodRow.Shortname : '||vPeriodRow.shortname,sqlerrm);  


          if vPeriodRow.shortname IN ('Mar', 'Jun', 'Sep', 'Dec') Then -- Quaterly Report Generation only 

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'START - PRC_BSC_QTR_PAY_SUM -QTR- Started'||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  

            PRC_BSC_QTR_PAY_SUM('BSCQTRPAYSUM',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq); 

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'END - PRC_BSC_QTR_PAY_SUM -QTR- Ended',sqlerrm);

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'START - PRC_BSC_QTR_HIGHLIGHT -QTR- started'||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);

            PRC_BSC_QTR_HIGHLIGHT('PAYEEQTRPAYSUMMARY',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);  -- It should be always after PRC_BSC_PAY_MTHQTR_ACHV

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'END - PRC_BSC_QTR_HIGHLIGHT -QTR- Ended',sqlerrm); 

          End if;

    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'End-BSC Report Group',sqlerrm);

 End If;

        --CCO Procedures
        if v_reportlist like '%_CCO_%' THEN 
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Start-CCO Report Group',sqlerrm);

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Started - PRC_CCO_MOBILE_PAYOUTSUMM - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
          PRC_CCO_MOBILE_PAYOUTSUMM('CCOMOBILEPAYOUTSUMM',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Ended - PRC_CCO_MOBILE_PAYOUTSUMM - ',sqlerrm);

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Started - PRC_CCO_MOBILE_RAWDATA - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
          PRC_CCO_MOBILE_RAWDATA(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Ended - PRC_CCO_MOBILE_RAWDATA - ',sqlerrm);

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Started - PRC_CCO_SINGTELTV_DETAILAI - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
          PRC_CCO_SINGTELTV_DETAILAI(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Ended - PRC_CCO_SINGTELTV_DETAILAI - ',sqlerrm);

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Started - PRC_CCO_TV_PAYOUTSUMMARY - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
          PRC_CCO_TV_PAYOUTSUMMARY('CCOTVPAYMENTSUMMARY',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Ended - PRC_CCO_TV_PAYOUTSUMMARY - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'END-CCO Report Group',sqlerrm);
        End if;  

        --STS procedures 
        if v_reportlist like '%_STS_%' THEN
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Start-STS Report Group',sqlerrm);

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Started - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
          PRC_STS_RC_DS_PAYEE_INDIVIDUAL('STSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'MONTHLY');
           prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Ended - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - ',sqlerrm);

           prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'vPeriodRow.shortname : '||vPeriodRow.shortname,sqlerrm);  
         if vPeriodRow.shortname IN ('Mar', 'Jun', 'Sep', 'Dec') Then -- Quaterly Report Generation only 
             prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Started - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - Qrt -  '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
         PRC_STS_RC_DS_PAYEE_INDIVIDUAL('STSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'QUARTERLY');
             prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Ended - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - Qrt -  ',sqlerrm);
         End if;

             prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'STARTED - PRC_STSRCSDS_PAYEE_SUMMARY - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
          PRC_STSRCSDS_PAYEE_SUMMARY('STSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq); -- It should be always after PRC_STS_PAYEE_INDIVIDUAL
             prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'ENDED - PRC_STSRCSDS_PAYEE_SUMMARY - ',sqlerrm);

             prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Started - PRC_STS_ROADSHOW -  '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
          PRC_STS_ROADSHOW('STSROADSHOW',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq); -- It should be always after PRC_STS_PAYEE_INDIVIDUAL
             prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Ended - PRC_STS_ROADSHOW -  ',sqlerrm);

             prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Started - PRC_STS_COMM_HIGHLIGHT -  '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
          PRC_STS_COMM_HIGHLIGHT(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq); -- It should be always after PRC_STS_PAYEE_INDIVIDUAL
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Ended - PRC_STS_COMM_HIGHLIGHT -  ',sqlerrm);

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'END-STS Report Group',sqlerrm);
        End if;  

        --RCS Procedures 
        if v_reportlist like '%_RCS_%' THEN
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'START-RCS report Group',sqlerrm);

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Started - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
          PRC_STS_RC_DS_PAYEE_INDIVIDUAL('RCSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'MONTHLY');  
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Ended - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - ',sqlerrm); 

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'vPeriodRow.shortname: '||vPeriodRow.shortname,sqlerrm); 
          if vPeriodRow.shortname IN ('Mar', 'Jun', 'Sep', 'Dec') Then -- Quaterly Report Generation only 
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'STARTED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL -QTR - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
         PRC_STS_RC_DS_PAYEE_INDIVIDUAL('RCSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'QUARTERLY');
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'ENDED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL -QTR - ',sqlerrm); 
         End if;

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'STARTED - PRC_STSRCSDS_PAYEE_SUMMARY - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
          PRC_STSRCSDS_PAYEE_SUMMARY('RCSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq); 
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'ENDED - PRC_STSRCSDS_PAYEE_SUMMARY - ',sqlerrm); 

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'STARTED - PRC_RCS_MICHAEL_INDIVIDUAL - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
          PRC_RCS_MICHAEL_INDIVIDUAL('MRCSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'MONTHLY');
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'ENDED - PRC_RCS_MICHAEL_INDIVIDUAL - ',sqlerrm); 

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'End-RCS report group.',sqlerrm);
        End if;   

        --Digital Sales Procedures
        if v_reportlist like '%_DS_%' THEN
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'START-DS report group.',sqlerrm);

              prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'STARTED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
            PRC_STS_RC_DS_PAYEE_INDIVIDUAL('DSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'MONTHLY'); 
              prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'ENDED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL - ',sqlerrm);

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'vPeriodRow.shortname : '||vPeriodRow.shortname,sqlerrm);
           if vPeriodRow.shortname IN ('Mar', 'Jun', 'Sep', 'Dec') Then -- Quaterly Report Generation only 
              prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'STARTED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL -QTR- '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
            PRC_STS_RC_DS_PAYEE_INDIVIDUAL('DSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'QUARTERLY');
              prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'ENDED - PRC_STS_RC_DS_PAYEE_INDIVIDUAL -QTR- ',sqlerrm);
         End if;

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'STARTED - PRC_STSRCSDS_PAYEE_SUMMARY - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
          PRC_STSRCSDS_PAYEE_SUMMARY('DSPAYEEACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq); 
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'ENDED - PRC_STSRCSDS_PAYEE_SUMMARY - ',sqlerrm);

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'END -DS report group.',sqlerrm);
        End if;   
        /*
--TEPL Report only for Admin Group not for User Group
if v_reportlist like '%_TEPL_%' THEN
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'ENTER _TEPL_ report group.',sqlerrm);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_AI_SUMMARY - STARTED',sqlerrm);
  PRC_TEPL_AI_SUMMARY('TEPLAISUMMARY',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_AI_SUMMARY - ENDED',sqlerrm);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_MSF_SUMMONTHLY - STARTED',sqlerrm);
  PRC_TEPL_MSF_SUMMONTHLY(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_MSF_SUMMONTHLY - ENDED',sqlerrm);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_AI_MSF_DETAIL - STARTED',sqlerrm);
  PRC_TEPL_AI_MSF_DETAIL('TEPLAIMSFDETAIL',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq); -- It should be always after PRC_TEPL_AI_SUMMARY
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_AI_MSF_DETAIL - ENDED',sqlerrm);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_MSF_SUMMDETAIL - STARTED',sqlerrm);
  PRC_TEPL_MSF_SUMMDETAIL(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);        
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_MSF_SUMMDETAIL - ENDED',sqlerrm);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_MOBILE_MBB_SPICE - STARTED',sqlerrm);
  PRC_TEPL_MOBILE_MBB_SPICE(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_MOBILE_MBB_SPICE - ENDED',sqlerrm);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_ACT_ACR - STARTED',sqlerrm);
  PRC_TEPL_ACT_ACR('TEPLACTUALACCRUAL',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);   
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_ACT_ACR - ENDED',sqlerrm);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_VOUCHER_COVERNOTE - STARTED',sqlerrm);
  PRC_TEPL_VOUCHER_COVERNOTE(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_VOUCHER_COVERNOTE - ENDED',sqlerrm);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_CONSUMER_COVERNOTEYKM - STARTED',sqlerrm);
  PRC_TEPL_CONSUMER_COVERNOTEYKM('TEPLCVRNOTEKYM',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_CONSUMER_COVERNOTEYKM - ENDED',sqlerrm);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_CONSUMER_REQMEMO - STARTED',sqlerrm);
  PRC_TEPL_CONSUMER_REQMEMO('TEPLCONSREQMEMO',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Procedure PRC_TEPL_CONSUMER_REQMEMO - ENDED',sqlerrm);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Exit _TEPL_ report group.',sqlerrm);
End if;
*/

        --CSTI
        if v_reportlist like '%_CSTI_%' THEN
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'START - CSTI report group.',sqlerrm);

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'STARTED PRC_CSTI_TRANS_DETAIL - ',sqlerrm);
          PRC_CSTI_TRANS_DETAIL(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'ENDED PRC_CSTI_TRANS_DETAIL ',sqlerrm);

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'END - CSTI report group.',sqlerrm);
        End if;

        --COMMON
        if v_reportlist like '%_Comm_%' THEN
             prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'START - COMM report group',sqlerrm);

             prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'STARTED PRC_COMM_PAYOUT_MONTHLY - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
           PRC_COMM_PAYOUT_MONTHLY(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
             prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'ENDED PRC_COMM_PAYOUT_MONTHLY - ',sqlerrm);

             prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'STARTED PRC_COMM_SAA_SUMMARY - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
           PRC_COMM_SAA_SUMMARY('SAASUMMARY',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'MONTHLY');
             prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'ENDED PRC_COMM_SAA_SUMMARY - ',sqlerrm);

              prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'vPeriodRow.shortname : '||vPeriodRow.shortname,sqlerrm);
           if vPeriodRow.shortname IN ('Mar', 'Jun', 'Sep', 'Dec') Then -- Quaterly Report Generation only 
              prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'STARTED PRC_COMM_SAA_SUMMARY - QTR - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
           PRC_COMM_SAA_SUMMARY('SAASUMMARY',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'QUARTERLY');
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'ENDED PRC_COMM_SAA_SUMMARY - QTR - ',sqlerrm);
          End if;

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'END - COMM report group',sqlerrm);
        End if;

        -- Direct Sales
      if v_reportlist like '%_DirectSales_%' THEN
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'START DirectSales report group.',sqlerrm);

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'STARTED PRC_DIRECTSALES_INDIVIDUAL - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
        PRC_DIRECTSALES_INDIVIDUAL('DIRECTSALESACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'MONTHLY');
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'ENDED PRC_DIRECTSALES_INDIVIDUAL - ',sqlerrm);

         if vPeriodRow.shortname IN ('Mar', 'Jun', 'Sep', 'Dec') Then -- Quaterly Report Generation only 
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'STARTED PRC_DIRECTSALES_INDIVIDUAL -QTR- '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
         PRC_DIRECTSALES_INDIVIDUAL('DIRECTSALESACHIVEMENT',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'QUARTERLY');
            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'ENDED PRC_DIRECTSALES_INDIVIDUAL -QTR- ',sqlerrm);
         End if;

            prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'END DirectSales report group.',sqlerrm);
       End if;


        -- Internal Prepaid
      /* Commenting IP for implementing the quarterly code change for director
  if v_reportlist like '%_IP_Individual Payment Summary%' THEN
  PRC_INTPREPAID_INDIVIDUAL('INTMGR',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
  PRC_INTPREPAIDSNR_INDIVIDUAL('INTSNRBIZDEMGR',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
  End if;
  */
        -- Internal Prepaid
        if v_reportlist like '%_IP_Individual Payment Summary%' THEN
          prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'START - IP_Individual Report Group.',sqlerrm);

          prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'STARTED - PRC_INTPREPAID_INDIVIDUAL - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
        PRC_INTPREPAID_INDIVIDUAL('INTMGR',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'MONTHLY');
          prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'ENDED - PRC_INTPREPAID_INDIVIDUAL - ',sqlerrm);

          prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'STARTED - PRC_INTPREPAIDSNR_INDIVIDUAL - '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
        PRC_INTPREPAIDSNR_INDIVIDUAL('INTSNRBIZDEMGR',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'MONTHLY');
          prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'ENDED - PRC_INTPREPAIDSNR_INDIVIDUAL - ',sqlerrm);

          prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'vPeriodRow.shortname : '||vPeriodRow.shortname,sqlerrm);
         if vPeriodRow.shortname IN ('Mar', 'Jun', 'Sep', 'Dec') Then -- Quaterly Report Generation only 
          prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'STARTED - PRC_INTPREPAIDSNR_INDIVIDUAL -QTR- '||'Parms:vPeriodSeq :' ||vPeriodSeq||',vProcessingUnitSeq:' || vProcessingUnitSeq||',vCalendarSeq :' || vCalendarSeq,sqlerrm);  
         PRC_INTPREPAID_INDIVIDUAL('INTMGR',vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,'QUARTERLY');
          prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'ENDED - PRC_INTPREPAIDSNR_INDIVIDUAL -QTR- ',sqlerrm);

         End if;
         prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'END IP_Individual Report Group.',sqlerrm);
       End if;

    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'Ended Full Mode',sqlerrm); 
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,v_procname,vCalendarRow.name,'END Build Reporting Table',sqlerrm); 
END;

PROCEDURE prc_driver(vPeriodSeq IN CS_PERIODCALENDAR.periodseq%TYPE,vProcessingUnitSeq IN CS_PROCESSINGUNIT.processingunitseq%TYPE,vCalendarSeq IN CS_PERIODCALENDAR.calendarseq%TYPE,preportgroup IN VARCHAR2 DEFAULT NULL) IS
/****************************************************************************************************************
    The purpose of this procedure is to create one entry point for
    running more than one report extract in the ODSReportsGenerationConfig.xml.

    Date        Author     Description
    ------------------------------------------------------------------------------------------------------------
--- 30 Nov 2017 Tharanikumar  Initial release
****************************************************************************************************************/
BEGIN



   prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'PRC-DRIVER','START-PRC-DRIVER','vPeriodSeq:'||vPeriodSeq||',vProcessingUnitSeq: ' ||vProcessingUnitSeq||', vCalendarSeq :' ||vCalendarSeq||',preportgroup :' ||preportgroup,sqlerrm);  

   prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'PRC-DRIVER','START-prc_setVariables','Setting up variables...',NULL);
   prc_setVariables(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
   prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'PRC-DRIVER','END-prc_setVariables','Setting up variables...',NULL);


--IF odsutils.istorunodsprocs (vprocessingunitseq, vperiodseq)
--THEN
   prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'PRC-DRIVER','START-DELETE -rpt_base_salestransaction','',NULL);
   --Delete Base Tables here.

  -- prc_DeleteTable('rpt_base_salestransaction',vProcessingUnitSeq);

  --Since we're clearing the table, truncate is sufficient. There is only one PU being used.
  -- if this changes in the future, the parition name should have the PU in it, so the repsective parition can be truncated
  EXECUTE IMMEDIATE 'ALTER TABLE rpt_base_salestransaction TRUNCATE PARTITION P_STEL_00001' ;

    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'PRC-DRIVER','END-DELETE -rpt_base_salestransaction','',NULL);

      prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'PRC-DRIVER','START-DELETE -rpt_base_padimension','',NULL);
   prc_DeleteTable('rpt_base_padimension',vProcessingUnitSeq);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'PRC-DRIVER','END-DELETE -rpt_base_padimension','',NULL);


     prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'PRC-DRIVER','START-DELETE -rpt_base_pareporting','',NULL);
   prc_DeleteTable('rpt_base_pareporting',vProcessingUnitSeq);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'PRC-DRIVER','END-DELETE -rpt_base_pareporting','',NULL);

     prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'PRC-DRIVER','START-DELETE -rpt_base_credit','',NULL);
   prc_DeleteTable('rpt_base_credit',vProcessingUnitSeq);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'PRC-DRIVER','END-DELETE -rpt_base_credit','',NULL);

     prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'PRC-DRIVER','START-DELETE -rpt_base_measurement','',NULL);
   prc_DeleteTable('rpt_base_measurement',vProcessingUnitSeq);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'PRC-DRIVER','END-DELETE -rpt_base_measurement','',NULL);

     prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'PRC-DRIVER','START-DELETE -rpt_base_incentive','',NULL);
   prc_DeleteTable('rpt_base_incentive',vProcessingUnitSeq);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'PRC-DRIVER','END-DELETE -rpt_base_incentive','',NULL);

     prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'PRC-DRIVER','START-DELETE -rpt_base_deposit','',NULL);
   prc_DeleteTable('rpt_base_deposit',vProcessingUnitSeq);
    prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'PRC-DRIVER','END-DELETE -rpt_base_deposit','',NULL);
   --prc_AddTableSubpartition('STELEXT','CS_CREDIT','TCMP','STEL',vProcessingUnitSeq,vPeriodSeq,'rpt_base_salestransaction');


   prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_driver','START-prc_buildbasetables','Base Table Populations STARTED...',NULL);
   prc_buildbasetables(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq);
prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_driver','END-prc_buildbasetables','Base Table Populations COMPLETED...',NULL);
--END IF;

   prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_driver','START-prc_buildreportingtables','START Reporting Table Populations...',NULL);
   prc_buildreportingtables(vPeriodSeq,vProcessingUnitSeq,vCalendarSeq,preportgroup);
   prc_logevent(PKG_REPORTING_EXTRACT_R2.vPeriodRow.name,'prc_driver','END-prc_buildreportingtables','End Reporting table populations...',NULL);

END prc_driver;
END PKG_REPORTING_EXTRACT_R2;


