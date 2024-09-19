CREATE PROCEDURE dbmtk_init_session_global()
LANGUAGE SQLSCRIPT
AS
BEGIN
    DECLARE init_timestamp VARCHAR(50) := TO_VARCHAR(CURRENT_TIMESTAMP);
    /* do not execute further if package already initialized */
    IF SESSION_CONTEXT('DBMTK_INIT_SESSION_GLOBAL') IS NOT NULL
    THEN
        RETURN;
    END IF;

    /* mark package as initialized */
    SET SESSION 'DBMTK_INIT_SESSION_GLOBAL' = :init_timestamp;
    
    BEGIN
        /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
        DECLARE cdt_EndOfTime date = EndOfTime();  /* ORIGSQL: cdt_EndOfTime constant date := to_date('01/01/2200 00:00:00', 'MM/DD/YYYY HH24:MI:SS') ; */
        DECLARE Txt_User VARCHAR(20) = 'HookUser';  /* ORIGSQL: Txt_User Constant Varchar2(20) := 'HookUser' ; */

        DECLARE gv_error VARCHAR(1000) = NULL;  /* ORIGSQL: gv_error varchar2(1000); */
        DECLARE gv_prePeriodSeq1 BIGINT = 0;  /* ORIGSQL: gv_prePeriodSeq1 int := 0; */

        -- last period
        DECLARE gv_prePeriodSeq2 BIGINT = 0;  /* ORIGSQL: gv_prePeriodSeq2 int := 0; */

        -- last 2 period
        DECLARE Gv_Periodname VARCHAR(100) = NULL;  /* ORIGSQL: Gv_Periodname Varchar2(100); */
        DECLARE Gv_Processingunitseq BIGINT = 38280596832649218;  /* ORIGSQL: Gv_Processingunitseq Int := 38280596832649218; */
        DECLARE Gv_Periodseq BIGINT = 2533274790398900;  /* ORIGSQL: Gv_Periodseq Int := 2533274790398900; */

        --last period  November 2014
        DECLARE gv_calendarSeq BIGINT = 2251799813685250;  /* ORIGSQL: gv_calendarSeq int := 2251799813685250; */
        DECLARE gv_plStartTime TIMESTAMP = NULL;  /* ORIGSQL: gv_plStartTime timestamp; */
        DECLARE gv_isYearEnd BIGINT = 0;  /* ORIGSQL: gv_isYearEnd int := 0; */
        DECLARE gv_isMonthEnd BIGINT = 0;  /* ORIGSQL: gv_isMonthEnd int := 0; */
        DECLARE Gv_Pipelinerunseq BIGINT = 0;  /* ORIGSQL: Gv_Pipelinerunseq Int := 0; */
        DECLARE gv_CrossoverEffectiveDate TIMESTAMP = NULL;  /* ORIGSQL: gv_CrossoverEffectiveDate date; */

        --for revamp begin
        DECLARE Gv_Setnumbernador1 BIGINT = 6;  /* ORIGSQL: Gv_Setnumbernador1 Int := 6; */

        --for revamp end
        DECLARE Gv_Setnumbernador BIGINT = 5;  /* ORIGSQL: Gv_Setnumbernador Int := 5; */
        DECLARE Gv_Setnumberpi BIGINT = 3;  /* ORIGSQL: Gv_Setnumberpi Int := 3; */
        DECLARE gv_setnumberaor BIGINT = 4;  /* ORIGSQL: gv_setnumberaor Int := 4; */

        --for revamp begin
        DECLARE Gv_Setnumberoragy BIGINT = 101;  /* ORIGSQL: Gv_Setnumberoragy Int := 101; */
        DECLARE Gv_Setnumberordist BIGINT = 102;  /* ORIGSQL: Gv_Setnumberordist Int := 102; */
        DECLARE Gv_Setnumberoragyanddist BIGINT = 103;  /* ORIGSQL: Gv_Setnumberoragyanddist Int := 103; */

        --for revamp end
        DECLARE gv_hryc BIGINT = 16607023625930577;  /* ORIGSQL: gv_hryc int := 16607023625930577; */

        /* Next lines imported from package header (DBMTK_USER_NAME.PK_STAGE_HOOK.PACKAGE.plsql): */
        /* package/session variables start here: */

        /* End lines imported from package header */

        /* saving values of initialized package/session variables: */
        SET SESSION 'GV_ERROR' = :gv_error;
        SET SESSION 'GV_PREPERIODSEQ1' = CAST(:gv_prePeriodSeq1 AS VARCHAR(512));
        SET SESSION 'GV_PREPERIODSEQ2' = CAST(:gv_prePeriodSeq2 AS VARCHAR(512));
        SET SESSION 'GV_PERIODNAME' = :Gv_Periodname;
        SET SESSION 'GV_PROCESSINGUNITSEQ' = CAST(:Gv_Processingunitseq AS VARCHAR(512));
        SET SESSION 'GV_PERIODSEQ' = CAST(:Gv_Periodseq AS VARCHAR(512));
        SET SESSION 'GV_CALENDARSEQ' = CAST(:gv_calendarSeq AS VARCHAR(512));
        SET SESSION 'GV_PLSTARTTIME' = TO_VARCHAR(:gv_plStartTime, 'yyyy Mon dd hh24:mi:ss:ff3');
        SET SESSION 'GV_ISYEAREND' = CAST(:gv_isYearEnd AS VARCHAR(512));
        SET SESSION 'GV_ISMONTHEND' = CAST(:gv_isMonthEnd AS VARCHAR(512));
        SET SESSION 'GV_PIPELINERUNSEQ' = CAST(:Gv_Pipelinerunseq AS VARCHAR(512));
        SET SESSION 'GV_CROSSOVEREFFECTIVEDATE' = TO_VARCHAR(:gv_CrossoverEffectiveDate, 'yyyy Mon dd hh24:mi:ss:ff3');
        SET SESSION 'GV_SETNUMBERNADOR1' = CAST(:Gv_Setnumbernador1 AS VARCHAR(512));
        SET SESSION 'GV_SETNUMBERNADOR' = CAST(:Gv_Setnumbernador AS VARCHAR(512));
        SET SESSION 'GV_SETNUMBERPI' = CAST(:Gv_Setnumberpi AS VARCHAR(512));
        SET SESSION 'GV_SETNUMBERAOR' = CAST(:gv_setnumberaor AS VARCHAR(512));
        SET SESSION 'GV_SETNUMBERORAGY' = CAST(:Gv_Setnumberoragy AS VARCHAR(512));
        SET SESSION 'GV_SETNUMBERORDIST' = CAST(:Gv_Setnumberordist AS VARCHAR(512));
        SET SESSION 'GV_SETNUMBERORAGYANDDIST' = CAST(:Gv_Setnumberoragyanddist AS VARCHAR(512));
        SET SESSION 'GV_HRYC' = CAST(:gv_hryc AS VARCHAR(512));
        /* package/session variables end here */
    END;
END;