CREATE LIBRARY "EXT"."PK_STAGE_HOOK" LANGUAGE SQLSCRIPT AS
BEGIN
  PUBLIC FUNCTION cdt_EndOfTime
()
RETURNS result TIMESTAMP
LANGUAGE SQLSCRIPT
AS
BEGIN
    /* ORIGSQL: cdt_EndOfTime constant date := to_date('01/01/2200 00:00:00', 'MM/DD/YYYY HH24:MI:SS') ; */
     result = TO_DATE('01/01/2200 00:00:00',  'MM/DD/YYYY HH24:MI:SS');
END;
  PUBLIC FUNCTION Txt_User
()
RETURNS RESULT VARCHAR(20)
LANGUAGE SQLSCRIPT
AS
BEGIN
    /* ORIGSQL: Txt_User Constant Varchar2(20) := 'HookUser' ; */
    RESULT = 'HookUser';
END;
  PUBLIC PROCEDURE init_session_global
AS
BEGIN
    DECLARE init_timestamp VARCHAR(50) := TO_VARCHAR(CURRENT_TIMESTAMP);
    /* do not execute further if package already initialized */
    IF SESSION_CONTEXT('INIT_SESSION_GLOBAL') IS NOT NULL
    THEN
        RETURN;
    END IF;

    /* mark package as initialized */
    SET SESSION 'INIT_SESSION_GLOBAL' = :init_timestamp;
    
    BEGIN
        /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
        DECLARE cdt_EndOfTime CONSTANT TIMESTAMP = cdt_EndOfTime();  /* ORIGSQL: cdt_EndOfTime constant date := to_date('01/01/2200 00:00:00', 'MM/DD/YYYY HH24:MI:SS') ; */
        DECLARE Txt_User CONSTANT VARCHAR(20) = Txt_User();  /* ORIGSQL: Txt_User Constant Varchar2(20) := 'HookUser' ; */

        /*
        ************************************************
        Version     Create By       Create Date   Change
        ************************************************
        1           Callidus         20150407    Production version
        2           Endi             20160229    Added query hint to SP_TXA_PIAOR procedure - see Log('40')
        3           Win Tan          20160601    For Fair BSC project
        4           Win Tan          20170124    Revise the logic to process compensation portion in month-end only
        5           Jeff             20170217    PIAOR enhancement
        6           Win Tan          20180629    For BSC clawback recovery
        7           Sammi            20180709    Change Nador logic for Transfer FA agent
        8           Sammi            20181023    fix multiple position version
        9           Sammi            20181114    comment SP_TXA_PIAOR and SP_TXNTXA_YREND_PIAOR,since PIAOR has been rewrite
        10          Sammi            20181127    fix compile issue since cs_credit add a new column
        11          Jeff             201901      SPecial handling for PBU BUyout logic due to 27 Nov 2018 tripple commision load issue
        12          Endi             20190530    specify column for insert into PMCREDITTRACE in SP_CLAWBACK_CALCULATION due to 1905 release
        13          Sammi            20191001    Exclude FYC_Introducer and OFYC_Introducer for PI/AOR/NADOR Pre classify in Comcleanassignment
        14          Amanda           20190522    For BSC SPI/SPI_FA Clawback enhancement
        15          Yang             20220513    Harmonizationn Tier3 lumpsum
        16          Endi             20220917    Add month-end check for PBU Lumpsum
        17          Endi             20220926    Finetune PBU stagehook
        18          Duncan           20221031    don't delete 3rd party transactionassignment
        19          Endi             20230211    BSC CB special batch enhancement
        20          Michael Tang     20230217    Nador issue: Migrate-then-Promote
        21          Zero Wang        20230704    GST enhancement
        22          Zero Wang        20231128    For MAS Section86 project
        23          Wendy Wang       20231207    For MAS Section86 project, PBU Lumpsum
        24          Zero Wang        20240221    For MAS Section86 project Nador
        */

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

        /* Next lines imported from package header (EXT.PK_STAGE_HOOK.PACKAGE.plsql): */
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
  PUBLIC PROCEDURE Log
(
    IN inText VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                             /* ORIGSQL: inText varchar2 */
)

AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE vText VARCHAR(4000);  /* ORIGSQL: vText varchar2(4000); */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: when others then */
        BEGIN
            /* ORIGSQL: rollback; */
            ROLLBACK;

            /* ORIGSQL: raise; */
            RESIGNAL;
        END;



    /* ORIGSQL: pragma autonomous_transaction; */
    BEGIN AUTONOMOUS TRANSACTION
        vText = SUBSTRING(:inText,1,4000);  /* ORIGSQL: substr(inText, 1, 4000) */

        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_DEBUG_CUSTOM' not found */

        /* ORIGSQL: insert into CS_Debug_custom (text, value) values ('STAGEHOOK_' || vText, 1); */
        INSERT INTO CS_Debug_custom
            (
                text, value
            )
        VALUES (
                'STAGEHOOK_' || IFNULL(:vText,''),
                1
        );

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: dbms_output.put_line('STAGEHOOK_' || vText); */
        /* RESOLVE: Statement 'CALL sapdbmtk.sp_dbmtk_buffered_output_writeln' not currently supported in HANA AUTONOMOUS TRANSACTION */
        -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('STAGEHOOK_' || IFNULL(:vText,''));

        /* ORIGSQL: exception when others then */
    END;

    /*
    Modified at Aug 17 2014
    Desc: change tbl_agent_move to DM_tbl_agent_move
    */
END;
  PUBLIC PROCEDURE comDebugger
(
    IN i_objName VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                 /* ORIGSQL: i_objName IN varchar2 */
    IN i_objContent VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                   /* ORIGSQL: i_objContent IN varchar2 */
)

/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* ORIGSQL: pragma autonomous_transaction; */
    BEGIN AUTONOMOUS TRANSACTION
        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.SH_DEBUGGER' not found */

        /* ORIGSQL: insert into sh_debugger values (i_objName, sysdate, i_objContent); */
        INSERT INTO EXT.sh_debugger
        VALUES (
                :i_objName,
                CURRENT_TIMESTAMP,  /* ORIGSQL: sysdate */
                :i_objContent
        );

        /* ORIGSQL: commit; */
        COMMIT;
    END;

    /*
    Modified at Aug 17 2014
    Desc: change tbl_agent_role_move to DM_tbl_agent_role_move
    */
END;
  PUBLIC FUNCTION Comgeteventtypeseq
(
    IN I_Eventtypeid VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                    /* ORIGSQL: I_Eventtypeid IN Varchar2 */
)
RETURNS RESULT BIGINT   /* ORIGSQL: Return Int */
LANGUAGE SQLSCRIPT
AS
BEGIN
    DECLARE cdt_EndOfTime CONSTANT TIMESTAMP = cdt_EndOfTime();

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_eventtypeseq BIGINT;  /* ORIGSQL: v_eventtypeseq int; */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: when others then */
        BEGIN 
            RESULT = 0;
            /* sapdbmtk: Closing RETURN in exception handler commented out, not supported in HANA */
            --RETURN;
        END;



    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_EVENTTYPE' not found */

    SELECT
        datatypeseq
    INTO
        v_eventtypeseq
    FROM
        Cs_Eventtype
    WHERE
        Eventtypeid = :I_Eventtypeid
        AND removedate = :cdt_EndOfTime;

    RESULT = :v_eventtypeseq;


    /* ORIGSQL: exception when others then */
END;
  PUBLIC PROCEDURE Sp_Update_Txn
(
    IN In_Periodseq BIGINT     /* ORIGSQL: In_Periodseq IN Int */
)

/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN 
/* Start Deepan : Commenting below variables as they are not required in HANA */
    -- SEQUENTIAL EXECUTION
    -- DECLARE DBMTK_TMPVAR_INT_6 BIGINT; /*sapdbmtk-generated help variable*/
    -- DECLARE DBMTK_TMPVAR_INT_7 BIGINT; /*sapdbmtk-generated help variable*/
    -- DECLARE DBMTK_TMPVAR_STRING_1 VARCHAR(5000); /*sapdbmtk-generated help variable*/
    -- DECLARE DBMTK_TMPVAR_TIMESTAMP_1 TIMESTAMP; /*sapdbmtk-generated help variable*/
    -- DECLARE DBMTK_TMPVAR_TIMESTAMP_2 TIMESTAMP; /*sapdbmtk-generated help variable*/
    -- DECLARE DBMTK_TMPVAR_TIMESTAMP_3 TIMESTAMP; /*sapdbmtk-generated help variable*/
    -- DECLARE DBMTK_TMPVAR_TIMESTAMP_4 TIMESTAMP; /*sapdbmtk-generated help variable*/
    -- DECLARE DBMTK_TMPVAR_TIMESTAMP_5 TIMESTAMP; /*sapdbmtk-generated help variable*/
    -- DECLARE DBMTK_CTV_PROCID INT := EXT.ctv_procid(); /*sapdbmtk-generated help variable*/
  /* End Deepan */
    
    DECLARE cdt_EndOfTime CONSTANT TIMESTAMP = cdt_EndOfTime();
    DECLARE Gv_Processingunitseq BIGINT; /* package/session variable */

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    /* RESOLVE: Identifier not found: Table/Column 'CS_Message.messageSeq' not found (for %TYPE declaration) */

    DECLARE v_transferDate TIMESTAMP;  /* ORIGSQL: v_transferDate date; */
    DECLARE v_componentValue VARCHAR(30) = 'TXNUPD';  /* ORIGSQL: v_componentValue varchar2(30) := 'TXNUPD'; */
    DECLARE vstartdate TIMESTAMP;  /* ORIGSQL: vstartdate date; */
    DECLARE venddate TIMESTAMP;  /* ORIGSQL: venddate date; */
    DECLARE vparname VARCHAR(255);  /* ORIGSQL: vparname varchar2(255); */
    DECLARE v_ET1 DECIMAL(38,10);  /* ORIGSQL: v_ET1 number; */
    DECLARE v_ET2 DECIMAL(38,10);  /* ORIGSQL: v_ET2 number; */
    DECLARE v_ET3 DECIMAL(38,10);  /* ORIGSQL: v_ET3 number; */
    DECLARE v_ET4 DECIMAL(38,10);  /* ORIGSQL: v_ET4 number; */
    DECLARE v_ET5 DECIMAL(38,10);  /* ORIGSQL: v_ET5 number; */
    DECLARE v_ET6 DECIMAL(38,10);  /* ORIGSQL: v_ET6 number; */
    DECLARE v_ET7 DECIMAL(38,10);  /* ORIGSQL: v_ET7 number; */
    DECLARE v_ET8 DECIMAL(38,10);  /* ORIGSQL: v_ET8 number; */
    DECLARE v_ET9 DECIMAL(38,10);  /* ORIGSQL: v_ET9 number; */
    DECLARE vSQL VARCHAR(4000);  /* ORIGSQL: vSQL varchar2(4000); */
    DECLARE vDecodeSQL VARCHAR(1000);  /* ORIGSQL: vDecodeSQL varchar2(1000); */

    /* RESOLVE: Identifier not found: Table/Column 'cs_gasalestransaction.genericdate7' not found (for %TYPE declaration) */

    -- TYPE txnseq_t IS TABLE OF cs_salestransaction.salestransactionseq%TYPE;
    -- TYPE transferdt_t IS TABLE OF cs_gasalestransaction.genericdate2%TYPE;
    -- TYPE contractdt_t IS TABLE OF cs_gasalestransaction.genericdate1%TYPE;
    -- TYPE welcomepk_t IS TABLE OF cs_gasalestransaction.genericboolean1%TYPE;
    -- TYPE assigndt_t IS TABLE OF cs_gasalestransaction.genericdate3%TYPE;
    -- TYPE ldrsocdt_t IS TABLE OF cs_gasalestransaction.genericdate5%TYPE;
    -- TYPE starttxndt_t IS TABLE OF cs_gasalestransaction.genericdate7%TYPE;


    -- DECLARE l_txnseq EXT.PK_STAGE_HOOK.SP_UPDATE_TXN.txnseq_t;  /* ORIGSQL: l_txnseq txnseq_t; */
    -- DECLARE l_transferdt EXT.PK_STAGE_HOOK.SP_UPDATE_TXN.transferdt_t;  /* ORIGSQL: l_transferdt transferdt_t; */
    -- DECLARE l_contractdt EXT.PK_STAGE_HOOK.SP_UPDATE_TXN.contractdt_t;  /* ORIGSQL: l_contractdt contractdt_t; */
    -- DECLARE l_welcomepk EXT.PK_STAGE_HOOK.SP_UPDATE_TXN.welcomepk_t;  /* ORIGSQL: l_welcomepk welcomepk_t; */
    -- DECLARE l_assigndt EXT.PK_STAGE_HOOK.SP_UPDATE_TXN.assigndt_t;  /* ORIGSQL: l_assigndt assigndt_t; */
    -- DECLARE l_ldrsocdt EXT.PK_STAGE_HOOK.SP_UPDATE_TXN.ldrsocdt_t;  /* ORIGSQL: l_ldrsocdt ldrsocdt_t; */
    -- DECLARE l_starttxndt EXT.PK_STAGE_HOOK.SP_UPDATE_TXN.starttxndt_t;  /* ORIGSQL: l_starttxndt starttxndt_t; */


    DECLARE l_txnseq BIGINT;  /* ORIGSQL: l_txnseq txnseq_t; */
    DECLARE l_transferdt TIMESTAMP;  /* ORIGSQL: l_transferdt transferdt_t; */
    DECLARE l_contractdt TIMESTAMP;  /* ORIGSQL: l_contractdt contractdt_t; */
    DECLARE l_welcomepk BOOLEAN;  /* ORIGSQL: l_welcomepk welcomepk_t; */
    DECLARE l_assigndt TIMESTAMP;  /* ORIGSQL: l_assigndt assigndt_t; */
    DECLARE l_ldrsocdt TIMESTAMP;  /* ORIGSQL: l_ldrsocdt ldrsocdt_t; */
    DECLARE l_starttxndt TIMESTAMP;  /* ORIGSQL: l_starttxndt starttxndt_t; */
    DECLARE DEC_TXNADJSEQ  BIGINT;
    DECLARE DEC_TXNSEQ   BIGINT;
    DECLARE DEC_MessageLogSeq BIGINT;
    DECLARE DEC_MessageSeq BIGINT;

    DECLARE indx INT;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: When Others Then */
        BEGIN
            /* ORIGSQL: ROLLBACK; */
            ROLLBACK;

            --Log('Update failure with Error:'||sqlerrm);
            /* ORIGSQL: raise; */
            RESIGNAL;

            /* sapdbmtk: Closing RETURN in exception handler commented out, not supported in HANA */
            --RETURN;
        END;

        /* initialize session variables, if not yet done */
        CALL init_session_global();
        /* retrieve the package/session variables referenced in this procedure */
        SELECT CAST(SESSION_CONTEXT('GLOBVAR_USER_NAME_PK_STAGE_HOOK_GV_PROCESSINGUNITSEQ') AS BIGINT) INTO Gv_Processingunitseq FROM SYS.DUMMY ;
        /* end of package/session variables */


    -- :DEC_TXNADJSEQ  BIGINT;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table/Column 'CS_TransactionAdjustment.transactionadjustmentseq' not found (for %TYPE declaration) */
    -- :DEC_TXNSEQ   BIGINT;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table/Column 'CS_SalesTransaction.salestransactionseq' not found (for %TYPE declaration) */
    -- :DEC_MessageLogSeq BIGINT;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table/Column 'CS_Message.messageLogSeq' not found (for %TYPE declaration) */
    -- :DEC_MessageSeq BIGINT;/* NOT CONVERTED! */


    /* Mark -20150820 - start */

    /*--- TYPE definition moved to  scripts/dbmtk_create_types.sqlscript ---
    ----- Converted type 'txnseq_t' to 'DBMTK_USER_NAME.PK_STAGE_HOOK__SP_UPDATE_TXN__txnseq_t'
    TYPE txnseq_t IS TABLE OF cs_salestransaction.salestransactionseq%TYPE;
    ---end of TYPE definition commented out---*/
    /* RESOLVE: Identifier not found: Table/Column 'cs_salestransaction.salestransactionseq' not found (for %TYPE declaration) */

    /*--- TYPE definition moved to  scripts/dbmtk_create_types.sqlscript ---
    ----- Converted type 'transferdt_t' to 'DBMTK_USER_NAME.PK_STAGE_HOOK__SP_UPDATE_TXN__transferdt_t'
    TYPE transferdt_t IS TABLE OF cs_gasalestransaction.genericdate2%TYPE;
    ---end of TYPE definition commented out---*/
    /* RESOLVE: Identifier not found: Table/Column 'cs_gasalestransaction.genericdate2' not found (for %TYPE declaration) */

    /*--- TYPE definition moved to  scripts/dbmtk_create_types.sqlscript ---
    ----- Converted type 'contractdt_t' to 'DBMTK_USER_NAME.PK_STAGE_HOOK__SP_UPDATE_TXN__contractdt_t'
    TYPE contractdt_t IS TABLE OF cs_gasalestransaction.genericdate1%TYPE;
    ---end of TYPE definition commented out---*/
    /* RESOLVE: Identifier not found: Table/Column 'cs_gasalestransaction.genericdate1' not found (for %TYPE declaration) */

    /*--- TYPE definition moved to  scripts/dbmtk_create_types.sqlscript ---
    ----- Converted type 'welcomepk_t' to 'DBMTK_USER_NAME.PK_STAGE_HOOK__SP_UPDATE_TXN__welcomepk_t'
    TYPE welcomepk_t IS TABLE OF cs_gasalestransaction.genericboolean1%TYPE;
    ---end of TYPE definition commented out---*/
    /* RESOLVE: Identifier not found: Table/Column 'cs_gasalestransaction.genericboolean1' not found (for %TYPE declaration) */

    /*--- TYPE definition moved to  scripts/dbmtk_create_types.sqlscript ---
    ----- Converted type 'assigndt_t' to 'DBMTK_USER_NAME.PK_STAGE_HOOK__SP_UPDATE_TXN__assigndt_t'
    TYPE assigndt_t IS TABLE OF cs_gasalestransaction.genericdate3%TYPE;
    ---end of TYPE definition commented out---*/
    /* RESOLVE: Identifier not found: Table/Column 'cs_gasalestransaction.genericdate3' not found (for %TYPE declaration) */

    /*--- TYPE definition moved to  scripts/dbmtk_create_types.sqlscript ---
    ----- Converted type 'ldrsocdt_t' to 'DBMTK_USER_NAME.PK_STAGE_HOOK__SP_UPDATE_TXN__ldrsocdt_t'
    TYPE ldrsocdt_t IS TABLE OF cs_gasalestransaction.genericdate5%TYPE;
    ---end of TYPE definition commented out---*/
    /* RESOLVE: Identifier not found: Table/Column 'cs_gasalestransaction.genericdate5' not found (for %TYPE declaration) */

    /*--- TYPE definition moved to  scripts/dbmtk_create_types.sqlscript ---
    ----- Converted type 'starttxndt_t' to 'DBMTK_USER_NAME.PK_STAGE_HOOK__SP_UPDATE_TXN.starttxndt_t'
    TYPE starttxndt_t IS TABLE OF cs_gasalestransaction.genericdate7%TYPE;
    ---end of TYPE definition commented out---*/ 
    /* Mark -20150820 - end */

    /* ORIGSQL: comInitialPartition(v_componentValue, v_componentValue, in_periodSeq) */
    -- CALL ComInitialpartition(:v_componentValue, :v_componentValue, :In_Periodseq); Deepan : ComInitialpartition not required

    /* ORIGSQL: Log('Start 1') */
    CALL Log('Start 1');

    --Mark 20150820
    --Maintenance.Enablepdml;

    /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_PERIOD' not found */

    SELECT
        Startdate,
        enddate
    INTO
        vstartdate,
        venddate
    FROM
        cs_period per
    WHERE
        per.periodSeq = :In_Periodseq
        AND per.removedate = :cdt_EndOfTime;

    /* ORIGSQL: Log('Looking for partition  ' || gv_ProcessingUnitSeq || ' date ' || to_char(venddate, 'YYYYMMDD') || ' periodseq ' || in_periodSeq) */
    CALL Log('Looking for partition  '|| IFNULL(TO_VARCHAR(:Gv_Processingunitseq),'') || ' date '|| IFNULL(TO_VARCHAR(:venddate,'YYYYMMDD'),'') || ' periodseq '|| IFNULL(TO_VARCHAR(:In_Periodseq),'') 
        );  /* ORIGSQL: to_char(venddate, 'YYYYMMDD') */

    vparname = 'P_AIAS_00002_20161201';
    BEGIN 
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
            /* ORIGSQL: when others then */
            BEGIN
                /* ORIGSQL: RAISE_APPLICATION_ERROR(-20000,'No TX partition found, goodbye') */
                -- sapdbmtk: mapped error code -20000 => 10000: (ABS(-20000)%10000)+10000
                SIGNAL SQL_ERROR_CODE 10000 SET MESSAGE_TEXT = 'No TX partition found, goodbye';
            END;


        /* RESOLVE: Identifier not found: Table/view 'DBMTK_USER_NAME.CS_SALESTRANSACTION' not found */

    --     SELECT
    --         (
    --             SELECT   /* ORIGSQL: (SELECT subobject_name FROM all_objects WHERE data_object_id = dbms_rowid.rowid_object (CS_SALESTRANSACTION.CS_SALESTRANSACTION.ROWID)) */
    --                 subobject_name
    --             FROM
    --                 SYS.OBJECTS  /* RESOLVE: Catalog reference(partly converted): Oracle catalog 'ALL_OBJECTS': verify conversion */
    --                              /* ORIGSQL: all_objects (Oracle catalog) */
    --             WHERE
    --                 data_object_id = dbms_rowid.rowid_object (CS_SALESTRANSACTION."ROWID")  /* RESOLVE: Standard Package call(not converted): 'dbms_rowid.rowid_object' not supported, manual conversion required */
    --                                                                                         /* RESOLVE: Identifier renamed, reserved word in target DBMS: column 'ROWID' (=reserved word in HANA) renamed to '"ROWID"'; ensure all calls/references are renamed accordingly */
    --                                                                          /* ORIGSQL: CS_SALESTRANSACTION.ROWID */
    --         )
    --     INTO
    --         vparname
    --     FROM
    --         CS_SALESTRANSACTION
    --     WHERE
    --         tenantid = 'AIAS'
    --         AND processingUnitseq = :Gv_Processingunitseq
    --         AND compensationdate >= :vstartdate
    --         AND compensationdate < :venddate
    --     LIMIT 1;  /* ORIGSQL: rownum = 1 */

    --     /* ORIGSQL: exception when others then */
    -- END; -----Deepan : partitions not required for HANA

    /* ORIGSQL: Log('Found partition name  ' || NVL(vParName, 'NULL')) */
    CALL Log('Found partition name  '|| IFNULL(:vparname, 'NULL') 
        );  /* ORIGSQL: NVL(vParName, 'NULL') */

    v_ET1 = Comgeteventtypeseq('APF');  /* ORIGSQL: Comgeteventtypeseq('APF') */

    v_ET2 = Comgeteventtypeseq('APF Payable');  /* ORIGSQL: Comgeteventtypeseq('APF Payable') */

    v_ET3 = Comgeteventtypeseq('API');  /* ORIGSQL: Comgeteventtypeseq('API') */

    v_ET4 = Comgeteventtypeseq('FYC');  /* ORIGSQL: Comgeteventtypeseq('FYC') */

    v_ET5 = Comgeteventtypeseq('OFYC');  /* ORIGSQL: Comgeteventtypeseq('OFYC') */

    v_ET6 = Comgeteventtypeseq('ORYC');  /* ORIGSQL: Comgeteventtypeseq('ORYC') */

    v_ET7 = Comgeteventtypeseq('OSSCP');  /* ORIGSQL: Comgeteventtypeseq('OSSCP') */

    v_ET8 = Comgeteventtypeseq('SSCP');  /* ORIGSQL: Comgeteventtypeseq('SSCP') */

    v_ET9 = Comgeteventtypeseq('RYC');  /* ORIGSQL: Comgeteventtypeseq('RYC') */

    --Maintenance.Enablepdml;

    vDecodeSQL = 'CASE TXn.EVENTTYPESEQ ' ||
    ' WHEN ' || IFNULL(TO_VARCHAR(:v_ET1),'0') || ' THEN ''APF''' ||
    ' WHEN ' || IFNULL(TO_VARCHAR(:v_ET2),'0') || ' THEN ''APF Payable''' ||
    ' WHEN ' || IFNULL(TO_VARCHAR(:v_ET3),'0') || ' THEN ''API''' ||
    ' WHEN ' || IFNULL(TO_VARCHAR(:v_ET4),'0') || ' THEN ''FYC''' ||
    ' WHEN ' || IFNULL(TO_VARCHAR(:v_ET5),'0') || ' THEN ''OFYC''' ||
    ' WHEN ' || IFNULL(TO_VARCHAR(:v_ET6),'0') || ' THEN ''ORYC''' ||
    ' WHEN ' || IFNULL(TO_VARCHAR(:v_ET7),'0') || ' THEN ''OSSCP''' ||
    ' WHEN ' || IFNULL(TO_VARCHAR(:v_ET8),'0') || ' THEN ''SSCP''' ||
    ' WHEN ' || IFNULL(TO_VARCHAR(:v_ET9),'0') || ' THEN ''RYC''' ||
    ' ELSE ''Unknown'' END';


    /* ORIGSQL: Log('Found decode string  ' || vDecodeSQL) */
    CALL Log('Found decode string  '|| IFNULL(:vDecodeSQL,''));

    vSQL := 'INSERT INTO ext.Sh_Query_Result ';
	vSQL := vSQL || '(Component, periodseq, Genericsequence1, Genericnumber1, Genericnumber2, Genericattribute1, genericattribute2, Genericattribute3, Genericattribute4, Genericattribute5, genericboolean1, Genericboolean2, Genericdate1, genericdate2, Genericdate3, genericsequence2, Genericattribute6, Genericattribute7, Genericattribute8, Genericdate4, genericDate5) ';
	vSQL := vSQL || 'SELECT ' || TO_VARCHAR(:v_componentValue) || ',' || TO_VARCHAR(:In_Periodseq);
	vSQL := vSQL || ', txn.salestransactionseq, txn.linenumber, txn.sublinenumber, ' || :vDecodeSQL || ' as eventtypeid, ';
	vSQL := vSQL || 'txn.salesorderseq, txn.businessunitmap, txn.genericattribute14 as classCode, Pos.Genericattribute4, ';
	vSQL := vSQL || 'decode(nvl(pos.genericattribute4, ''#''), nvl(txn.genericattribute14, ''#''), 1, 0), ';
	vSQL := vSQL || 'pos.genericBoolean4 as welcomePackage, pa.hireDate as contractDate, pos.genericDate4 as assignmentDate, ';
	vSQL := vSQL || 'Gpa.Genericdate6 As Leadersocdate, pos.ruleElementOwnerSeq as AgtPosSeq, txn.genericAttribute12 as agentCode, ';
	vSQL := vSQL || 'Txn.Genericattribute13 As Agencycode, Txn.Businessunitmap, Gapos12.Genericdate19 As Starttransactiondate, ';
	vSQL := vSQL || 'txn.compensationdate as compdate FROM Cs_Salestransaction Txn ';
	vSQL := vSQL || 'INNER JOIN cs_transactionassignment asg ON txn.salestransactionseq=asg.salestransactionseq and asg.tenantid=''AIAS'' ';
	vSQL := vSQL || 'AND asg.positionname like ''%T%'' and nvl(txn.genericattribute12, ''#'') = substr(asg.positionname, 4, 8) ';
	vSQL := vSQL || 'INNER JOIN cs_position pos ON pos.removedate = :cdt_EndOfTime AND pos.name = asg.positionname ';
	vSQL := vSQL || 'AND Pos.Effectivestartdate < TO_DATE(' || TO_VARCHAR(:venddate, 'yyyymmdd') || ', ''yyyymmdd'') ';
	vSQL := vSQL || 'and pos.tenantid=''AIAS'' and pos.effectiveEnddate >= TO_DATE(' || TO_VARCHAR(:venddate, 'yyyymmdd') || ', ''yyyymmdd'') ';
	vSQL := vSQL || 'left join cs_participant pa on pos.payeeSeq=pa.payeeSeq ';
	vSQL := vSQL || 'and pa.tenantid=''AIAS'' and pa.effectiveStartDate < TO_DATE(' || TO_VARCHAR(:venddate, 'yyyymmdd') || ', ''yyyymmdd'') ';
	vSQL := vSQL || 'and pa.effectiveEndDate >= TO_DATE(' || TO_VARCHAR(:venddate, 'yyyymmdd') || ', ''yyyymmdd'') and pa.removeDate=:cdt_EndOfTime ';
	vSQL := vSQL || 'left join cs_gaparticipant gpa on pa.payeeSeq=gpa.payeeSeq ';
	vSQL := vSQL || 'and gpa.tenantid=''AIAS'' and gpa.effectiveStartDate < TO_DATE(' || TO_VARCHAR(:venddate, 'yyyymmdd') || ', ''yyyymmdd'') ';
	vSQL := vSQL || 'and gpa.effectiveEndDate >= TO_DATE(' || TO_VARCHAR(:venddate, 'yyyymmdd') || ', ''yyyymmdd'') And Gpa.Removedate=:Cdt_Endoftime ';
	vSQL := vSQL || 'And Gpa.Pagenumber=0 Inner Join Cs_Position Posga12 on ''SGT''||txn.Genericattribute12=Posga12.Name ';
	vSQL := vSQL || 'and Posga12.tenantid=''AIAS'' And Posga12.Removedate=:Cdt_Endoftime ';
	vSQL := vSQL || 'And Posga12.Effectivestartdate < TO_DATE(' || TO_VARCHAR(:venddate, 'yyyymmdd') || ', ''yyyymmdd'') ';
	vSQL := vSQL || 'and Posga12.tenantid=''AIAS'' And Posga12.Effectiveenddate  >= TO_DATE(' || TO_VARCHAR(:venddate, 'yyyymmdd') || ', ''yyyymmdd'') ';
	vSQL := vSQL || 'Inner Join Cs_Gaposition Gapos12 on Gapos12.Pagenumber=0 and Gapos12.tenantid=''AIAS'' ';
	vSQL := vSQL || 'And posga12.ruleElementOwnerSeq=Gapos12.ruleElementOwnerSeq And Gapos12.Removedate=:Cdt_Endoftime ';
	vSQL := vSQL || 'And Gapos12.Effectivestartdate < TO_DATE(' || TO_VARCHAR(:venddate, 'yyyymmdd') || ', ''yyyymmdd'') ';
	vSQL := vSQL || 'and Gapos12.effectiveEndDate >= TO_DATE(' || TO_VARCHAR(:venddate, 'yyyymmdd') || ', ''yyyymmdd'') ';
	vSQL := vSQL || 'WHERE txn.tenantid=''AIAS'' and TXN.EVENTTYPESEQ IN (' || TO_VARCHAR(:v_ET1) || ',' || TO_VARCHAR(:v_ET2) || ',' || TO_VARCHAR(:v_ET3) || ',' || TO_VARCHAR(:v_ET4) || ',' || TO_VARCHAR(:v_ET5) || ',' || TO_VARCHAR(:v_ET6) || ',' || TO_VARCHAR(:v_ET7) || ',' || TO_VARCHAR(:v_ET8) || ',' || TO_VARCHAR(:v_ET9) || ')';
    /* ORIGSQL: dbms_output.put_line('SQL is ' || vSQL); */
    -- CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('SQL is '|| IFNULL(:vSQL,''));

    /* ORIGSQL: Log('SQL is ' || vSQL) */
    CALL Log('SQL is '|| IFNULL(:vSQL,''));

    /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
    /* ORIGSQL: execute immediate vSQL using Cdt_Endoftime, Cdt_Endoftime, Cdt_Endoftime, Cdt_Endoftime, Cdt_Endoftime; */
    -- CALL sapdbmtk.sp_dbmtk_prepare_execute_sql(:vSQL, :DBMTK_TMPVAR_STRING_1);
    EXECUTE IMMEDIATE :vSQL USING :cdt_EndOfTime, :cdt_EndOfTime, :cdt_EndOfTime, :cdt_EndOfTime, :cdt_EndOfTime;

    /* ORIGSQL: Log('End 1') */
    CALL Log('End 1');

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: Log('Start 2') */
    CALL Log('Start 2');

    --Mark 20150820
    --  Merge  Into Cs_Salestransaction Txn 
    /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
    /* ORIGSQL: Merge Into Cs_Salestransaction Txn Using (SELECT R.genericsequence1 AS salestransactionseq, R.Genericattribute5 AS Classcode, R.Genericboolean1 AS Classcodeflag FROM Sh_Query_Result R Where Component (...) */
    MERGE INTO Cs_Salestransaction AS Txn
        /* RESOLVE: Identifier not found: Table/view 'EXT.SH_QUERY_RESULT' not found */
        USING
        (
            SELECT   /* ORIGSQL: (Select R.genericsequence1 as salestransactionseq, R.Genericattribute5 As Classcode, R.Genericboolean1 As Classcodeflag From Sh_Query_Result R Where Component = v_componentValue and periodseq = in_per(...) */
                R.genericsequence1 AS salestransactionseq,
                /* --R.Genericattribute4 As Classcode, */
                R.Genericattribute5 AS Classcode /* --update by pos.classcode */, R.Genericboolean1 AS Classcodeflag
            FROM
                Sh_Query_Result R
            WHERE
                Component = :v_componentValue
                AND periodseq = :In_Periodseq
                AND r.genericboolean1 = 0 --only the ps.classcode is differet from txn.classcode, then will be update
        ) AS T
        ON (t.salestransactionseq = txn.salestransactionseq
        AND txn.tenantid = 'AIAS')
    WHEN MATCHED THEN
        UPDATE SET Txn.Genericattribute14 = T.Classcode;

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: Log('End  2') */
    CALL Log('End  2');

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: Log('Start  3') */
    CALL Log('Start  3');


/* Deepan : Removed the entire logic for updating cs_gasalestransaction with this */
MERGE INTO cs_gasalestransaction tgt
USING (
    SELECT
    r.genericSequence1 AS salestransactionseq,
    p.ruleelementownerseq,
    COALESCE(gatxn.genericdate1, TO_DATE('01/01/2200', 'DD-MON-YYYY' )) AS TxnContractdate,
    r.genericdate1 AS Contractdate,
    COALESCE(gatxn.genericboolean1, 0) AS TxnWelcomepackage,
    r.genericboolean2 AS Welcomepackage,
    CASE
        WHEN p.genericattribute1 = r.genericattribute7 AND p.Transferdate IS NOT NULL
        THEN p.Transferdate
        ELSE m.oldTransferDate
    END AS TransferDate,
    COALESCE(gatxn.genericdate2, TO_DATE('01/01/2200', 'DD-MON-YYYY' )) AS TxnTransferdate,
    r.genericdate2 AS AssignmentDate,
    COALESCE(gatxn.genericdate3, TO_DATE('01/01/2200', 'DD-MON-YYYY' )) AS TxnAssignmentDate,
    r.genericdate3 AS Leadersocdate,
    COALESCE(gatxn.Genericdate5, TO_DATE('01/01/2200', 'DD-MON-YYYY' )) AS TxnLeadersocdate,
    r.genericdate4 AS StartTransactionDate,
    COALESCE(gatxn.genericdate7, TO_DATE('01/01/2200', 'DD-MON-YYYY' )) AS TxnStartTransactionDate
FROM
    Sh_Query_Result r
INNER JOIN (
    SELECT
        Ruleelementownerseq,
        MAX(Genericdate3) AS Transferdate,
        effectivestartdate,
        effectiveenddate,
        genericattribute1,
        tenantid,
        Removedate
    FROM
        Cs_Position
    WHERE
        tenantid = 'AIAS' AND Removedate = TO_DATE('01/01/2200', 'DD-MON-YYYY' )
    GROUP BY
        Ruleelementownerseq, effectivestartdate, effectiveenddate, genericattribute1, tenantid, Removedate
) AS p ON p.Ruleelementownerseq = r.genericSequence2
    AND p.Effectivestartdate <= r.Genericdate5
    AND p.effectiveenddate > r.GenericDate5
    AND p.tenantid = 'AIAS' 
    AND p.Removedate = TO_DATE('01/01/2200', 'DD-MON-YYYY' )
INNER JOIN
    Cs_gaSalestransaction gatxn ON gatxn.salestransactionseq = r.genericSequence1
    AND gatxn.pagenumber = 0 AND gatxn.tenantid = 'AIAS'
LEFT JOIN (
    SELECT
        Agent_code,
        New_Agency_code,
        MAX(Effective_Date) AS Oldtransferdate
    FROM
        dm_tbl_agent_move
    WHERE
        Move_Type IN ('31', '30') OR (Move_Type = '20' AND Agency_Code != New_Agency_Code)
    GROUP BY
        agent_code, new_agency_code
) AS m ON m.agent_code = r.genericAttribute6 AND m.new_agency_code = r.genericAttribute7
WHERE
    r.periodseq = 12345 AND r.component = 'adsads' AND gatxn.tenantid = 'AIAS'

) AS src
ON (tgt.salestransactionseq = src.salestransactionseq)
WHEN MATCHED THEN UPDATE SET
    tgt.genericdate1 = src.Contractdate,
    tgt.genericdate2 = src.TransferDate,
    tgt.genericboolean1 = src.Welcomepackage,
    tgt.genericdate3 = src.AssignmentDate,
    tgt.genericdate5 = src.Leadersocdate,
    tgt.genericdate7 = src.StartTransactionDate;
-- WHERE
--     src.TxnContractdate != src.Contractdate OR
--     src.TxnWelcomepackage != src.Welcomepackage OR
--     src.TxnTransferdate != src.TransferDate OR
--     src.TxnAssignmentDate != src.AssignmentDate OR
--     src.TxnLeadersocdate != src.Leadersocdate OR
--     src.TxnStartTransactionDate != src.StartTransactionDate;


    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: Log('End  3') */
    CALL Log('End  3');

    /* ORIGSQL: commit; */
    COMMIT;

    /* ORIGSQL: EXCEPTION When Others Then */
END;

END;
END