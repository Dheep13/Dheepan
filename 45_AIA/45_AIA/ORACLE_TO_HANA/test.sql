
CREATE OR REPLACE LIBRARY EXT.PK_PIAOR_CALCULATION
LANGUAGE SQLSCRIPT
DEFAULT SCHEMA EXT
AS
BEGIN
    /* ORIGSQL: CREATE OR REPLACE PACKAGE BODY EXT.PK_PIAOR_CALCULATION is cdt_EndOfTime constant date := to_date('01/01/2200 00:00:00','MM/DD/YYYY HH24:MI:SS'); */
    /*---TYPE definition moved to  scripts/dbmtk_create_types.sqlscript ---*/
    -- Type  R_Agydisttrxn Is Record(
    --     Salestransactionseq  Int,
    --     SALESORDERSEQ  INT,
    --     Wagency  Varchar2(30),
    --     wAgencyLeader  varchar2(30),
    --     Wagyldrtitle  Varchar2(30),
    --     LdrCurRole  Varchar2(30), /* -- add by Nelson */
    --     Wagyldrdistrict  Varchar2(30),
    --     CurDistrict  Varchar2(30),  /* -- add by Nelson */
    --     Policyissuedate  Date,
    --     Compensationdate  Date,
    --     Wagtclass  Varchar2(30),
    --     Commissionagy  Varchar2(30),
    --     Runningtype  Varchar(100),
    --     Eventtypeid  Varchar2(30),
    --     Productname  Varchar2(100),
    --     Businessunitmap  Varchar2(100),
    --     Orphanpolicy  Varchar2(30),
    --     Managerseq  Int, /* -- add for spin off */
    --     Agyspinoffindicator  Varchar2(30),
    --     Agyspinoffflag  Int,
    --     Versioningdate  Date,
    --     Periodseq  Int,
    --     Spinstartdate  Date,
    --     Spindaterange  Number,
    --     Txnclasscode  Varchar2(10),
    --     Spinenddate  Date,
    --     actualOrphanPolicy  varchar2(30),
    --     wAgyLdrCde  varchar2(30), /* -- add by Nelson */
    --     setup  varchar2(30), /* -- add by Nelson */
    --     txnCode  varchar2(30) /* -- add by Nelson */
    -- ) ;

    PUBLIC VARIABLE R_Agydisttrxn ROW (
        Salestransactionseq  Int,
        SALESORDERSEQ  INT,
        Wagency  Varchar2(30),
        wAgencyLeader  varchar2(30),
        Wagyldrtitle  Varchar2(30),
        LdrCurRole  Varchar2(30), /* -- add by Nelson */
        Wagyldrdistrict  Varchar2(30),
        CurDistrict  Varchar2(30),  /* -- add by Nelson */
        Policyissuedate  Date,
        Compensationdate  Date,
        Wagtclass  Varchar2(30),
        Commissionagy  Varchar2(30),
        Runningtype  Varchar(100),
        Eventtypeid  Varchar2(30),
        Productname  Varchar2(100),
        Businessunitmap  Varchar2(100),
        Orphanpolicy  Varchar2(30),
        Managerseq  Int, /* -- add for spin off */
        Agyspinoffindicator  Varchar2(30),
        Agyspinoffflag  Int,
        Versioningdate  Date,
        Periodseq  Int,
        Spinstartdate  Date,
        Spindaterange  Number,
        Txnclasscode  Varchar2(10),
        Spinenddate  Date,
        actualOrphanPolicy  varchar2(30),
        wAgyLdrCde  varchar2(30), /* -- add by Nelson */
        setup  varchar2(30), /* -- add by Nelson */
        txnCode  varchar2(30) /* -- add by Nelson */
    ) ;


    ---end of TYPE definition commented out---*/ 
    -- Author  : ASNPHY8
    -- Created : 9/4/2018 4:06:02 PM
    -- Purpose : PIAOR rewrite

    PUBLIC VARIABLE cdt_EndOfTime CONSTANT TIMESTAMP = to_timestamp('01/01/2200 00:00:00','MM/DD/YYYY HH24:MI:SS');  /* ORIGSQL: cdt_EndOfTime constant date := to_date('01/01/2200 00:00:00','MM/DD/YYYY HH24:MI:SS') ; */

    PUBLIC VARIABLE AOR_PIB_Rate DECIMAL(10,5);  /* ORIGSQL: AOR_PIB_Rate number(10,5) ; */
    PUBLIC VARIABLE AOR_RYC_Rate DECIMAL(10,5);  /* ORIGSQL: AOR_RYC_Rate number(10,5) ; */

    PUBLIC VARIABLE STR_PU CONSTANT VARCHAR(20) = 'AGY_PU';  /* ORIGSQL: STR_PU CONSTANT VARCHAR2(20) := 'AGY_PU' ; */
    PUBLIC VARIABLE STR_CALENDARNAME CONSTANT VARCHAR(50) = 'AIA Singapore Calendar';  /* ORIGSQL: STR_CALENDARNAME CONSTANT VARCHAR2(50) := 'AIA Singapore Calendar' ; */

    PUBLIC VARIABLE Txt_User CONSTANT VARCHAR(20) = 'HookUser';  /* ORIGSQL: Txt_User Constant Varchar2(20) := 'HookUser' ; */

    /* End lines imported from package header */

    /*
    ************************************************
    Version     Create By       Create Date   Change
    ************************************************
    1           Callidus         20150407    Production version
    2           Endi             20160229    Added query hint to SP_TXA_PIAOR procedure - see Log('40')
    5           Jeff             20170217    PIAOR enhancement
    8           Sammi            20180824    Rewrite PIAOR,logic copy from callidus rules and pakeage stage hook
    9           Sammi            20181206    add breakdown by Line of business for PI and AOR detail calculation
    10          Sammi            20190102    SP_TXA_PIAOR long run tuning
    11          Sammi            20200110    Fix per_limra dupicate data
    */
    PRIVATE VARIABLE gv_error VARCHAR(1000);  /* ORIGSQL: gv_error varchar2(1000) ; */
    PRIVATE VARIABLE Gv_Processingunitseq BIGINT;  /* ORIGSQL: Gv_Processingunitseq Int ; */
    PRIVATE VARIABLE Gv_Periodseq BIGINT;  /* ORIGSQL: Gv_Periodseq Int ; */
    --last period  November 2014
    PRIVATE VARIABLE gv_calendarSeq BIGINT;  /* ORIGSQL: gv_calendarSeq int ; */
    PRIVATE VARIABLE gv_plStartTime TIMESTAMP;  /* ORIGSQL: gv_plStartTime timestamp ; */
    PRIVATE VARIABLE gv_isYearEnd BIGINT;  /* ORIGSQL: gv_isYearEnd int ; */
    PRIVATE VARIABLE Gv_Pipelinerunseq BIGINT;  /* ORIGSQL: Gv_Pipelinerunseq Int ; */
    PRIVATE VARIABLE gv_CrossoverEffectiveDate TIMESTAMP;  /* ORIGSQL: gv_CrossoverEffectiveDate date ; */
    PRIVATE VARIABLE gv_CYCLE_DATE VARCHAR(10);  /* ORIGSQL: gv_CYCLE_DATE VARCHAR2(10) ; */

    --for revamp end
    PRIVATE VARIABLE Gv_Setnumberpi BIGINT;  /* ORIGSQL: Gv_Setnumberpi Int ; */
    PRIVATE VARIABLE gv_setnumberaor BIGINT;  /* ORIGSQL: gv_setnumberaor Int ; */

    --for revamp end
    PRIVATE VARIABLE gv_hryc BIGINT;  /* ORIGSQL: gv_hryc int ; */


/* The procedure EXT.PK_PIAOR_CALCULATION:init_session_global initializes the package/library variables */ 

--********** Creating procedure 'EXT.PK_PIAOR_CALCULATION:init_session_global' ********************
PUBLIC PROCEDURE init_session_global()
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN

        /*
        ************************************************
        Version     Create By       Create Date   Change
        ************************************************
        1           Callidus         20150407    Production version
        2           Endi             20160229    Added query hint to SP_TXA_PIAOR procedure - see Log('40')
        5           Jeff             20170217    PIAOR enhancement
        8           Sammi            20180824    Rewrite PIAOR,logic copy from callidus rules and pakeage stage hook
        9           Sammi            20181206    add breakdown by Line of business for PI and AOR detail calculation
        10          Sammi            20190102    SP_TXA_PIAOR long run tuning
        11          Sammi            20200110    Fix per_limra dupicate data
        */

    DECLARE init_timestamp VARCHAR(50) := TO_VARCHAR(CURRENT_TIMESTAMP);
    DECLARE cEndofTime CONSTANT TIMESTAMP = TO_DATE('01012200','mmddyyyy');
            -- Author  : ASNPHY8
        -- Created : 9/4/2018 4:06:02 PM
        -- Purpose : PIAOR rewrite
        SET 'AOR_PIB_Rate' = NULL;
        SET 'AOR_PIB_Rate' = 0.02;
        SET 'AOR_RYC_Rate' = 0.03;
        SET 'gv_error'= NULL;
        SET 'Gv_Processingunitseq' = 38280596832649218;
        SET 'Gv_Periodseq' = 2533274790398900;
        --last period  November 2014
        SET 'gv_calendarSeq' = 2251799813685250;
        SET 'gv_plStartTime' = NULL;
        SET 'gv_isYearEnd' = 0;
        SET 'Gv_Pipelinerunseq' = 0;
        SET 'gv_CrossoverEffectiveDate' = NULL;
        SET 'gv_CYCLE_DATE' = NULL;
        --for revamp end
        SET 'Gv_Setnumberpi' = 3;
        SET 'gv_setnumberaor' = 4;
        --for revamp end
        SET 'gv_hryc' = 16607023625930577;
        SET 'cEndofTime' = :cEndofTime;
    END;

--- must be defined here for testing, otherwise, HRYC will be impact
--********** Creating procedure 'EXT.PK_PIAOR_CALCULATION:init' ********************
/* ORIGSQL: procedure init as begin */
PUBLIC PROCEDURE init
()
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* initialize library variables, if not yet done */
    CALL init_session_global();

    --setup processing unit seq number
    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PROCESSINGUNIT' not found */

    SELECT
        processingunitseq
    INTO
        Gv_Processingunitseq
    FROM
        cs_processingunit
    WHERE
        name = :STR_PU;

    --setup calendar seq number
    /* RESOLVE: Identifier not found: Table/view 'EXT.CS_CALENDAR' not found */

    SELECT
        CALENDARSEQ
    INTO
        gv_calendarSeq
    FROM
        cs_calendar
    WHERE
        name = :STR_CALENDARNAME;

    --get current cycle date

    SELECT
        CTL.TXT_KEY_VALUE
    INTO
        gv_CYCLE_DATE
    FROM
        IN_ETL_CONTROL CTL
    WHERE
        CTL.TXT_KEY_STRING = 'OPER_CYCLE_DATE';
END;

--********** Creating procedure 'EXT.PK_PIAOR_CALCULATION:init_dbmtkoverloaded_1' ********************
/* ORIGSQL: PROCEDURE init(P_STR_CYCLEDATE in VARCHAR2) as begin */
/* sapdbmtk: Procedure declaration 'init' is overloaded, renamed to 'init_dbmtkoverloaded_1' */
PRIVATE PROCEDURE init_dbmtkoverloaded_1
(
    IN P_STR_CYCLEDATE VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                      /* ORIGSQL: P_STR_CYCLEDATE IN VARCHAR2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* initialize library variables, if not yet done */
    CALL init_session_global();

    --setup processing unit seq number 
    SELECT
        processingunitseq
    INTO
        Gv_Processingunitseq
    FROM
        cs_processingunit
    WHERE
        name = :STR_PU;

    --setup calendar seq number 
    SELECT
        CALENDARSEQ
    INTO
        gv_calendarSeq
    FROM
        cs_calendar
    WHERE
        name = :STR_CALENDARNAME;

    --get current cycle date

    gv_CYCLE_DATE = :P_STR_CYCLEDATE;
END;

--********** Creating procedure 'EXT.PK_PIAOR_CALCULATION:Log' ********************
/* ORIGSQL: procedure Log(inText varchar2) is BEGIN AUTONOMOUS TRANSACTION vText varchar2(4000); */
PUBLIC PROCEDURE Log
(
    IN inText VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                             /* ORIGSQL: inText varchar2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
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

        /* ORIGSQL: insert into CS_PIAOR_DEBUG (text, value) values ('PIAOR_' || vText, 1); */
        INSERT INTO CS_PIAOR_DEBUG
            (
                text, value
            )
        VALUES (
                'PIAOR_' || IFNULL(:vText,''),
                1
        );

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: dbms_output.put_line('PIAOR_' || vText); */
        /* RESOLVE: Statement 'CALL sapdbmtk.sp_dbmtk_buffered_output_writeln' not currently supported in HANA AUTONOMOUS TRANSACTION */
        CALL SQLSCRIPT_PRINT:PRINT_LINE ('PIAOR_' || IFNULL(:vText,''));

        /* ORIGSQL: exception when others then */
    END;
END;

--********** Creating procedure 'EXT.PK_PIAOR_CALCULATION:comDebugger' ********************
/* ORIGSQL: procedure comDebugger(i_objName in varchar2, i_objContent in varchar2) is BEGIN AUTONOMOUS TRANSACTION begin pragma autonomous_transaction; */
PUBLIC PROCEDURE comDebugger
(
    IN i_objName VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                 /* ORIGSQL: i_objName IN varchar2 */
    IN i_objContent VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                   /* ORIGSQL: i_objContent IN varchar2 */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* ORIGSQL: pragma autonomous_transaction; */
    BEGIN AUTONOMOUS TRANSACTION
        /* ORIGSQL: insert into sh_debugger values (i_objName, sysdate, i_objContent); */
        INSERT INTO sh_debugger
        VALUES (
                :i_objName,
                CURRENT_TIMESTAMP,  /* ORIGSQL: sysdate */
                :i_objContent
        );

        /* ORIGSQL: commit; */
        COMMIT;
    END;
END;

        --********** Creating procedure 'EXT.PK_PIAOR_CALCULATION:Comtransferpiaor' ********************
        /* ORIGSQL: Procedure Comtransferpiaor(I_R_Agydisttrxn In R_Agydisttrxn) as v_policyIssueDate date; */
PUBLIC PROCEDURE Comtransferpiaor
(
    IN I_R_Agydisttrxn_FIELD_Salestransactionseq BIGINT,     /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_SALESORDERSEQ BIGINT,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Wagency VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_wAgencyLeader VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Wagyldrtitle VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_LdrCurRole VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Wagyldrdistrict VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_CurDistrict VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Policyissuedate TIMESTAMP,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Compensationdate TIMESTAMP,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Wagtclass VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Commissionagy VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Runningtype VARCHAR(100),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Eventtypeid VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Productname VARCHAR(100),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Businessunitmap VARCHAR(100),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Orphanpolicy VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Managerseq BIGINT,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Agyspinoffindicator VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Agyspinoffflag BIGINT,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Versioningdate TIMESTAMP,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Periodseq BIGINT,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Spinstartdate TIMESTAMP,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Spindaterange DECIMAL(38,10),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Txnclasscode VARCHAR(10),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Spinenddate TIMESTAMP,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_actualOrphanPolicy VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_wAgyLdrCde VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_setup VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_txnCode VARCHAR(30)   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_policyIssueDate TIMESTAMP;  /* ORIGSQL: v_policyIssueDate date; */
    DECLARE V_Compensationdate TIMESTAMP;  /* ORIGSQL: V_Compensationdate Date; */
    DECLARE v_maxSetNumber BIGINT;  /* ORIGSQL: v_maxSetNumber int; */

    --removed
    DECLARE v_rule VARCHAR(100);  /* ORIGSQL: v_rule varchar2(100); */
    DECLARE v_writingAgyLdr VARCHAR(30);  /* ORIGSQL: v_writingAgyLdr varchar2(30); */
    DECLARE v_wAgency VARCHAR(30);  /* ORIGSQL: v_wAgency varchar2(30); */
    DECLARE v_wAgencyLeader VARCHAR(30);  /* ORIGSQL: v_wAgencyLeader varchar2(30); */
    DECLARE v_wAgyLdrTitle VARCHAR(30);  /* ORIGSQL: v_wAgyLdrTitle varchar2(30); */
    DECLARE v_wAgyLdrDistrict VARCHAR(30);  /* ORIGSQL: v_wAgyLdrDistrict varchar2(30); */
    DECLARE v_Wagtclass VARCHAR(10);  /* ORIGSQL: v_Wagtclass varchar2(10); */
    DECLARE vNewWritingAgy VARCHAR(10);  /* ORIGSQL: vNewWritingAgy varchar2(10); */
    DECLARE vAorNewWritingAgy VARCHAR(10);  /* ORIGSQL: vAorNewWritingAgy varchar2(10); */
    DECLARE v_AorRule VARCHAR(100);  /* ORIGSQL: v_AorRule varchar2(100); */
    DECLARE V_Aorwagyldr VARCHAR(30);  /* ORIGSQL: V_Aorwagyldr Varchar2(30); */
    DECLARE v_commissionAgy VARCHAR(30);  /* ORIGSQL: v_commissionAgy varchar2(30); */
    DECLARE v_CurDistrict VARCHAR(30);  /* ORIGSQL: v_CurDistrict Varchar2(30); */

    --add by nelson
    DECLARE v_LdrCurRole VARCHAR(30);  /* ORIGSQL: v_LdrCurRole Varchar2(30); */

    --add by nelson
    DECLARE v_wAgyLdrCde VARCHAR(30);  /* ORIGSQL: v_wAgyLdrCde Varchar2(30); */

    --add by nelson
    DECLARE v_setup VARCHAR(30);  /* ORIGSQL: v_setup Varchar2(30); */

    --add by nelson
    DECLARE v_wAgyLdrCurClass VARCHAR(30);  /* ORIGSQL: v_wAgyLdrCurClass varchar2(30); */
    DECLARE v_standalone VARCHAR(30);  /* ORIGSQL: v_standalone varchar2(30); */

    --add by jeff
    DECLARE v_standalone2 DECIMAL(30);  /* ORIGSQL: v_standalone2 number(30); */

    --add by jeff
    DECLARE v_periodenddate TIMESTAMP;  /* ORIGSQL: v_periodenddate date; */

    --add by jeff

    DECLARE V_Crossoverflag BIGINT = 0;  /* ORIGSQL: V_Crossoverflag Int:=0; */
    DECLARE v_ConstantCrossoverDate TIMESTAMP = to_char('1/1/2005','mm/dd/yyyy');  /* ORIGSQL: v_ConstantCrossoverDate date:=to_date('1/1/2005','mm/dd/yyyy') ; */

    DECLARE V_Manageragy VARCHAR(30);  /* ORIGSQL: V_Manageragy Varchar2(30); */
    DECLARE v_RunningType VARCHAR(255);  /* ORIGSQL: v_RunningType varchar2(255); */
    DECLARE v_OrphanPolicy VARCHAR(30);  /* ORIGSQL: v_OrphanPolicy varchar2(30); */
    DECLARE INVALID_MANAGER CONDITION;  /* ORIGSQL: Invalid_Manager exception; */

    DECLARE EXIT HANDLER FOR INVALID_MANAGER
        /* ORIGSQL: When Invalid_Manager Then */
        BEGIN
            /* ORIGSQL: Comdebugger('ComTransferPIAOR','Stagehook is not able get any spin off manager'||I_R_Agydisttrxn.salestransactionSeq) */
            CALL comDebugger('ComTransferPIAOR', 'Stagehook is not able get any spin off manager'||IFNULL(:I_R_Agydisttrxn_FIELD_Salestransactionseq,''));
        END;



    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: when others then */
        BEGIN
            /* ORIGSQL: comDebugger('ComTransferPIAOR','Error'||sqlerrm) */
            CALL comDebugger('ComTransferPIAOR', 'Error'||::SQL_ERROR_MESSAGE 
            );  /* ORIGSQL: sqlerrm */
        END;

        /* initialize library variables, if not yet done */
        CALL init_session_global();



    /*
    begin
      select nvl(max(setNumber),0)
      into v_maxSetNumber
      from cs_transactionAssignment
      where I_R_Agydisttrxn.salestransactionSeq=salestransactionSeq
     And Genericattribute4 IS NULL;
    
    exception when no_data_found then
      v_maxSetNumber:=0;
    end;
    
    */

    --DBMS_OUTPUT.put_line(c_txn.salestransactionSeq||'   start c_txn  issue date  '||to_char(v_policyIssueDate,'mm/dd/yyyy'));
    --DBMS_OUTPUT.put_line(v_policyIssueDate||' ---- '||v_cutoverdate);

    v_wAgency = :I_R_Agydisttrxn_FIELD_Wagency;

    v_wAgencyLeader = :I_R_Agydisttrxn_FIELD_wAgencyLeader;

    v_wAgyLdrTitle = :I_R_Agydisttrxn_FIELD_Wagyldrtitle;

    v_wAgyLdrDistrict = :I_R_Agydisttrxn_FIELD_Wagyldrdistrict;

    v_Wagtclass = :I_R_Agydisttrxn_FIELD_Wagtclass;

    v_policyIssueDate = :I_R_Agydisttrxn_FIELD_Policyissuedate;

    v_commissionAgy = :I_R_Agydisttrxn_FIELD_Commissionagy;

    v_RunningType = :I_R_Agydisttrxn_FIELD_Runningtype;

    v_OrphanPolicy = :I_R_Agydisttrxn_FIELD_Orphanpolicy;

    v_CurDistrict = :I_R_Agydisttrxn_FIELD_CurDistrict;
    --add by nelson

    v_LdrCurRole = :I_R_Agydisttrxn_FIELD_LdrCurRole;
    --add by nelson

    v_wAgyLdrCde = :I_R_Agydisttrxn_FIELD_wAgyLdrCde;
    --add by nelson

    v_setup = :I_R_Agydisttrxn_FIELD_setup;
    --add by nelson 

    SELECT
        enddate
    INTO
        v_periodenddate
    FROM
        cs_period
    WHERE
        periodseq = :Gv_Periodseq;

    --comDebugger('PIAOR DEBUGGER','WAGENCY['||v_wAgency||']||');

    IF :I_R_Agydisttrxn_FIELD_Wagency IS NOT NULL
    THEN
        --comDebugger('PIAOR DEBUGGER','vNewWritingAgy: '||vNewWritingAgy ||' -- '||v_wAgencyLeader);
        -- pi

        IF (:I_R_Agydisttrxn_FIELD_Eventtypeid = 'RYC' 
            OR (I_R_Agydisttrxn_FIELD_Eventtypeid = 'ORYC'
                AND SUBSTRING(:I_R_Agydisttrxn_FIELD_Orphanpolicy,1,1) = 'X'))  /* ORIGSQL: substr(I_R_Agydisttrxn.OrphanPolicy,1,1) */
        AND :I_R_Agydisttrxn_FIELD_Productname IN ('LF','HS')
        AND :I_R_Agydisttrxn_FIELD_txnCode IN ('PAY2','PAY3','PAY4','PAY5','PAY6')
        THEN
            IF :v_wAgyLdrTitle = 'FSD' 
            THEN
                IF :v_wAgyLdrDistrict = :v_commissionAgy
                THEN
                    v_rule = 'PI - Direct Team';

                    v_writingAgyLdr = :v_wAgyLdrDistrict;

                    --               Log('C2');
                ELSE 
                    v_rule = 'PI - Indirect Team';

                    v_writingAgyLdr = :v_wAgyLdrDistrict;

                    --         Log('C3');
                END IF;
                ELSEIF :v_wAgyLdrTitle = 'FSAD'   /* ORIGSQL: Elsif V_Wagyldrtitle='FSAD' Then */
                THEN
                    -- If I_R_Agydisttrxn.Agyspinoffindicator='Y' And I_R_Agydisttrxn.Agyspinoffflag=1 Then
                    --above checking was disabled

                    IF :I_R_Agydisttrxn_FIELD_Spinstartdate IS NOT NULL
                    THEN
                        --spin off case

                        v_writingAgyLdr = :v_wAgencyLeader;

                        v_rule = 'PI - Direct Team';

                        v_RunningType = IFNULL(:v_RunningType,'')||'_SpinOff';

                        --         Log('C4');
                    ELSE 
                        --non spin off case

                        IF :I_R_Agydisttrxn_FIELD_Wagtclass <> '12' 
                        THEN
                            -- FSAD no need compare ga13 with district, as long as class=10, writingAgy always get PI

                            /* RESOLVE: Identifier not found: Table/view 'EXT.CS_POSITION' not found */
                            SELECT
                                p.genericattribute4
                            INTO
                                v_wAgyLdrCurClass
                            FROM
                                cs_position p,
                                cs_period t
                            WHERE
                                p.name = :v_wAgyLdrCde
                                AND p.removeDate = :cdt_EndOfTime
                                AND t.periodseq = :Gv_Periodseq
                                AND p.effectiveStartDate <= TO_DATE(ADD_SECONDS(t.enddate,(86400*-1)))   /* ORIGSQL: t.enddate -1 */
                                AND p.effectiveEndDate > TO_DATE(ADD_SECONDS(t.enddate,(86400*-1)));  /* ORIGSQL: t.enddate-1 */

                            --Log('C5');

                            IF :v_wAgyLdrCurClass <> '12' 
                            THEN
                                v_rule = 'PI - Direct Team';

                                v_writingAgyLdr = :v_wAgencyLeader;

                                --                  Log('C6');
                            ELSE 
                                v_rule = 'PI - Indirect Team';

                                v_writingAgyLdr = :v_wAgyLdrDistrict;

                                --                Log('C7');
                            END IF;
                        ELSE 
                            v_rule = 'PI - Indirect Team';

                            v_writingAgyLdr = :v_wAgyLdrDistrict;

                            --            Log('C8');
                        END IF;
                    END IF;
                    --spin off chking
                ELSEIF :v_wAgyLdrTitle IN ('FSM','AM')  /* ORIGSQL: elsif v_wAgyLdrTitle in ('FSM','AM') then */
                THEN
                    --
                    --  Log('C9');

                    IF :I_R_Agydisttrxn_FIELD_Agyspinoffindicator = 'N' 
                    AND :I_R_Agydisttrxn_FIELD_Agyspinoffflag = 1
                    THEN
                        --spin off case
                        BEGIN 
                            DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
                                /* ORIGSQL: When No_Data_Found Then */
                                BEGIN
                                    /* ORIGSQL: raise Invalid_Manager; */
                                    SIGNAL INVALID_MANAGER;
                                END;

                                 
                            SELECT
                                Name
                            INTO
                                V_Manageragy
                            FROM
                                Cs_Position
                            WHERE
                                Ruleelementownerseq = :I_R_Agydisttrxn_FIELD_Managerseq
                                AND Removedate = :cdt_EndOfTime
                                AND Effectivestartdate <= :I_R_Agydisttrxn_FIELD_Versioningdate
                                AND effectiveEndDate > :I_R_Agydisttrxn_FIELD_Versioningdate;

                            /* ORIGSQL: Exception When No_Data_Found Then */
                        END;
                        --Log('C10');

                        v_rule = 'PI - Indirect Team';

                        v_writingAgyLdr = :V_Manageragy;

                        v_RunningType = IFNULL(:v_RunningType,'')||'_SpinOff';
                    ELSE 
                        --non spin off

                        IF :v_wAgyLdrDistrict = :v_commissionAgy
                        THEN
                            v_rule = 'PI - Indirect Team';

                            v_writingAgyLdr = :v_wAgyLdrDistrict;

                            --            Log('C11');
                        ELSE 
                            v_rule = 'PI - Indirect Team';

                            v_writingAgyLdr = :v_wAgyLdrDistrict;

                            --          Log('C12');
                        END IF;
                    END IF;
                    --spin off chking for um
                END IF;
                --Log('C13');

                --add version 5 (if payee is ever a standalone AM, and current title is FSAD, then it will set all in direct team)
                IF :v_writingAgyLdr IS NOT NULL
                THEN 
                    SELECT
                        po.genericattribute11
                    INTO
                        v_standalone
                    FROM
                        cs_position po
                    WHERE
                        po.name = :v_writingAgyLdr
                        AND PO.REMOVEDATE = :cdt_EndOfTime
                        AND po.effectivestartdate <= TO_DATE(ADD_SECONDS(:v_periodenddate,(86400*-1)))   /* ORIGSQL: v_periodenddate - 1 */
                        AND po.effectiveenddate > TO_DATE(ADD_SECONDS(:v_periodenddate,(86400*-1)));  /* ORIGSQL: v_periodenddate - 1 */
                    SELECT
                        COUNT(1)
                    INTO
                        v_standalone2
                    FROM
                        TBL_PIAOR_STANDALONE
                    WHERE
                        agyname = :v_writingAgyLdr;

                    IF :v_standalone = 'FSAD' 
                    AND :v_standalone2 >= 1
                    THEN
                        v_rule = 'PI - Direct Team';
                    END IF;
                END IF;
                --end add

                --check crossover set up
                IF :v_setup <> 'X' 
                AND :v_wAgyLdrTitle IN ('FSD','FSAD','FSM','AM')
                THEN
                    vNewWritingAgy = comGetCrossoverAgy('PI', :v_wAgyLdrCde, :v_policyIssueDate);
                    --add by nelson
                    --Log('C14');

                    IF :vNewWritingAgy IS NOT NULL
                    THEN
                        -- add by nelson

                        v_writingAgyLdr = 'SGY'||IFNULL(:vNewWritingAgy,'');
                        -- add by nelson

                        v_rule = 'PI - Direct Team';
                        -- add by nelson
                        --             Log('C15');
                    END IF;
                    -- add by nelson
                END IF;
            END IF;
            --eventtype check FOR PI
            --comDebugger('PIAOR DEBUGGER','v_rule1: '||v_rule);
            --Log('C16');
            --aor

            IF :I_R_Agydisttrxn_FIELD_Eventtypeid IN ('API','IFYC','FYC', 'SSCP')
            OR (I_R_Agydisttrxn_FIELD_Eventtypeid = 'RYC'
                AND :I_R_Agydisttrxn_FIELD_Productname IN ('LF','HS','PA','CS'))
            THEN
                --Log('C17');
                --add by nelson start

                IF :v_wAgyLdrTitle = 'FSD' 
                AND :v_wAgyLdrDistrict = :v_commissionAgy
                AND :v_wAgyLdrDistrict = :v_CurDistrict
                AND :v_LdrCurRole = 'FSD'
                THEN
                    v_AorRule = 'AOR - Direct Team';

                    V_Aorwagyldr = :v_CurDistrict;

                    --  Log('C18');
                ELSEIF :v_wAgyLdrTitle = 'FSAD'
                AND :v_wAgyLdrDistrict <> :v_CurDistrict
                AND :v_LdrCurRole = 'FSD'   /* ORIGSQL: Elsif v_wAgyLdrTitle = 'FSAD' and v_wAgyLdrDistrict<>v_CurDistrict and v_LdrCurRole ='FSD' then */
                THEN
                    v_AorRule = 'AOR - Indirect Team';

                    V_Aorwagyldr = :v_wAgyLdrDistrict;

                    --Log('C19');
                ELSEIF :v_wAgyLdrTitle = 'FSAD'
                AND (:I_R_Agydisttrxn_FIELD_Spinstartdate IS NOT NULL
                    OR :I_R_Agydisttrxn_FIELD_Spinenddate IS NOT NULL)  /* ORIGSQL: Elsif v_wAgyLdrTitle = 'FSAD' and (I_R_Agydisttrxn.Spinstartdate IS NOT NULL or I_R_Agydisttrxn.Spinenddate IS NOT NULL) then */
                THEN
                    --spin off case

                    v_AorRule = 'AOR - Direct Team';

                    V_Aorwagyldr = :v_wAgyLdrDistrict;

                    --                  Log('C20');

                    IF :I_R_Agydisttrxn_FIELD_Spindaterange > 8
                    OR (:I_R_Agydisttrxn_FIELD_actualOrphanPolicy <> 'O'
                    AND :I_R_Agydisttrxn_FIELD_Compensationdate > :I_R_Agydisttrxn_FIELD_Spinenddate)
                    THEN
                        v_OrphanPolicy = 'XO';
                        --- set the flag as 'XO', the transaction will not get PI or AOR, but stamp 1 to EB4 of trxn
                        --                    Log('C21');
                    END IF;
                ELSE 
                    v_AorRule = 'AOR - Indirect Team';

                    V_Aorwagyldr = :v_CurDistrict;

                    --                Log('C22');
                END IF;
                --add by nelson end

                --add version 5 (if payee is ever a standalone AM, it will reassign to current DM.)
                IF :V_Aorwagyldr IS NOT NULL
                THEN
                    SELECT
                        COUNT(1)
                    INTO
                        v_standalone2
                    FROM
                        TBL_PIAOR_STANDALONE
                    WHERE
                        agyname = :V_Aorwagyldr;

                    IF :v_standalone2 >= 1
                    THEN
                        V_Aorwagyldr = :v_CurDistrict;
                    END IF;
                END IF;
                --end add

                IF :v_setup <> 'X' 
                THEN
                    vAorNewWritingAgy = comGetCrossoverAgy('AOR', :v_wAgyLdrCde, :v_policyIssueDate);

                    --Log('C23');

                    IF :vAorNewWritingAgy IS NOT NULL
                    THEN
                        --Vnewwritingagy:='SGY'||Vnewwritingagy; commented by nelson

                        V_Aorwagyldr = 'SGY'||IFNULL(:vAorNewWritingAgy,'');
                        -- add by nelson

                        v_AorRule = 'AOR - Direct Team';

                        --               Log('C24');
                    END IF;
                END IF;
            END IF;
            --EVENTTYPE CHECK FOR AOR
            --comDebugger('PIAOR DEBUGGER','v_rule2'||v_rule);
            --Log('C25');

            IF :v_rule IS NOT NULL
            AND :I_R_Agydisttrxn_FIELD_Eventtypeid IN ('RYC','ORYC')
            THEN
                -- v_maxSetNumber:=v_maxSetNumber+1;
                ---   Log('C26');

                /* ORIGSQL: insert into SH_QUERY_RESULT (component,periodseq, genericSequence1, genericSequence2, genericAttribute1, genericAttribute2, genericAttribute3, genericAttribute4, genericAttribute5, genericDate1, gener(...) */
                INSERT INTO SH_QUERY_RESULT
                    (
                        component, periodseq,
                        genericSequence1 /* --txnseq */,
                        genericSequence2  /* --orderSeq */,
                        genericAttribute1 /* --wAgyLdr */,
                        genericAttribute2  /* --wAgyLdrTitle */,
                        genericAttribute3   /* --wAgy */,
                        genericAttribute4   /* --wAgyDistrict */,
                        genericAttribute5  /* --rule */,
                        genericDate1    /* --policyIssueDate */,
                        genericDate2     /* --compensationDate */,
                        genericNumber1  /* --setNumber */,
                        genericAttribute6  /* --BUMap */,
                        GENERICATTRIBUTE7,
                        Genericattribute8,
                        Genericattribute9,
                        Genericattribute10 /* --ga10 is the rule before redirect */,
                        Genericattribute11,
                        genericAttribute12
                    )
                VALUES (
                        'PI',
                        :Gv_Periodseq,
                        :I_R_Agydisttrxn_FIELD_Salestransactionseq,
                        :I_R_Agydisttrxn_FIELD_SALESORDERSEQ,
                        :v_writingAgyLdr,
                        :v_wAgyLdrTitle,
                        :v_wAgency,
                        :v_wAgyLdrDistrict,
                        :v_rule,
                        :v_policyIssueDate,
                        :I_R_Agydisttrxn_FIELD_Compensationdate,
                        :Gv_Setnumberpi,
                        :I_R_Agydisttrxn_FIELD_Businessunitmap,
                        :I_R_Agydisttrxn_FIELD_Eventtypeid,
                        :I_R_Agydisttrxn_FIELD_Productname,
                        'PI '||IFNULL(:v_RunningType,''),
                        :v_rule,
                        :I_R_Agydisttrxn_FIELD_Orphanpolicy  /* --REAL GA17 */,
                        :I_R_Agydisttrxn_FIELD_Txnclasscode
                );
            END IF;

            IF :v_AorRule IS NOT NULL
            AND :I_R_Agydisttrxn_FIELD_Eventtypeid IN ('RYC','API','IFYC','FYC', 'SSCP')
            THEN
                --v_maxSetNumber:=v_maxSetNumber+1;
                -- Log('C27');

                /* ORIGSQL: insert into SH_QUERY_RESULT (component,periodseq, genericSequence1, genericSequence2, genericAttribute1, genericAttribute2, genericAttribute3, genericAttribute4, genericAttribute5, genericDate1, gener(...) */
                INSERT INTO SH_QUERY_RESULT
                    (
                        component, periodseq,
                        genericSequence1 /* --txnseq */,
                        genericSequence2  /* --orderSeq */,
                        genericAttribute1 /* --wAgyLdr */,
                        genericAttribute2  /* --wAgyLdrTitle */,
                        genericAttribute3   /* --wAgy */,
                        genericAttribute4   /* --wAgyDistrict */,
                        genericAttribute5  /* --rule */,
                        genericDate1    /* --policyIssueDate */,
                        genericDate2     /* --compensationDate */,
                        genericNumber1  /* --setNumber */,
                        genericAttribute6  /* --BUMap */,
                        GENERICATTRIBUTE7 /* --eventtype */,
                        GENERICATTRIBUTE8 /* --productname */,
                        Genericattribute9 /* --running type */,
                        Genericattribute10 /* --rule before redicrect, because, after the SP, stagehook will update GA5 */,
                        Genericattribute11  /* --REAL GA17 */,
                        Genericdate3 /* --spin off start date */,
                        Genericnumber2 /* --spin off range */,
                        genericAttribute12 /* --txn classcode */
                    )
                VALUES (
                        'AOR',
                        :Gv_Periodseq,
                        :I_R_Agydisttrxn_FIELD_Salestransactionseq,
                        :I_R_Agydisttrxn_FIELD_SALESORDERSEQ,
                        :V_Aorwagyldr,
                        :v_wAgyLdrTitle,
                        :v_wAgency,
                        :v_wAgyLdrDistrict,
                        :v_AorRule,
                        :v_policyIssueDate,
                        :I_R_Agydisttrxn_FIELD_Compensationdate,
                        :gv_setnumberaor,
                        :I_R_Agydisttrxn_FIELD_Businessunitmap,
                        :I_R_Agydisttrxn_FIELD_Eventtypeid,
                        :I_R_Agydisttrxn_FIELD_Productname,
                        'AOR '||IFNULL(:v_RunningType,''),
                        :v_AorRule,
                        :v_OrphanPolicy,
                        :I_R_Agydisttrxn_FIELD_Spinstartdate,
                        :I_R_Agydisttrxn_FIELD_Spindaterange,
                        :I_R_Agydisttrxn_FIELD_Txnclasscode
                );
            END IF;
        END IF;
        -- wAgency IS NOT NULL

        v_rule = NULL;

        v_AorRule = NULL;

        --Log('C29');

        /* ORIGSQL: Exception When Invalid_Manager Then */
    END;

    --********** Creating procedure 'EXT.PK_PIAOR_CALCULATION:Comtransferpiaor_debug' ********************
    /* ORIGSQL: Procedure Comtransferpiaor_debug(I_R_Agydisttrxn In R_Agydisttrxn) as v_policyIssueDate date; */

PUBLIC PROCEDURE Comtransferpiaor_debug
(
    IN I_R_Agydisttrxn_FIELD_Salestransactionseq BIGINT,     /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_SALESORDERSEQ BIGINT,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Wagency VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_wAgencyLeader VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Wagyldrtitle VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_LdrCurRole VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Wagyldrdistrict VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_CurDistrict VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Policyissuedate TIMESTAMP,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Compensationdate TIMESTAMP,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Wagtclass VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Commissionagy VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Runningtype VARCHAR(100),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Eventtypeid VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Productname VARCHAR(100),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Businessunitmap VARCHAR(100),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Orphanpolicy VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Managerseq BIGINT,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Agyspinoffindicator VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Agyspinoffflag BIGINT,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Versioningdate TIMESTAMP,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Periodseq BIGINT,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Spinstartdate TIMESTAMP,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Spindaterange DECIMAL(38,10),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Txnclasscode VARCHAR(10),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Spinenddate TIMESTAMP,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_actualOrphanPolicy VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_wAgyLdrCde VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_setup VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_txnCode VARCHAR(30)   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_policyIssueDate TIMESTAMP;  /* ORIGSQL: v_policyIssueDate date; */
    DECLARE V_Compensationdate TIMESTAMP;  /* ORIGSQL: V_Compensationdate Date; */
    DECLARE v_maxSetNumber BIGINT;  /* ORIGSQL: v_maxSetNumber int; */

    --removed
    DECLARE v_rule VARCHAR(100);  /* ORIGSQL: v_rule varchar2(100); */
    DECLARE v_writingAgyLdr VARCHAR(30);  /* ORIGSQL: v_writingAgyLdr varchar2(30); */
    DECLARE v_wAgency VARCHAR(30);  /* ORIGSQL: v_wAgency varchar2(30); */
    DECLARE v_wAgencyLeader VARCHAR(30);  /* ORIGSQL: v_wAgencyLeader varchar2(30); */
    DECLARE v_wAgyLdrTitle VARCHAR(30);  /* ORIGSQL: v_wAgyLdrTitle varchar2(30); */
    DECLARE v_wAgyLdrDistrict VARCHAR(30);  /* ORIGSQL: v_wAgyLdrDistrict varchar2(30); */
    DECLARE v_Wagtclass VARCHAR(10);  /* ORIGSQL: v_Wagtclass varchar2(10); */
    DECLARE vNewWritingAgy VARCHAR(10);  /* ORIGSQL: vNewWritingAgy varchar2(10); */
    DECLARE vAorNewWritingAgy VARCHAR(10);  /* ORIGSQL: vAorNewWritingAgy varchar2(10); */
    DECLARE v_AorRule VARCHAR(100);  /* ORIGSQL: v_AorRule varchar2(100); */
    DECLARE V_Aorwagyldr VARCHAR(30);  /* ORIGSQL: V_Aorwagyldr Varchar2(30); */
    DECLARE v_commissionAgy VARCHAR(30);  /* ORIGSQL: v_commissionAgy varchar2(30); */
    DECLARE v_CurDistrict VARCHAR(30);  /* ORIGSQL: v_CurDistrict Varchar2(30); */

    --add by nelson
    DECLARE v_LdrCurRole VARCHAR(30);  /* ORIGSQL: v_LdrCurRole Varchar2(30); */

    --add by nelson
    DECLARE v_wAgyLdrCde VARCHAR(30);  /* ORIGSQL: v_wAgyLdrCde Varchar2(30); */

    --add by nelson
    DECLARE v_setup VARCHAR(30);  /* ORIGSQL: v_setup Varchar2(30); */

    --add by nelson
    DECLARE v_wAgyLdrCurClass VARCHAR(30);  /* ORIGSQL: v_wAgyLdrCurClass varchar2(30); */
    DECLARE V_Crossoverflag BIGINT = 0;  /* ORIGSQL: V_Crossoverflag Int:=0; */
    DECLARE v_ConstantCrossoverDate TIMESTAMP = to_char('1/1/2005','mm/dd/yyyy');  /* ORIGSQL: v_ConstantCrossoverDate date:=to_date('1/1/2005','mm/dd/yyyy') ; */

    DECLARE V_Manageragy VARCHAR(30);  /* ORIGSQL: V_Manageragy Varchar2(30); */
    DECLARE v_RunningType VARCHAR(255);  /* ORIGSQL: v_RunningType varchar2(255); */
    DECLARE v_OrphanPolicy VARCHAR(30);  /* ORIGSQL: v_OrphanPolicy varchar2(30); */
    DECLARE INVALID_MANAGER CONDITION;  /* ORIGSQL: Invalid_Manager exception; */

    DECLARE EXIT HANDLER FOR INVALID_MANAGER
        /* ORIGSQL: When Invalid_Manager Then */
        BEGIN
            /* ORIGSQL: Comdebugger('ComTransferPIAOR','Stagehook is not able get any spin off manager'||I_R_Agydisttrxn.salestransactionSeq) */
            CALL comDebugger('ComTransferPIAOR', 'Stagehook is not able get any spin off manager'||IFNULL(:I_R_Agydisttrxn_FIELD_Salestransactionseq,''));
        END;



    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: when others then */
        BEGIN
            /* ORIGSQL: comDebugger('ComTransferPIAOR','Error'||sqlerrm) */
            CALL comDebugger('ComTransferPIAOR', 'Error'||::SQL_ERROR_MESSAGE 
            );  /* ORIGSQL: sqlerrm */
        END;

        /* initialize library variables, if not yet done */
        CALL init_session_global();



    /*
    begin
      select nvl(max(setNumber),0)
      into v_maxSetNumber
      from cs_transactionAssignment
      where I_R_Agydisttrxn.salestransactionSeq=salestransactionSeq
     And Genericattribute4 IS NULL;
    
    exception when no_data_found then
      v_maxSetNumber:=0;
    end;
    
    */

    --DBMS_OUTPUT.put_line(c_txn.salestransactionSeq||'   start c_txn  issue date  '||to_char(v_policyIssueDate,'mm/dd/yyyy'));
    --DBMS_OUTPUT.put_line(v_policyIssueDate||' ---- '||v_cutoverdate);

    v_wAgency = :I_R_Agydisttrxn_FIELD_Wagency;

    v_wAgencyLeader = :I_R_Agydisttrxn_FIELD_wAgencyLeader;

    v_wAgyLdrTitle = :I_R_Agydisttrxn_FIELD_Wagyldrtitle;

    v_wAgyLdrDistrict = :I_R_Agydisttrxn_FIELD_Wagyldrdistrict;

    v_Wagtclass = :I_R_Agydisttrxn_FIELD_Wagtclass;

    v_policyIssueDate = :I_R_Agydisttrxn_FIELD_Policyissuedate;

    v_commissionAgy = :I_R_Agydisttrxn_FIELD_Commissionagy;

    v_RunningType = :I_R_Agydisttrxn_FIELD_Runningtype;

    v_OrphanPolicy = :I_R_Agydisttrxn_FIELD_Orphanpolicy;

    v_CurDistrict = :I_R_Agydisttrxn_FIELD_CurDistrict;
    --add by nelson

    v_LdrCurRole = :I_R_Agydisttrxn_FIELD_LdrCurRole;
    --add by nelson

    v_wAgyLdrCde = :I_R_Agydisttrxn_FIELD_wAgyLdrCde;
    --add by nelson

    v_setup = :I_R_Agydisttrxn_FIELD_setup;
    --add by nelson

    --comDebugger('PIAOR DEBUGGER','WAGENCY['||v_wAgency||']||');

    /* ORIGSQL: Log('Comtransferpiaor wAgency '|| I_R_Agydisttrxn.wAgency) */
    CALL Log('Comtransferpiaor wAgency '|| IFNULL(:I_R_Agydisttrxn_FIELD_Wagency,''));

    /* ORIGSQL: Log('Comtransferpiaor Eventtypeid '|| I_R_Agydisttrxn.Eventtypeid) */
    CALL Log('Comtransferpiaor Eventtypeid '|| IFNULL(:I_R_Agydisttrxn_FIELD_Eventtypeid,''));

    /* ORIGSQL: Log('Comtransferpiaor OrphanPolicy '|| I_R_Agydisttrxn.OrphanPolicy) */
    CALL Log('Comtransferpiaor OrphanPolicy '|| IFNULL(:I_R_Agydisttrxn_FIELD_Orphanpolicy,''));

    /* ORIGSQL: Log('Comtransferpiaor productname '|| I_R_Agydisttrxn.productname) */
    CALL Log('Comtransferpiaor productname '|| IFNULL(:I_R_Agydisttrxn_FIELD_Productname,''));

    /* ORIGSQL: Log('Comtransferpiaor txnCode '|| I_R_Agydisttrxn.txnCode) */
    CALL Log('Comtransferpiaor txnCode '|| IFNULL(:I_R_Agydisttrxn_FIELD_txnCode,''));

    /* ORIGSQL: Log('Comtransferpiaor CurDistrict '|| I_R_Agydisttrxn.CurDistrict) */
    CALL Log('Comtransferpiaor CurDistrict '|| IFNULL(:I_R_Agydisttrxn_FIELD_CurDistrict,''));

    /* ORIGSQL: Log('Comtransferpiaor LdrCurRole '|| I_R_Agydisttrxn.LdrCurRole) */
    CALL Log('Comtransferpiaor LdrCurRole '|| IFNULL(:I_R_Agydisttrxn_FIELD_LdrCurRole,''));

    /* ORIGSQL: Log('Comtransferpiaor wAgyLdrCde '|| I_R_Agydisttrxn.wAgyLdrCde) */
    CALL Log('Comtransferpiaor wAgyLdrCde '|| IFNULL(:I_R_Agydisttrxn_FIELD_wAgyLdrCde,''));

    /* ORIGSQL: Log('Comtransferpiaor wAgencyLeader '|| I_R_Agydisttrxn.wAgencyLeader) */
    CALL Log('Comtransferpiaor wAgencyLeader '|| IFNULL(:I_R_Agydisttrxn_FIELD_wAgencyLeader,''));

    /* ORIGSQL: Log('Comtransferpiaor wAgyLdrTitle '|| I_R_Agydisttrxn.wAgyLdrTitle) */
    CALL Log('Comtransferpiaor wAgyLdrTitle '|| IFNULL(:I_R_Agydisttrxn_FIELD_Wagyldrtitle,''));

    /* ORIGSQL: Log('Comtransferpiaor wAgyLdrDistrict '|| I_R_Agydisttrxn.wAgyLdrDistrict) */
    CALL Log('Comtransferpiaor wAgyLdrDistrict '|| IFNULL(:I_R_Agydisttrxn_FIELD_Wagyldrdistrict,''));

    /* ORIGSQL: Log('Comtransferpiaor Spinstartdate '|| I_R_Agydisttrxn.Spinstartdate) */
    CALL Log('Comtransferpiaor Spinstartdate '|| IFNULL(TO_VARCHAR(:I_R_Agydisttrxn_FIELD_Spinstartdate),''));

    /* ORIGSQL: Log('Comtransferpiaor SpinEnddate '|| I_R_Agydisttrxn.SpinEnddate) */
    CALL Log('Comtransferpiaor SpinEnddate '|| IFNULL(TO_VARCHAR(:I_R_Agydisttrxn_FIELD_Spinenddate),''));

    /* ORIGSQL: Log('Comtransferpiaor Txnclasscode '|| I_R_Agydisttrxn.Txnclasscode) */
    CALL Log('Comtransferpiaor Txnclasscode '|| IFNULL(:I_R_Agydisttrxn_FIELD_Txnclasscode,''));

    /* ORIGSQL: Log('Comtransferpiaor Salestransactionseq '|| I_R_Agydisttrxn.Salestransactionseq) */
    CALL Log('Comtransferpiaor Salestransactionseq '|| IFNULL(:I_R_Agydisttrxn_FIELD_Salestransactionseq,''));

    /* ORIGSQL: Log('Comtransferpiaor Wagency '|| I_R_Agydisttrxn.Wagency) */
    CALL Log('Comtransferpiaor Wagency '|| IFNULL(:I_R_Agydisttrxn_FIELD_Wagency,''));

    /* ORIGSQL: Log('Comtransferpiaor Commissionagy '|| I_R_Agydisttrxn.Commissionagy) */
    CALL Log('Comtransferpiaor Commissionagy '|| IFNULL(:I_R_Agydisttrxn_FIELD_Commissionagy,''));

    IF :I_R_Agydisttrxn_FIELD_Wagency IS NOT NULL
    THEN
        --comDebugger('PIAOR DEBUGGER','vNewWritingAgy: '||vNewWritingAgy ||' -- '||v_wAgencyLeader);
        /* ORIGSQL: Log('C1') */
        CALL Log('C1');

        -- pi

        IF (:I_R_Agydisttrxn_FIELD_Eventtypeid = 'RYC' 
            OR (I_R_Agydisttrxn_FIELD_Eventtypeid = 'ORYC'
                AND SUBSTRING(:I_R_Agydisttrxn_FIELD_Orphanpolicy,1,1) = 'X'))  /* ORIGSQL: substr(I_R_Agydisttrxn.OrphanPolicy,1,1) */
        AND :I_R_Agydisttrxn_FIELD_Productname IN ('LF','HS')
        AND :I_R_Agydisttrxn_FIELD_txnCode IN ('PAY2','PAY3','PAY4','PAY5','PAY6')
        THEN
            IF :v_wAgyLdrTitle = 'FSD' 
            THEN
                IF :v_wAgyLdrDistrict = :v_commissionAgy
                THEN
                    v_rule = 'PI - Direct Team';

                    v_writingAgyLdr = :v_wAgyLdrDistrict;

                    /* ORIGSQL: Log('C2') */
                    CALL Log('C2');
                ELSE 
                    v_rule = 'PI - Indirect Team';

                    v_writingAgyLdr = :v_wAgyLdrDistrict;

                    /* ORIGSQL: Log('C3') */
                    CALL Log('C3');
                END IF;
                ELSEIF :v_wAgyLdrTitle = 'FSAD'   /* ORIGSQL: Elsif V_Wagyldrtitle='FSAD' Then */
                THEN
                    -- If I_R_Agydisttrxn.Agyspinoffindicator='Y' And I_R_Agydisttrxn.Agyspinoffflag=1 Then
                    --above checking was disabled

                    IF :I_R_Agydisttrxn_FIELD_Spinstartdate IS NOT NULL
                    THEN
                        --spin off case

                        v_writingAgyLdr = :v_wAgencyLeader;

                        v_rule = 'PI - Direct Team';

                        v_RunningType = IFNULL(:v_RunningType,'')||'_SpinOff';

                        /* ORIGSQL: Log('C4') */
                        CALL Log('C4');
                    ELSE 
                        --non spin off case

                        IF :I_R_Agydisttrxn_FIELD_Wagtclass <> '12' 
                        THEN
                            -- FSAD no need compare ga13 with district, as long as class=10, writingAgy always get PI  

                            SELECT
                                p.genericattribute4
                            INTO
                                v_wAgyLdrCurClass
                            FROM
                                cs_position p,
                                cs_period t
                            WHERE
                                p.name = :v_wAgyLdrCde
                                AND p.removeDate = :cdt_EndOfTime
                                AND t.periodseq = :Gv_Periodseq
                                AND p.effectiveStartDate <= TO_DATE(ADD_SECONDS(t.enddate,(86400*-1)))   /* ORIGSQL: t.enddate -1 */
                                AND p.effectiveEndDate > TO_DATE(ADD_SECONDS(t.enddate,(86400*-1)));  /* ORIGSQL: t.enddate-1 */

                            /* ORIGSQL: Log('C5') */
                            CALL Log('C5');

                            IF :v_wAgyLdrCurClass <> '12' 
                            THEN
                                v_rule = 'PI - Direct Team';

                                v_writingAgyLdr = :v_wAgencyLeader;

                                /* ORIGSQL: Log('C6') */
                                CALL Log('C6');
                            ELSE 
                                v_rule = 'PI - Indirect Team';

                                v_writingAgyLdr = :v_wAgyLdrDistrict;

                                /* ORIGSQL: Log('C7') */
                                CALL Log('C7');
                            END IF;
                        ELSE 
                            v_rule = 'PI - Indirect Team';

                            v_writingAgyLdr = :v_wAgyLdrDistrict;

                            /* ORIGSQL: Log('C8') */
                            CALL Log('C8');
                        END IF;
                    END IF;
                    --spin off chking
                ELSEIF :v_wAgyLdrTitle IN ('FSM','AM')  /* ORIGSQL: elsif v_wAgyLdrTitle in ('FSM','AM') then */
                THEN
                    --
                    /* ORIGSQL: Log('C9') */
                    CALL Log('C9');

                    IF :I_R_Agydisttrxn_FIELD_Agyspinoffindicator = 'N' 
                    AND :I_R_Agydisttrxn_FIELD_Agyspinoffflag = 1
                    THEN
                        --spin off case
                        BEGIN 
                            DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
                                /* ORIGSQL: When No_Data_Found Then */
                                BEGIN
                                    /* ORIGSQL: raise Invalid_Manager; */
                                    SIGNAL INVALID_MANAGER;
                                END;

                                 
                            SELECT
                                Name
                            INTO
                                V_Manageragy
                            FROM
                                Cs_Position
                            WHERE
                                Ruleelementownerseq = :I_R_Agydisttrxn_FIELD_Managerseq
                                AND Removedate = :cdt_EndOfTime
                                AND Effectivestartdate <= :I_R_Agydisttrxn_FIELD_Versioningdate
                                AND effectiveEndDate > :I_R_Agydisttrxn_FIELD_Versioningdate;

                            /* ORIGSQL: Exception When No_Data_Found Then */
                        END;

                        /* ORIGSQL: Log('C10') */
                        CALL Log('C10');

                        v_rule = 'PI - Indirect Team';

                        v_writingAgyLdr = :V_Manageragy;

                        v_RunningType = IFNULL(:v_RunningType,'')||'_SpinOff';
                    ELSE 
                        --non spin off

                        IF :v_wAgyLdrDistrict = :v_commissionAgy
                        THEN
                            v_rule = 'PI - Indirect Team';

                            v_writingAgyLdr = :v_wAgyLdrDistrict;

                            /* ORIGSQL: Log('C11') */
                            CALL Log('C11');
                        ELSE 
                            v_rule = 'PI - Indirect Team';

                            v_writingAgyLdr = :v_wAgyLdrDistrict;

                            /* ORIGSQL: Log('C12') */
                            CALL Log('C12');
                        END IF;
                    END IF;
                    --spin off chking for um
                END IF;
                /* ORIGSQL: Log('C13') */
                CALL Log('C13');

                --check crossover set up
                IF :v_setup <> 'X' 
                AND :v_wAgyLdrTitle IN ('FSD','FSAD','FSM','AM')
                THEN
                    vNewWritingAgy = comGetCrossoverAgy('PI', :v_wAgyLdrCde, :v_policyIssueDate);
                    --add by nelson
                    /* ORIGSQL: Log('C14') */
                    CALL Log('C14');

                    IF :vNewWritingAgy IS NOT NULL
                    THEN
                        -- add by nelson

                        v_writingAgyLdr = 'SGY'||IFNULL(:vNewWritingAgy,'');
                        -- add by nelson

                        v_rule = 'PI - Direct Team';
                        -- add by nelson
                        /* ORIGSQL: Log('C15') */
                        CALL Log('C15');
                    END IF;
                    -- add by nelson
                END IF;
            END IF;
            --eventtype check FOR PI
            --comDebugger('PIAOR DEBUGGER','v_rule1: '||v_rule);
            /* ORIGSQL: Log('C16') */
            CALL Log('C16');

            --aor

            IF :I_R_Agydisttrxn_FIELD_Eventtypeid IN ('API','IFYC','FYC', 'SSCP')
            OR (I_R_Agydisttrxn_FIELD_Eventtypeid = 'RYC'
                AND :I_R_Agydisttrxn_FIELD_Productname IN ('LF','HS','PA','CS'))
            THEN
                /* ORIGSQL: Log('C17') */
                CALL Log('C17');

                --add by nelson start

                IF :v_wAgyLdrTitle = 'FSD' 
                AND :v_wAgyLdrDistrict = :v_commissionAgy
                AND :v_wAgyLdrDistrict = :v_CurDistrict
                AND :v_LdrCurRole = 'FSD'
                THEN
                    v_AorRule = 'AOR - Direct Team';

                    V_Aorwagyldr = :v_CurDistrict;

                    /* ORIGSQL: Log('C18') */
                    CALL Log('C18');
                ELSEIF :v_wAgyLdrTitle = 'FSAD'
                AND :v_wAgyLdrDistrict <> :v_CurDistrict
                AND :v_LdrCurRole = 'FSD'   /* ORIGSQL: Elsif v_wAgyLdrTitle = 'FSAD' and v_wAgyLdrDistrict<>v_CurDistrict and v_LdrCurRole ='FSD' then */
                THEN
                    v_AorRule = 'AOR - Indirect Team';

                    V_Aorwagyldr = :v_wAgyLdrDistrict;

                    /* ORIGSQL: Log('C19') */
                    CALL Log('C19');
                ELSEIF :v_wAgyLdrTitle = 'FSAD'
                AND (:I_R_Agydisttrxn_FIELD_Spinstartdate IS NOT NULL
                    OR :I_R_Agydisttrxn_FIELD_Spinenddate IS NOT NULL)  /* ORIGSQL: Elsif v_wAgyLdrTitle = 'FSAD' and (I_R_Agydisttrxn.Spinstartdate IS NOT NULL or I_R_Agydisttrxn.Spinenddate IS NOT NULL) then */
                THEN
                    --spin off case

                    v_AorRule = 'AOR - Direct Team';

                    V_Aorwagyldr = :v_wAgyLdrDistrict;

                    /* ORIGSQL: Log('C20') */
                    CALL Log('C20');

                    IF :I_R_Agydisttrxn_FIELD_Spindaterange > 8
                    OR (:I_R_Agydisttrxn_FIELD_actualOrphanPolicy <> 'O'
                    AND :I_R_Agydisttrxn_FIELD_Compensationdate > :I_R_Agydisttrxn_FIELD_Spinenddate)
                    THEN
                        v_OrphanPolicy = 'XO';
                        --- set the flag as 'XO', the transaction will not get PI or AOR, but stamp 1 to EB4 of trxn
                        /* ORIGSQL: Log('C21') */
                        CALL Log('C21');
                    END IF;
                ELSE 
                    v_AorRule = 'AOR - Indirect Team';

                    V_Aorwagyldr = :v_CurDistrict;

                    /* ORIGSQL: Log('C22') */
                    CALL Log('C22');
                END IF;
                --add by nelson end

                IF :v_setup <> 'X' 
                THEN
                    vAorNewWritingAgy = comGetCrossoverAgy('AOR', :v_wAgyLdrCde, :v_policyIssueDate);

                    /* ORIGSQL: Log('C23') */
                    CALL Log('C23');

                    IF :vAorNewWritingAgy IS NOT NULL
                    THEN
                        --Vnewwritingagy:='SGY'||Vnewwritingagy; commented by nelson

                        V_Aorwagyldr = 'SGY'||IFNULL(:vAorNewWritingAgy,'');
                        -- add by nelson

                        v_AorRule = 'AOR - Direct Team';

                        /* ORIGSQL: Log('C24') */
                        CALL Log('C24');
                    END IF;
                END IF;
            END IF;
            --EVENTTYPE CHECK FOR AOR
            --comDebugger('PIAOR DEBUGGER','v_rule2'||v_rule);
            /* ORIGSQL: Log('C25 '|| V_Rule) */
            CALL Log('C25 '|| IFNULL(:v_rule,''));

            /* ORIGSQL: log('C25-1 '||V_Aorrule) */
            CALL Log('C25-1 '||IFNULL(:v_AorRule,''));

            IF :v_rule IS NOT NULL
            AND :I_R_Agydisttrxn_FIELD_Eventtypeid IN ('RYC','ORYC')
            THEN
                -- v_maxSetNumber:=v_maxSetNumber+1;
                /* ORIGSQL: Log('C26') */
                CALL Log('C26');

                /* ORIGSQL: insert into SH_QUERY_RESULT (component,periodseq, genericSequence1, genericSequence2, genericAttribute1, genericAttribute2, genericAttribute3, genericAttribute4, genericAttribute5, genericDate1, gener(...) */
                INSERT INTO SH_QUERY_RESULT
                    (
                        component, periodseq,
                        genericSequence1 /* --txnseq */,
                        genericSequence2  /* --orderSeq */,
                        genericAttribute1 /* --wAgyLdr */,
                        genericAttribute2  /* --wAgyLdrTitle */,
                        genericAttribute3   /* --wAgy */,
                        genericAttribute4   /* --wAgyDistrict */,
                        genericAttribute5  /* --rule */,
                        genericDate1    /* --policyIssueDate */,
                        genericDate2     /* --compensationDate */,
                        genericNumber1  /* --setNumber */,
                        genericAttribute6  /* --BUMap */,
                        GENERICATTRIBUTE7,
                        Genericattribute8,
                        Genericattribute9,
                        Genericattribute10 /* --ga10 is the rule before redirect */,
                        Genericattribute11,
                        genericAttribute12
                    )
                VALUES (
                        'PI',
                        :Gv_Periodseq,
                        :I_R_Agydisttrxn_FIELD_Salestransactionseq,
                        :I_R_Agydisttrxn_FIELD_SALESORDERSEQ,
                        :v_writingAgyLdr,
                        :v_wAgyLdrTitle,
                        :v_wAgency,
                        :v_wAgyLdrDistrict,
                        :v_rule,
                        :v_policyIssueDate,
                        :I_R_Agydisttrxn_FIELD_Compensationdate,
                        :Gv_Setnumberpi,
                        :I_R_Agydisttrxn_FIELD_Businessunitmap,
                        :I_R_Agydisttrxn_FIELD_Eventtypeid,
                        :I_R_Agydisttrxn_FIELD_Productname,
                        'PI '||IFNULL(:v_RunningType,''),
                        :v_rule,
                        :I_R_Agydisttrxn_FIELD_Orphanpolicy  /* --REAL GA17 */,
                        :I_R_Agydisttrxn_FIELD_Txnclasscode
                );
            END IF;

            IF :v_AorRule IS NOT NULL
            AND :I_R_Agydisttrxn_FIELD_Eventtypeid IN ('RYC','API','IFYC','FYC', 'SSCP')
            THEN
                --v_maxSetNumber:=v_maxSetNumber+1;
                /* ORIGSQL: Log('C27') */
                CALL Log('C27');

                /* ORIGSQL: insert into SH_QUERY_RESULT (component,periodseq, genericSequence1, genericSequence2, genericAttribute1, genericAttribute2, genericAttribute3, genericAttribute4, genericAttribute5, genericDate1, gener(...) */
                INSERT INTO SH_QUERY_RESULT
                    (
                        component, periodseq,
                        genericSequence1 /* --txnseq */,
                        genericSequence2  /* --orderSeq */,
                        genericAttribute1 /* --wAgyLdr */,
                        genericAttribute2  /* --wAgyLdrTitle */,
                        genericAttribute3   /* --wAgy */,
                        genericAttribute4   /* --wAgyDistrict */,
                        genericAttribute5  /* --rule */,
                        genericDate1    /* --policyIssueDate */,
                        genericDate2     /* --compensationDate */,
                        genericNumber1  /* --setNumber */,
                        genericAttribute6  /* --BUMap */,
                        GENERICATTRIBUTE7 /* --eventtype */,
                        GENERICATTRIBUTE8 /* --productname */,
                        Genericattribute9 /* --running type */,
                        Genericattribute10 /* --rule before redicrect, because, after the SP, stagehook will update GA5 */,
                        Genericattribute11  /* --REAL GA17 */,
                        Genericdate3 /* --spin off start date */,
                        Genericnumber2 /* --spin off range */,
                        genericAttribute12 /* --txn classcode */
                    )
                VALUES (
                        'AOR',
                        :Gv_Periodseq,
                        :I_R_Agydisttrxn_FIELD_Salestransactionseq,
                        :I_R_Agydisttrxn_FIELD_SALESORDERSEQ,
                        :V_Aorwagyldr,
                        :v_wAgyLdrTitle,
                        :v_wAgency,
                        :v_wAgyLdrDistrict,
                        :v_AorRule,
                        :v_policyIssueDate,
                        :I_R_Agydisttrxn_FIELD_Compensationdate,
                        :gv_setnumberaor,
                        :I_R_Agydisttrxn_FIELD_Businessunitmap,
                        :I_R_Agydisttrxn_FIELD_Eventtypeid,
                        :I_R_Agydisttrxn_FIELD_Productname,
                        'AOR '||IFNULL(:v_RunningType,''),
                        :v_AorRule,
                        :v_OrphanPolicy,
                        :I_R_Agydisttrxn_FIELD_Spinstartdate,
                        :I_R_Agydisttrxn_FIELD_Spindaterange,
                        :I_R_Agydisttrxn_FIELD_Txnclasscode
                );
            END IF;
        END IF;
        -- wAgency IS NOT NULL

        v_rule = NULL;

        v_AorRule = NULL;

        /* ORIGSQL: Log('C29') */
        CALL Log('C29');

        /* ORIGSQL: Exception When Invalid_Manager Then */
    END;





--********** Creating procedure 'EXT.PK_PIAOR_CALCULATION:SP_TXA_PIAOR' ********************
/* ORIGSQL: procedure SP_TXA_PIAOR as v_periodSeq int; */
PUBLIC PROCEDURE SP_TXA_PIAOR
()
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE v_periodSeq BIGINT;  /* ORIGSQL: v_periodSeq int; */
    DECLARE v_periodStartDate TIMESTAMP;  /* ORIGSQL: v_periodStartDate date; */
    DECLARE v_periodEndDate TIMESTAMP;  /* ORIGSQL: v_periodEndDate date; */
    DECLARE v_cutOverDate TIMESTAMP;  /* ORIGSQL: v_cutOverDate date; */
    DECLARE v_positionName VARCHAR(100);  /* ORIGSQL: v_positionName varchar2(100); */
    DECLARE V_Crossoverflag BIGINT = 0;  /* ORIGSQL: V_Crossoverflag Int:=0; */
    DECLARE v_ConstantCrossoverDate TIMESTAMP;  /* ORIGSQL: v_ConstantCrossoverDate date; */

    DECLARE v_AgyDistTrxn_FIELD_Salestransactionseq BIGINT;  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_SALESORDERSEQ BIGINT;  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Wagency VARCHAR(30);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_wAgencyLeader VARCHAR(30);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Wagyldrtitle VARCHAR(30);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_LdrCurRole VARCHAR(30);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Wagyldrdistrict VARCHAR(30);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_CurDistrict VARCHAR(30);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Policyissuedate TIMESTAMP;  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Compensationdate TIMESTAMP;  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Wagtclass VARCHAR(30);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Commissionagy VARCHAR(30);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Runningtype VARCHAR(100);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Eventtypeid VARCHAR(30);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Productname VARCHAR(100);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Businessunitmap VARCHAR(100);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Orphanpolicy VARCHAR(30);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Managerseq BIGINT;  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Agyspinoffindicator VARCHAR(30);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Agyspinoffflag BIGINT;  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Versioningdate TIMESTAMP;  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Periodseq BIGINT;  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Spinstartdate TIMESTAMP;  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Spindaterange DECIMAL(38,10);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Txnclasscode VARCHAR(10);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_Spinenddate TIMESTAMP;  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_actualOrphanPolicy VARCHAR(30);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_wAgyLdrCde VARCHAR(30);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_setup VARCHAR(30);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_AgyDistTrxn_FIELD_txnCode VARCHAR(30);  /* ORIGSQL: v_AgyDistTrxn R_AgyDistTrxn; */
    DECLARE v_ga13 VARCHAR(30);  /* ORIGSQL: v_ga13 varchar2(30); */
    DECLARE V_Componentvalue_Pi VARCHAR(10) = 'PI';  /* ORIGSQL: V_Componentvalue_Pi Varchar2(10):='PI'; */
    DECLARE v_componentValue_aor VARCHAR(10) = 'AOR';  /* ORIGSQL: v_componentValue_aor varchar2(10):='AOR'; */
    DECLARE V_HRYCSEQ BIGINT;  /* ORIGSQL: V_HRYCSEQ int; */
    DECLARE vSQL VARCHAR(4000);  /* ORIGSQL: vSQL varchar2(4000); */

    /* ORIGSQL: For C_Txn In (WITH tmp_pos AS (select * from cs_position where removeDate=cdt_EndofTime and effectiveStartDate<=v_periodEndDate -1 and effectiveEndDate > v_periodEndDate-1) select x.genericAttribute2,(...) */
    DECLARE CURSOR dbmtk_cursor_54124
    FOR
        --version 8 comment  for tunning
        /*select \*+ parallel(8) leading(x) *\
        x.genericAttribute2,  --add by nelson txn code
        x.genericAttribute14,
        x.Genericattribute17,
        x.Businessunitmap,
        x.Productname,
        --x.Genericattribute13,
            decode(x.AGY_agency,null,x.Genericattribute13,x.AGY_agency) as Genericattribute13, --version 8
        x.Compensationdate,
        x.SALESORDERSEQ,
        x.Salestransactionseq,
        x.genericDate2,
        x.eventtypeid,
        x.wAgency,
        x.wAgencyLeader,
        x.wAgyLdrTitle,
        'SGY'||x.Wagyldrdistrict As Wagyldrdistrict,
        x.Wagtclass,
        'SGY'||curDis.Genericattribute3 as CurDistrict,
        tt.name as LdrCurRole,
        x.wAgyLdrCde ,
         nvl((select stp.txtagt from  In_Pi_Aor_Setup stp
                 where 'SGT'||to_number(stp.txtagt) =  x.wAgyLdrCde
                 and stp.dtecycle = v_periodEndDate-1
                 and stp.txttype in ('C','D')
         and rownum =1),'X') as setup
        from  aia_x  x,
        ---version 5 fix incorrect DMcode
        cs_position agt,  --commision agent search cs_position.genericattribute1
        cs_position curDis,--commision_agent.cs_position.genericattribute1=cs_position.name,search cs_position.Genericattribute3
        cs_position LdrCurRole ,--commision agent' leader agent code.cs_position.titleseq corressponding cs_title.ruleelementownerseq search for name
        cs_title tt
        where curDis.tenantid='AIAS'
        and LdrCurRole.tenantid='AIAS'
        and tt.tenantid='AIAS'
        --and 'SGT'||x.genericattribute12=agt.name
         and decode(x.AGY_agent,null,'SGT'||x.genericattribute12,'SGT'||x.AGY_agent)=agt.name --add version 8
        and agt.removeDate=cdt_EndofTime
        AND agt.effectiveStartDate<=v_periodEndDate -1
        AND agt.effectiveEndDate > v_periodEndDate-1
        and 'SGY'||agt.genericattribute1 = curDis.Name
        and curDis.removeDate=cdt_EndofTime
        AND curDis.effectiveStartDate<=v_periodEndDate -1
        AND curDis.effectiveEndDate > v_periodEndDate-1
        and x.wAgyLdrCde = LdrCurRole.name
        and LdrCurRole.removeDate=cdt_EndofTime
        AND LdrCurRole.effectiveStartDate<=v_periodEndDate -1
        AND LdrCurRole.effectiveEndDate > v_periodEndDate-1
        and LdrCurRole.titleseq = tt.ruleelementownerseq
        and tt.removedate = cdt_EndofTime
        and tt.effectiveenddate  = cdt_EndofTime*/

        --add by nelson end

        WITH 
        tmp_pos   
        AS (
            SELECT   /* ORIGSQL: (select * from cs_position where removeDate=cdt_EndofTime and effectiveStartDate<=v_periodEndDate -1 and effectiveEndDate > v_periodEndDate-1) */
                *
            FROM
                cs_position
            WHERE
                removeDate = :cdt_EndOfTime
                AND effectiveStartDate <= TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate -1 */
        AND effectiveEndDate > TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))    /* ORIGSQL: v_periodEndDate-1 */
        )
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_TITLE' not found */

        SELECT   /* ORIGSQL: select x.genericAttribute2, x.genericAttribute14, x.Genericattribute17, x.Businessunitmap, x.Productname, decode(x.AGY_agency,null,x.Genericattribute13,x.AGY_agency) as Genericattribute13, x.Compensat(...) */
            --tmp table
            /* ORIGSQL: select / *+ leading(x) * / */
            x.genericAttribute2  /* --add by nelson txn code */, x.genericAttribute14,
            x.Genericattribute17,
            x.Businessunitmap,
            x.Productname,
            /* --x.Genericattribute13, */
            MAP(x.AGY_agency, NULL, x.Genericattribute13, x.AGY_agency) AS Genericattribute13 /* --version 8 */, x.Compensationdate,  /* ORIGSQL: decode(x.AGY_agency,null,x.Genericattribute13,x.AGY_agency) */
            x.SALESORDERSEQ,
            x.Salestransactionseq,
            x.genericDate2,
            x.eventtypeid,
            x.wAgency,
            x.wAgencyLeader,
            x.wAgyLdrTitle,
            'SGY'||IFNULL(x.Wagyldrdistrict,'') AS Wagyldrdistrict,
            x.Wagtclass,
            'SGY'||IFNULL(curDis.Genericattribute3,'') AS CurDistrict,
            tt.name AS LdrCurRole,
            x.wAgyLdrCde,
            IFNULL(  /* ORIGSQL: nvl((select stp.txtagt from In_Pi_Aor_Setup stp where 'SGT'||to_number(stp.txtagt) = x.wAgyLdrCde and stp.dtecycle = v_periodEndDate-1 and stp.txttype in ('C','D') and rownum =1),'X') */
                (
                    /* RESOLVE: Review semantics in source vs. target DBMS: LIMIT/OFFSET without ORDER BY: consistent results not guaranteed */
                    SELECT   /* ORIGSQL: (select stp.txtagt from In_Pi_Aor_Setup stp where 'SGT'||to_number(stp.txtagt) = x.wAgyLdrCde and stp.dtecycle = v_periodEndDate-1 and stp.txttype in ('C','D') LIMIT 1) rownum =1 */
                        stp.txtagt
                    FROM
                        In_Pi_Aor_Setup stp
                    WHERE
                        'SGT'||IFNULL(TO_VARCHAR(TO_DECIMAL(stp.txtagt,38,18)),'') = x.wAgyLdrCde  /* ORIGSQL: to_number(stp.txtagt) */
                        AND stp.dtecycle = TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate-1 */
                        AND stp.txttype IN ('C','D')
                    LIMIT 1  /* ORIGSQL: rownum =1 */
                )
            ,'X') AS setup
        FROM
            aia_x x,
            tmp_pos agt,
            tmp_pos curDis,
            tmp_pos LdrCurRole,
            cs_title tt
        WHERE
            curDis.tenantid = 'AIAS'
            AND LdrCurRole.tenantid = 'AIAS'
            AND tt.tenantid = 'AIAS'
            AND MAP(x.AGY_agent, NULL, 'SGT'||IFNULL(x.genericattribute12,''), 'SGT'||IFNULL(x.AGY_agent,'')) = agt.name  /* ORIGSQL: decode(x.AGY_agent,null,'SGT'||x.genericattribute12,'SGT'||x.AGY_agent) */
            AND 'SGY'||IFNULL(agt.genericattribute1,'') = curDis.Name
            AND x.wAgyLdrCde = LdrCurRole.name
            AND LdrCurRole.titleseq = tt.ruleelementownerseq
            AND tt.removedate = :cdt_EndOfTime
            AND tt.effectiveenddate = :cdt_EndOfTime;--version 8 tunning end

    /* ORIGSQL: For C_Txn In (select x.genericAttribute2, x.genericAttribute14, x.Genericattribute17, x.Businessunitmap, x.Productname, decode(x.AGY_agency,null,x.Genericattribute13,x.AGY_agency) as Genericattribute1(...) */
    DECLARE CURSOR dbmtk_cursor_54127
    FOR 
        /* ORIGSQL: select / *+ leading(x1) * / */
        SELECT   /* ORIGSQL: select x.genericAttribute2, x.genericAttribute14, x.Genericattribute17, x.Businessunitmap, x.Productname, decode(x.AGY_agency,null,x.Genericattribute13,x.AGY_agency) as Genericattribute13, x.Compensat(...) */
            x.genericAttribute2  /* --add by nelson txn code */, x.genericAttribute14,
            x.Genericattribute17,
            x.Businessunitmap,
            x.Productname,
            /* --x.Genericattribute13, */
            MAP(x.AGY_agency, NULL, x.Genericattribute13, x.AGY_agency) AS Genericattribute13 /* --version 8 */, x.Compensationdate,  /* ORIGSQL: decode(x.AGY_agency,null,x.Genericattribute13,x.AGY_agency) */
            x.SALESORDERSEQ,
            x.Salestransactionseq,
            x.genericDate2,
            x.eventtypeid,
            x.wAgency,
            x.wAgencyLeader,
            x.wAgyLdrTitle,
            'SGY'||IFNULL(x.Wagyldrdistrict,'') AS Wagyldrdistrict,
            x.Wagtclass,
            'SGY'||IFNULL(curDis.Genericattribute3,'') AS CurDistrict,
            tt.name AS LdrCurRole,
            x.wAgyLdrCde,
            IFNULL(  /* ORIGSQL: nvl((select stp.txtagt from In_Pi_Aor_Setup stp where 'SGT'||to_number(stp.txtagt) = x.wAgyLdrCde and stp.dtecycle = v_periodEndDate-1 and stp.txttype in ('C','D') and rownum =1),'X') */
                (
                    /* RESOLVE: Review semantics in source vs. target DBMS: LIMIT/OFFSET without ORDER BY: consistent results not guaranteed */
                    SELECT   /* ORIGSQL: (select stp.txtagt from In_Pi_Aor_Setup stp where 'SGT'||to_number(stp.txtagt) = x.wAgyLdrCde and stp.dtecycle = v_periodEndDate-1 and stp.txttype in ('C','D') LIMIT 1) rownum =1 */
                        stp.txtagt
                    FROM
                        In_Pi_Aor_Setup stp
                    WHERE
                        'SGT'||IFNULL(TO_VARCHAR(TO_DECIMAL(stp.txtagt,38,18)),'') = x.wAgyLdrCde  /* ORIGSQL: to_number(stp.txtagt) */
                        AND stp.dtecycle = TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate-1 */
                        AND stp.txttype IN ('C','D')
                    LIMIT 1  /* ORIGSQL: rownum =1 */
                )
            ,'X') AS setup
        FROM
            aia_x1 x,
            ---version 5 fix incorrect DMcode
            cs_position agt,
            cs_position curDis,
            cs_position LdrCurRole,
            cs_title tt
        WHERE
            --'SGT'||x.genericattribute12=agt.name
            MAP(x.AGY_agent, NULL, 'SGT'||IFNULL(x.genericattribute12,''), 'SGT'||IFNULL(x.AGY_agent,'')) = agt.name --add version 8
            /* ORIGSQL: decode(x.AGY_agent,null,'SGT'||x.genericattribute12,'SGT'||x.AGY_agent) */
            AND agt.removeDate = :cdt_EndOfTime
            AND agt.effectiveStartDate <= TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate -1 */
            AND agt.effectiveEndDate > TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate-1 */
            AND 'SGY'||IFNULL(agt.genericattribute1,'') = curDis.Name
            AND curDis.removeDate = :cdt_EndOfTime
            AND curDis.effectiveStartDate <= TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate-1 */
            AND curDis.effectiveEndDate > TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate-1 */
            AND x.wAgyLdrCde = LdrCurRole.name
            AND LdrCurRole.removeDate = :cdt_EndOfTime
            AND LdrCurRole.effectiveStartDate <= TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate -1 */
            AND LdrCurRole.effectiveEndDate > TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate-1 */
            AND LdrCurRole.titleseq = tt.ruleelementownerseq
            AND tt.removedate = :cdt_EndOfTime
            AND tt.effectiveenddate = :cdt_EndOfTime;

    --add by nelson end

    --version 10 end

    --add by nelson start
    /* ORIGSQL: for c_txn in (select x.genericAttribute2, x.genericAttribute14, x.Genericattribute17, x.Businessunitmap, x.Productname, decode(x.AGY_agency,null,x.Genericattribute13,x.AGY_agency) as Genericattribute1(...) */
    DECLARE CURSOR dbmtk_cursor_54130
    FOR 
        /* ORIGSQL: select / *+ * / */
        SELECT   /* ORIGSQL: select x.genericAttribute2, x.genericAttribute14, x.Genericattribute17, x.Businessunitmap, x.Productname, decode(x.AGY_agency,null,x.Genericattribute13,x.AGY_agency) as Genericattribute13, x.Compensat(...) */
            x.genericAttribute2  /* --add by nelson txn code */, x.genericAttribute14,
            x.Genericattribute17,
            x.Businessunitmap,
            x.Productname,
            /* --x.Genericattribute13, */
            MAP(x.AGY_agency, NULL, x.Genericattribute13, x.AGY_agency) AS Genericattribute13 /* --version 8 */, x.Compensationdate,  /* ORIGSQL: decode(x.AGY_agency,null,x.Genericattribute13,x.AGY_agency) */
            x.SALESORDERSEQ,
            x.Salestransactionseq,
            x.genericDate2,
            x.eventtypeid,
            x.wAgency,
            x.wAgencyLeader,
            x.wAgyLdrTitle,
            'SGY'||IFNULL(x.Wagyldrdistrict,'') AS Wagyldrdistrict,
            x.Wagtclass,
            x.wAgyLdrCde,
            x.Spinstartdate,
            x.spinEndDate,
            'SGY'||IFNULL(curDis.Genericattribute3,'') AS CurDistrict,
            tt.name AS LdrCurRole,
            IFNULL(  /* ORIGSQL: nvl((select stp.txtagt from In_Pi_Aor_Setup stp where 'SGT'||to_number(stp.txtagt) = x.wAgyLdrCde and stp.dtecycle = v_periodEndDate-1 and stp.txttype in ('C','D') and rownum =1),'X') */
                (
                    /* RESOLVE: Review semantics in source vs. target DBMS: LIMIT/OFFSET without ORDER BY: consistent results not guaranteed */
                    SELECT   /* ORIGSQL: (select stp.txtagt from In_Pi_Aor_Setup stp where 'SGT'||to_number(stp.txtagt) = x.wAgyLdrCde and stp.dtecycle = v_periodEndDate-1 and stp.txttype in ('C','D') LIMIT 1) rownum =1 */
                        stp.txtagt
                    FROM
                        In_Pi_Aor_Setup stp
                    WHERE
                        'SGT'||IFNULL(TO_VARCHAR(TO_DECIMAL(stp.txtagt,38,18)),'') = x.wAgyLdrCde  /* ORIGSQL: to_number(stp.txtagt) */
                        AND stp.dtecycle = TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate-1 */
                        AND stp.txttype IN ('C','D')
                    LIMIT 1  /* ORIGSQL: rownum =1 */
                )
            ,'X') AS setup
            /* --from x, */
        FROM
            tmp_x x,
            --version 10 add
            ---version 5 fix incorrect DMcode
            cs_position agt,
            cs_position curDis,
            cs_position LdrCurRole,
            cs_title tt
        WHERE
            --'SGT'||x.genericattribute12=agt.name
            MAP(x.AGY_agent, NULL, 'SGT'||IFNULL(x.genericattribute12,''), 'SGT'||IFNULL(x.AGY_agent,'')) = agt.name --add version 8
            /* ORIGSQL: decode(x.AGY_agent,null,'SGT'||x.genericattribute12,'SGT'||x.AGY_agent) */
            AND agt.removeDate = :cdt_EndOfTime
            AND agt.effectiveStartDate <= TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate -1 */
            AND agt.effectiveEndDate > TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate-1 */
            AND 'SGY'||IFNULL(agt.genericattribute1,'') = curDis.Name
            AND curDis.removeDate = :cdt_EndOfTime
            AND curDis.effectiveStartDate <= TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate -1 */
            AND curDis.effectiveEndDate > TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate-1 */
            AND x.wAgyLdrCde = LdrCurRole.name
            AND LdrCurRole.removeDate = :cdt_EndOfTime
            AND LdrCurRole.effectiveStartDate <= TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate -1 */
            AND LdrCurRole.effectiveEndDate > TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate-1 */
            AND LdrCurRole.titleseq = tt.ruleelementownerseq
            AND tt.removedate = :cdt_EndOfTime
            AND tt.effectiveenddate = :cdt_EndOfTime
            AND curDis.tenantid = 'AIAS'
            AND LdrCurRole.tenantid = 'AIAS'
            AND tt.tenantid = 'AIAS';

    --add by nelson end

    --search the commission agent current information ,during period end date

    /* ORIGSQL: for c_txn in (WITH tmp_pos AS (select * from cs_position where removeDate=cdt_EndofTime and effectiveStartDate<=v_periodEndDate -1 and effectiveEndDate > v_periodEndDate-1) select x.genericAttribute2,(...) */
    DECLARE CURSOR dbmtk_cursor_54133
    FOR
        --version 8 comment for tunning
        --with x as (select * from tmp_x_St)
        /*select \*+ parallel(8)  leading(curdis,x,ldr,LDRCURROLE,tt)  index(curdis CS_POSITION_AK1) index(ldrcurrole CS_POSITION_AK1) *\
           x.genericAttribute2,
           x.genericAttribute14,
           x.Genericattribute17,
           x.Businessunitmap,
           x.Productname,
           --x.Genericattribute13,
           decode(x.AGY_agency,null,x.Genericattribute13,x.AGY_agency) as Genericattribute13, --version 8
           x.Compensationdate,
           x.SALESORDERSEQ,
           x.Salestransactionseq,
           x.genericDate2,
           x.eventtypeid,
           x.wAgency,
           x.wAgencyLeader,
           x.wAgyLdrTitle,
           'SGY' || x.Wagyldrdistrict as Wagyldrdistrict,
           x.Wagtclass,
           x.wAgyLdrCode as wAgyLdrCde ,
           x.Spinstartdate,
           x.spinEndDate,
           'SGY' || curDis.Genericattribute3 as CurDistrict,
           tt.name as LdrCurRole
           ,x.setup
          from tmp_x_St x,
         ---version 5 fix incorrect DMcode
           cs_position agt,
           cs_position curDis,
           cs_position LdrCurRole,
           cs_title tt
         where --x.sgtga12 = agt.name
         decode(x.AGY_agent,null,x.sgtga12,'SGT'||x.AGY_agent)=agt.name --add version 8
         and agt.removeDate=cdt_EndofTime
         AND agt.effectiveStartDate<=v_periodEndDate -1
         AND agt.effectiveEndDate > v_periodEndDate-1
         and 'SGY'||agt.genericattribute1 = curDis.Name
         and curDis.tenantid = 'AIAS'
         and curDis.removeDate = cdt_EndofTime
         AND curDis.effectiveStartDate <= v_periodenddate-1
         AND curDis.effectiveEndDate > v_periodenddate-1
         and x.wAgyLdrCode = LdrCurRole.name
         and LdrCurRole.tenantid = 'AIAS'
         and LdrCurRole.removeDate = cdt_EndofTime
         AND LdrCurRole.effectiveStartDate <= v_periodenddate-1
         AND LdrCurRole.effectiveEndDate > v_periodenddate-1
         and LdrCurRole.titleseq = tt.ruleelementownerseq
         and tt.removedate = cdt_EndofTime
         and tt.effectiveenddate = cdt_EndofTime
     and tt.tenantid='AIAS')*/
    WITH 
    tmp_pos  
    AS (
        SELECT   /* ORIGSQL: (select * from cs_position where removeDate=cdt_EndofTime and effectiveStartDate<=v_periodEndDate -1 and effectiveEndDate > v_periodEndDate-1) */
            *
        FROM
            cs_position
        WHERE
            removeDate = :cdt_EndOfTime
            AND effectiveStartDate <= TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate -1 */
    AND effectiveEndDate > TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))    /* ORIGSQL: v_periodEndDate-1 */
    )  
    SELECT   /* ORIGSQL: select x.genericAttribute2, x.genericAttribute14, x.Genericattribute17, x.Businessunitmap, x.Productname, decode(x.AGY_agency,null,x.Genericattribute13,x.AGY_agency) as Genericattribute13, x.Compensat(...) */
        --tmp table
        /* ORIGSQL: select / *+ leading(x)* / */
        x.genericAttribute2,
        x.genericAttribute14,
        x.Genericattribute17,
        x.Businessunitmap,
        x.Productname,
        /* --x.Genericattribute13, */
        MAP(x.AGY_agency, NULL, x.Genericattribute13, x.AGY_agency) AS Genericattribute13 /* --version 8 */, x.Compensationdate,  /* ORIGSQL: decode(x.AGY_agency,null,x.Genericattribute13,x.AGY_agency) */
        x.SALESORDERSEQ,
        x.Salestransactionseq,
        x.genericDate2,
        x.eventtypeid,
        x.wAgency,
        x.wAgencyLeader,
        x.wAgyLdrTitle,
        'SGY'|| IFNULL(x.Wagyldrdistrict,'') AS Wagyldrdistrict,
        x.Wagtclass,
        x.wAgyLdrCode AS wAgyLdrCde,
        x.Spinstartdate,
        x.spinEndDate,
        'SGY'|| IFNULL(curDis.Genericattribute3,'') AS CurDistrict,
        tt.name AS LdrCurRole,
        x.setup
    FROM
        tmp_x_St x,
        ---version 5 fix incorrect DMcode
        tmp_pos agt,
        tmp_pos curDis,
        tmp_pos LdrCurRole,
        cs_title tt
    WHERE
        --x.sgtga12 = agt.name
        MAP(x.AGY_agent, NULL, x.sgtga12, 'SGT'||IFNULL(x.AGY_agent,'')) = agt.name --add version 8
        /* ORIGSQL: decode(x.AGY_agent,null,x.sgtga12,'SGT'||x.AGY_agent) */
        AND 'SGY'||IFNULL(agt.genericattribute1,'') = curDis.Name
        AND curDis.tenantid = 'AIAS'
        AND x.wAgyLdrCode = LdrCurRole.name
        AND LdrCurRole.tenantid = 'AIAS'
        AND LdrCurRole.titleseq = tt.ruleelementownerseq
        AND tt.removedate = :cdt_EndOfTime
        AND tt.effectiveenddate = :cdt_EndOfTime
        AND tt.tenantid = 'AIAS';

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: when others then */
        BEGIN
            /* ORIGSQL: rollback; */
            ROLLBACK;

            gv_error = 'Error [SP_TXA_PIAOR]: ' ||::SQL_ERROR_MESSAGE ||' - '||  /* ORIGSQL: sqlerrm */

            /* RESOLVE: Standard Package call(not converted): 'dbms_utility.format_error_backtrace' not supported, manual conversion required */
            dbms_utility.format_error_backtrace;

            /* ORIGSQL: raise_application_error(-20000,gv_error) */
            -- sapdbmtk: mapped error code -20000 => 10000: (ABS(-20000)%10000)+10000
            SIGNAL SQL_ERROR_CODE 10000 SET MESSAGE_TEXT = :gv_error;
        END;

        /* initialize library variables, if not yet done */
        CALL init_session_global();

        /* ORIGSQL: log('SP_TXA_PIAOR: start') */
        CALL Log('SP_TXA_PIAOR: start');

        --get period startDate, endDate

        /* ORIGSQL: log('gv_CYCLE_DATE: '||gv_CYCLE_DATE) */
        CALL Log('gv_CYCLE_DATE: '||IFNULL(:gv_CYCLE_DATE,''));

        /* ORIGSQL: log('gv_calendarSeq: '||gv_calendarSeq) */
        CALL Log('gv_calendarSeq: '||IFNULL(TO_VARCHAR(:gv_calendarSeq),''));

        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PERIOD' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PERIODTYPE' not found */

        SELECT
            cp.PERIODSEQ,
            cp.startDate,
            cp.endDate
        INTO
            v_periodSeq,
            v_periodStartDate,
            v_periodEndDate
        FROM
            CS_PERIOD cp,
            cs_periodtype pt
        WHERE
            cp.tenantid = 'AIAS'
            AND cp.REMOVEDATE = :cdt_EndOfTime
            AND cp.CALENDARSEQ = :gv_calendarSeq
            AND cp.startdate <= to_char(:gv_CYCLE_DATE,'yyyy-mm-dd')  /* ORIGSQL: to_date(gv_CYCLE_DATE,'yyyy-mm-dd') */
            AND cp.enddate > to_char(:gv_CYCLE_DATE,'yyyy-mm-dd')  /* ORIGSQL: to_date(gv_CYCLE_DATE,'yyyy-mm-dd') */
            AND pt.name = 'month'
            AND pt.periodtypeseq = cp.periodtypeseq;

        /* ORIGSQL: log('v_periodSeq: ' ||v_periodSeq) */
        CALL Log('v_periodSeq: '||IFNULL(TO_VARCHAR(:v_periodSeq),''));

        /* ORIGSQL: log('v_periodStartDate: '||v_periodStartDate) */
        CALL Log('v_periodStartDate: '||IFNULL(TO_VARCHAR(:v_periodStartDate),''));

        /* ORIGSQL: log('v_periodEndDate: '||v_periodEndDate) */
        CALL Log('v_periodEndDate: '||IFNULL(TO_VARCHAR(:v_periodEndDate),''));

        Gv_Periodseq = :v_periodSeq;

        --version 8 init piaor assignment
        /* ORIGSQL: Log('1 Init PIAOR assignment') */
        CALL Log('1 Init PIAOR assignment');

        /* ORIGSQL: AssignmentInitialpartition(v_periodSeq) */
--        CALL EXT.AssignmentInitialpartition(:v_periodSeq);/*Deepan : Uncomment if partition is to be used*/

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: execute immediate ('alter index PIAOR_ASSIGNMENT_PK rebuild parallel nologging'); */
--        EXECUTE IMMEDIATE ('alter index PIAOR_ASSIGNMENT_PK rebuild parallel nologging');/*Deepan : Uncomment if partition is to be used*/

        /* ORIGSQL: log('1 rebuild index PIAOR_ASSIGNMENT_PK done') */
        CALL Log('1 rebuild index PIAOR_ASSIGNMENT_PK done');

        /* ORIGSQL: Log('2 Pre Call init partition PI') */
        CALL Log('2 Pre Call init partition PI');

        /* ORIGSQL: comInitialPartition('PI',v_componentValue_pi,v_periodSeq) */
--        CALL ComInitialpartition('PI', :V_Componentvalue_Pi, :v_periodSeq);/*Deepan : Uncomment if partition is to be used*/

        /* ORIGSQL: Log('3 Pre Call init partition AOR') */
        CALL Log('3 Pre Call init partition AOR');

        /* ORIGSQL: comInitialPartition('AOR',v_componentValue_aor,v_periodSeq) */
--        CALL ComInitialpartition('AOR', :v_componentValue_aor, :v_periodSeq);/*Deepan : Uncomment if partition is to be used*/

        /* ORIGSQL: commit; */
        COMMIT;

        /*Arjun adding this below*/

        /* ORIGSQL: Log('4 Pre Build index Start') */
        CALL Log('4 Pre Build index Start');

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: execute immediate ('alter index SH_QUERY_RESULT_IDX2 rebuild parallel nologging'); */
--        EXECUTE IMMEDIATE ('alter index SH_QUERY_RESULT_IDX2 rebuild parallel nologging');/*Deepan : Uncomment if partition is to be used*/

        /* ORIGSQL: Log('5 Pre Build index Done') */
        CALL Log('5 Pre Build index Done');

        --cut over date to determine what position table need to be used
        --before cutoverdate, use AIA tbl_agent_move
        --after/on cutoverdate, use cs_position
        BEGIN 
            DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
                /* ORIGSQL: WHEN NO_DATA_FOUND Then */
                BEGIN
                    v_cutOverDate = to_char('11/30/2013','mm/dd/yyyy');  /* ORIGSQL: to_date('11/30/2013','mm/dd/yyyy') */
                END;



            SELECT
                IFNULL(refDateValue,to_char('1/1/2000','mm/dd/yyyy'))  /* ORIGSQL: NVL(refDateValue,to_date('1/1/2000','mm/dd/yyyy')) */
            INTO
                v_cutOverDate /* --12/31/2014 */
            FROM
                Sh_Reference
            WHERE
                Refid = 'CUTOVERDATE';

            /* ORIGSQL: Exception WHEN NO_DATA_FOUND Then */
        END;
        BEGIN 
            DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
                /* ORIGSQL: WHEN NO_DATA_FOUND Then */
                BEGIN
                    v_ConstantCrossoverDate = to_char('1/1/2005','mm/dd/yyyy');  /* ORIGSQL: to_date('1/1/2005','mm/dd/yyyy') */
                END;



            SELECT
                IFNULL(refDateValue,to_char('1/1/2000','mm/dd/yyyy'))  /* ORIGSQL: NVL(refDateValue,to_date('1/1/2000','mm/dd/yyyy')) */
            INTO
                v_ConstantCrossoverDate /* --4/1/2006 */
            FROM
                Sh_Reference
            WHERE
                Refid = 'CUTOVERISSUEDATE';

            /* ORIGSQL: Exception WHEN NO_DATA_FOUND Then */
        END;

        --version 8 get FA AGY relation

        /* ORIGSQL: execute immediate 'truncate table AIA_FA_AGY_RELA_TMP'; */
        /* ORIGSQL: truncate table AIA_FA_AGY_RELA_TMP ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_FA_AGY_RELA_TMP';

        /* ORIGSQL: INSERT INTO AIA_FA_AGY_RELA_TMP select ce.payeeid as FA_agent, cg.genericattribute5 as AGY_agent, cg.genericattribute6 as AGY_agency from CS_PAYEE ce, Cs_Gaparticipant cg where ce.payeeseq=cg.payeeseq(...) */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PAYEE' not found */
        /* RESOLVE: Identifier not found: Table/view 'EXT.CS_GAPARTICIPANT' not found */
        INSERT INTO AIA_FA_AGY_RELA_TMP
            SELECT   /* ORIGSQL: select ce.payeeid as FA_agent, cg.genericattribute5 as AGY_agent, cg.genericattribute6 as AGY_agency from CS_PAYEE ce, Cs_Gaparticipant cg where ce.payeeseq=cg.payeeseq and ce.islast=1 and cg.pagenumb(...) */
                ce.payeeid AS FA_agent /* --has prefix example SGT /SGY */, cg.genericattribute5 AS AGY_agent /* --no prefix */, cg.genericattribute6 AS AGY_agency /* --on prefix */
            FROM
                CS_PAYEE ce,
                Cs_Gaparticipant cg
            WHERE
                ce.payeeseq = cg.payeeseq
                AND ce.islast = 1
                AND cg.pagenumber = 0
                AND cg.effectivestartdate >= ce.effectivestartdate
                AND cg.effectiveenddate <= ce.effectiveenddate
                AND ce.removedate = :cdt_EndOfTime
                AND cg.removedate = :cdt_EndOfTime
                AND cg.genericattribute5 IS NOT NULL
                AND ce.effectivestartdate <= TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodEndDate-1 */
                AND ce.effectiveenddate >= TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)));  /* ORIGSQL: v_periodEndDate-1 */

        /* ORIGSQL: log('6 Get FA corresponding AGY old code: ' ||SQL%ROWCOUNT) */
        CALL Log('6 Get FA corresponding AGY old code: '||::ROWCOUNT);  

        /* ORIGSQL: COMMIT; */
        COMMIT;

        /*
            the piaor store proc is seperated into 2 portion,
            1st for policy issue before cutover date, need to look into aia custom table
            2nd for policy issue on/after cutover date, need look into ODS position tables
            */
        /* ORIGSQL: Log('7 Pre Call comConvertAgentRole') */
        CALL Log('7 Pre Call comConvertAgentRole');

        --look into aia customer table begin
        -- comConvertAgentRole(i_periodSeq);

        /* ORIGSQL: comDebugger('SQL Performance','PIAOR_Calculation[SP_TXA_PIAOR]-SQL1 START:' ||SYSDATE) */
        CALL comDebugger('SQL Performance', 'PIAOR_Calculation[SP_TXA_PIAOR]-SQL1 START:'||CURRENT_TIMESTAMP 
        );  /* ORIGSQL: SYSDATE */

        --step1, look into aia custom table with txn.GA12+13+issuedate

        --    Log('30');

        ---------------------------------genericAttribute17 equal O series and Genericdate2 before V_Cutoverdate----------------------------

        --if genericAttribute17=o,mean the writing code equal the commision code?
        --policy issue before cutover date, need to look into aia custom table
        --transaction store the current agent/agency information
        --look the old version from sh_agent_role or cs_position,use the condition for example issue data or compensation date between effstartdate and effenddate
        /* ORIGSQL: execute immediate 'truncate table aia_x'; */
        /* ORIGSQL: truncate table aia_x ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE aia_x';

        /* ORIGSQL: insert / *+ APPEND * / */

        /* ORIGSQL: insert into aia_x select st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.LINENUMBER, st.SUBLINENUMBER, st.EVENTTYPESEQ, st.PIPELINERUNSEQ, st.ORIGINTYPEID, st.COMPENSATIONDATE, st.BILLTOADDR(...) */
        INSERT INTO aia_x
            /* ORIGSQL: select / *+ * / */
            SELECT   /* ORIGSQL: select st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.LINENUMBER, st.SUBLINENUMBER, st.EVENTTYPESEQ, st.PIPELINERUNSEQ, st.ORIGINTYPEID, st.COMPENSATIONDATE, st.BILLTOADDRESSSEQ, st.SHIPTOA(...) */
                st.TENANTID,
                st.SALESTRANSACTIONSEQ,
                st.SALESORDERSEQ,
                st.LINENUMBER,
                st.SUBLINENUMBER,
                st.EVENTTYPESEQ,
                st.PIPELINERUNSEQ,
                st.ORIGINTYPEID,
                st.COMPENSATIONDATE,
                st.BILLTOADDRESSSEQ,
                st.SHIPTOADDRESSSEQ,
                st.OTHERTOADDRESSSEQ,
                st.ISRUNNABLE,
                st.BUSINESSUNITMAP,
                st.ACCOUNTINGDATE,
                st.PRODUCTID,
                st.PRODUCTNAME,
                st.PRODUCTDESCRIPTION,
                st.NUMBEROFUNITS,
                st.UNITVALUE,
                st.UNITTYPEFORUNITVALUE,
                st.PREADJUSTEDVALUE,
                st.UNITTYPEFORPREADJUSTEDVALUE,
                st.VALUE,
                st.UNITTYPEFORVALUE,
                st.NATIVECURRENCY,
                st.NATIVECURRENCYAMOUNT,
                st.DISCOUNTPERCENT,
                st.DISCOUNTTYPE,
                st.PAYMENTTERMS,
                st.PONUMBER,
                st.CHANNEL,
                st.ALTERNATEORDERNUMBER,
                st.DATASOURCE,
                st.REASONSEQ,
                st.COMMENTS,
                st.GENERICATTRIBUTE1,
                st.GENERICATTRIBUTE2,
                st.GENERICATTRIBUTE3,
                st.GENERICATTRIBUTE4,
                st.GENERICATTRIBUTE5,
                st.GENERICATTRIBUTE6,
                st.GENERICATTRIBUTE7,
                st.GENERICATTRIBUTE8,
                st.GENERICATTRIBUTE9,
                st.GENERICATTRIBUTE10,
                st.GENERICATTRIBUTE11,
                st.GENERICATTRIBUTE12,
                st.GENERICATTRIBUTE13,
                st.GENERICATTRIBUTE14,
                st.GENERICATTRIBUTE15,
                st.GENERICATTRIBUTE16,
                st.GENERICATTRIBUTE17,
                st.GENERICATTRIBUTE18,
                st.GENERICATTRIBUTE19,
                st.GENERICATTRIBUTE20,
                st.GENERICATTRIBUTE21,
                st.GENERICATTRIBUTE22,
                st.GENERICATTRIBUTE23,
                st.GENERICATTRIBUTE24,
                st.GENERICATTRIBUTE25,
                st.GENERICATTRIBUTE26,
                st.GENERICATTRIBUTE27,
                st.GENERICATTRIBUTE28,
                st.GENERICATTRIBUTE29,
                st.GENERICATTRIBUTE30,
                st.GENERICATTRIBUTE31,
                st.GENERICATTRIBUTE32,
                st.GENERICNUMBER1,
                st.UNITTYPEFORGENERICNUMBER1,
                st.GENERICNUMBER2,
                st.UNITTYPEFORGENERICNUMBER2,
                st.GENERICNUMBER3,
                st.UNITTYPEFORGENERICNUMBER3,
                st.GENERICNUMBER4,
                st.UNITTYPEFORGENERICNUMBER4,
                st.GENERICNUMBER5,
                st.UNITTYPEFORGENERICNUMBER5,
                st.GENERICNUMBER6,
                st.UNITTYPEFORGENERICNUMBER6,
                st.GENERICDATE1,
                st.GENERICDATE2,
                st.GENERICDATE3,
                st.GENERICDATE4,
                st.GENERICDATE5,
                st.GENERICDATE6,
                st.GENERICBOOLEAN1,
                st.GENERICBOOLEAN2,
                st.GENERICBOOLEAN3,
                st.GENERICBOOLEAN4,
                st.GENERICBOOLEAN5,
                st.GENERICBOOLEAN6,
                st.PROCESSINGUNITSEQ,
                st.MODIFICATIONDATE,
                st.UNITTYPEFORLINENUMBER,
                st.UNITTYPEFORSUBLINENUMBER,
                st.UNITTYPEFORNUMBEROFUNITS,
                st.UNITTYPEFORDISCOUNTPERCENT,
                st.UNITTYPEFORNATIVECURRENCYAMT,
                st.MODELSEQ,
                et.eventtypeid,
                'SGY'||IFNULL(agy.agencyCode,'') AS wAgency,
                'SGY'||IFNULL(ldr.agencyCode,'') AS wAgencyLeader,
                ldr.agentRole AS wAgyLdrTitle,
                Ldr.District AS Wagyldrdistrict,
                agy.Classcode AS Wagtclass /* --add by nelson */, 'SGT'||IFNULL(ldr.AGENTCODE,'') AS wAgyLdrCde,
                /* --add version 8 */
                afy.AGY_agent,
                afy.AGY_agency
            FROM
                /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                EXT.Sh_Agent_Role AS Ldr
            INNER JOIN
                EXT.sh_agent_role AS agy
                ON agy.agencyLeader = ldr.AGENTCODE
            INNER JOIN
                cs_salestransaction AS st
                ON ldr.effectiveEndDate > st.genericdate2
                AND ST.PROCESSINGUNITSEQ = :Gv_Processingunitseq
                AND st.compensationDate >= :v_periodStartDate
                AND st.COMPENSATIONDATE < :v_periodEndDate
                AND st.tenantid = 'AIAS' 
                AND st.businessunitmap IN (1,16) --add by nelson
                /* ORIGSQL: st.businessunitmap in (1,16) */
                AND st.genericAttribute17 = 'O' -- non reassignment transaction
                /* ORIGSQL: st.genericAttribute17 ='O' */
                AND St.Genericdate2 <= :v_cutOverDate
                AND ldr.effectiveStartDate <= st.genericdate2
            LEFT OUTER JOIN
                EXT.AIA_FA_AGY_RELA_TMP AS afy
                ON 'SGT'||IFNULL(st.genericattribute12,'') = afy.FA_agent --version 8 for transfer agent
                /* ORIGSQL: 'SGT'||st.genericattribute12=afy.FA_agent(+) */
                AND agy.effectiveEndDate > st.genericdate2
                AND agy.effectiveStartDate <= st.genericdate2
            INNER JOIN
                cs_eventtype AS et
                ON et.datatypeSeq = st.eventTypeSeq
                AND et.tenantid = 'AIAS' 
                AND Et.Removedate = :cdt_EndOfTime
                AND et.eventTypeId IN ('RYC','API','IFYC','FYC', 'SSCP') --and st.genericattribute12=agy.agentCode
                /* ORIGSQL: et.eventTypeId in ('RYC','API','IFYC','FYC', 'SSCP') */
                AND MAP(afy.AGY_agent, NULL, st.genericattribute12, afy.AGY_agent) = agy.agentCode;--version 8
        --version 8
        /* ORIGSQL: decode(afy.AGY_agent,null,st.genericattribute12,afy.AGY_agent) =agy.agentCode */

        /* ORIGSQL: log('8-1 just for mark : '||sql%rowcount) */
        CALL Log('8-1 just for mark : '||::ROWCOUNT);  

        /* ORIGSQL: commit; */
        COMMIT;

        FOR c_txn AS dbmtk_cursor_54124
        DO
            /* ORIGSQL: v_AgyDistTrxn.Wagency:= */
            v_AgyDistTrxn_FIELD_Wagency = :C_Txn.wAgency;

            /* ORIGSQL: v_AgyDistTrxn.wAgencyLeader:= */
            v_AgyDistTrxn_FIELD_wAgencyLeader = :C_Txn.wAgencyLeader;

            /* ORIGSQL: v_AgyDistTrxn.Wagyldrtitle:= */
            v_AgyDistTrxn_FIELD_Wagyldrtitle = :C_Txn.wAgyLdrTitle;

            /* ORIGSQL: v_AgyDistTrxn.Wagyldrdistrict:= */
            v_AgyDistTrxn_FIELD_Wagyldrdistrict = :C_Txn.Wagyldrdistrict;

            /* ORIGSQL: v_AgyDistTrxn.Wagtclass:= */
            v_AgyDistTrxn_FIELD_Wagtclass = :C_Txn.Wagtclass;

            /* ORIGSQL: v_AgyDistTrxn.CurDistrict:= */
            v_AgyDistTrxn_FIELD_CurDistrict = :C_Txn.CurDistrict;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.LdrCurRole:= */
            v_AgyDistTrxn_FIELD_LdrCurRole = :C_Txn.LdrCurRole;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.wAgyLdrCde:= */
            v_AgyDistTrxn_FIELD_wAgyLdrCde = :C_Txn.wAgyLdrCde;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.setup:= */
            v_AgyDistTrxn_FIELD_setup = :C_Txn.setup;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.txnCode:= */
            v_AgyDistTrxn_FIELD_txnCode = :C_Txn.genericAttribute2;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.Policyissuedate:= */
            v_AgyDistTrxn_FIELD_Policyissuedate = :C_Txn.genericDate2;

            /* ORIGSQL: v_AgyDistTrxn.Salestransactionseq:= */
            v_AgyDistTrxn_FIELD_Salestransactionseq = :C_Txn.Salestransactionseq;

            /* ORIGSQL: v_AgyDistTrxn.SALESORDERSEQ:= */
            v_AgyDistTrxn_FIELD_SALESORDERSEQ = :C_Txn.SALESORDERSEQ;

            /* ORIGSQL: v_AgyDistTrxn.Compensationdate:= */
            v_AgyDistTrxn_FIELD_Compensationdate = :C_Txn.Compensationdate;

            /* ORIGSQL: v_AgyDistTrxn.Commissionagy:= */
            v_AgyDistTrxn_FIELD_Commissionagy = 'SGY'||IFNULL(:C_Txn.Genericattribute13,'');

            /* ORIGSQL: v_AgyDistTrxn.Runningtype:= */
            v_AgyDistTrxn_FIELD_Runningtype = 'Before Cutover - GA17=O';

            /* ORIGSQL: v_AgyDistTrxn.Eventtypeid:= */
            v_AgyDistTrxn_FIELD_Eventtypeid = :C_Txn.eventtypeid;

            /* ORIGSQL: v_AgyDistTrxn.Productname:= */
            v_AgyDistTrxn_FIELD_Productname = :C_Txn.Productname;

            /* ORIGSQL: v_AgyDistTrxn.Businessunitmap:= */
            v_AgyDistTrxn_FIELD_Businessunitmap = :C_Txn.Businessunitmap;

            /* ORIGSQL: v_AgyDistTrxn.Orphanpolicy:= */
            v_AgyDistTrxn_FIELD_Orphanpolicy = :C_Txn.Genericattribute17;

            /* ORIGSQL: v_AgyDistTrxn.Periodseq:= */
            v_AgyDistTrxn_FIELD_Periodseq = :v_periodSeq;

            /* ORIGSQL: v_AgyDistTrxn.Txnclasscode:= */
            v_AgyDistTrxn_FIELD_Txnclasscode = :C_Txn.genericAttribute14;

            IF :C_Txn.Salestransactionseq = 14636699154312497
            THEN
                /* ORIGSQL: Comtransferpiaor_debug(V_Agydisttrxn) */
                 Comtransferpiaor_debug(
                        :v_AgyDistTrxn_FIELD_Salestransactionseq,
                        :v_AgyDistTrxn_FIELD_SALESORDERSEQ,
                        :v_AgyDistTrxn_FIELD_Wagency,
                        :v_AgyDistTrxn_FIELD_wAgencyLeader,
                        :v_AgyDistTrxn_FIELD_Wagyldrtitle,
                        :v_AgyDistTrxn_FIELD_LdrCurRole,
                        :v_AgyDistTrxn_FIELD_Wagyldrdistrict,
                        :v_AgyDistTrxn_FIELD_CurDistrict,
                        :v_AgyDistTrxn_FIELD_Policyissuedate,
                        :v_AgyDistTrxn_FIELD_Compensationdate,
                        :v_AgyDistTrxn_FIELD_Wagtclass,
                        :v_AgyDistTrxn_FIELD_Commissionagy,
                        :v_AgyDistTrxn_FIELD_Runningtype,
                        :v_AgyDistTrxn_FIELD_Eventtypeid,
                        :v_AgyDistTrxn_FIELD_Productname,
                        :v_AgyDistTrxn_FIELD_Businessunitmap,
                        :v_AgyDistTrxn_FIELD_Orphanpolicy,
                        :v_AgyDistTrxn_FIELD_Managerseq,
                        :v_AgyDistTrxn_FIELD_Agyspinoffindicator,
                        :v_AgyDistTrxn_FIELD_Agyspinoffflag,
                        :v_AgyDistTrxn_FIELD_Versioningdate,
                        :v_AgyDistTrxn_FIELD_Periodseq,
                        :v_AgyDistTrxn_FIELD_Spinstartdate,
                        :v_AgyDistTrxn_FIELD_Spindaterange,
                        :v_AgyDistTrxn_FIELD_Txnclasscode,
                        :v_AgyDistTrxn_FIELD_Spinenddate,
                        :v_AgyDistTrxn_FIELD_actualOrphanPolicy,
                        :v_AgyDistTrxn_FIELD_wAgyLdrCde,
                        :v_AgyDistTrxn_FIELD_setup,
                        :v_AgyDistTrxn_FIELD_txnCode
                    );
            ELSE 
                /* ORIGSQL: comTransferPIAOR(V_Agydisttrxn) */
                 Comtransferpiaor(
                        :v_AgyDistTrxn_FIELD_Salestransactionseq,
                        :v_AgyDistTrxn_FIELD_SALESORDERSEQ,
                        :v_AgyDistTrxn_FIELD_Wagency,
                        :v_AgyDistTrxn_FIELD_wAgencyLeader,
                        :v_AgyDistTrxn_FIELD_Wagyldrtitle,
                        :v_AgyDistTrxn_FIELD_LdrCurRole,
                        :v_AgyDistTrxn_FIELD_Wagyldrdistrict,
                        :v_AgyDistTrxn_FIELD_CurDistrict,
                        :v_AgyDistTrxn_FIELD_Policyissuedate,
                        :v_AgyDistTrxn_FIELD_Compensationdate,
                        :v_AgyDistTrxn_FIELD_Wagtclass,
                        :v_AgyDistTrxn_FIELD_Commissionagy,
                        :v_AgyDistTrxn_FIELD_Runningtype,
                        :v_AgyDistTrxn_FIELD_Eventtypeid,
                        :v_AgyDistTrxn_FIELD_Productname,
                        :v_AgyDistTrxn_FIELD_Businessunitmap,
                        :v_AgyDistTrxn_FIELD_Orphanpolicy,
                        :v_AgyDistTrxn_FIELD_Managerseq,
                        :v_AgyDistTrxn_FIELD_Agyspinoffindicator,
                        :v_AgyDistTrxn_FIELD_Agyspinoffflag,
                        :v_AgyDistTrxn_FIELD_Versioningdate,
                        :v_AgyDistTrxn_FIELD_Periodseq,
                        :v_AgyDistTrxn_FIELD_Spinstartdate,
                        :v_AgyDistTrxn_FIELD_Spindaterange,
                        :v_AgyDistTrxn_FIELD_Txnclasscode,
                        :v_AgyDistTrxn_FIELD_Spinenddate,
                        :v_AgyDistTrxn_FIELD_actualOrphanPolicy,
                        :v_AgyDistTrxn_FIELD_wAgyLdrCde,
                        :v_AgyDistTrxn_FIELD_setup,
                        :v_AgyDistTrxn_FIELD_txnCode
                    );
            END IF;
        END FOR;  /* ORIGSQL: end loop; */

        -- end c_txn

        /* ORIGSQL: Log('8 Before Cutover GA17 equal O') */
        CALL Log('8 Before Cutover GA17 equal O');

        /* ORIGSQL: commit; */
        COMMIT;

        --leader who retire or leave company can also get the PIAOR??

        /* ORIGSQL: execute immediate 'truncate table aia_x1'; */
        /* ORIGSQL: truncate table aia_x1 ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE aia_x1';

        /* ORIGSQL: insert / *+ append * / */

        /* ORIGSQL: insert into aia_x1 select st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.LINENUMBER, st.SUBLINENUMBER, st.EVENTTYPESEQ, st.PIPELINERUNSEQ, st.ORIGINTYPEID, st.COMPENSATIONDATE, st.BILLTOADD(...) */
        INSERT INTO aia_x1
            /* ORIGSQL: select / *+ materialize * / */
            SELECT   /* ORIGSQL: select st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.LINENUMBER, st.SUBLINENUMBER, st.EVENTTYPESEQ, st.PIPELINERUNSEQ, st.ORIGINTYPEID, st.COMPENSATIONDATE, st.BILLTOADDRESSSEQ, st.SHIPTOA(...) */
                st.TENANTID,
                st.SALESTRANSACTIONSEQ,
                st.SALESORDERSEQ,
                st.LINENUMBER,
                st.SUBLINENUMBER,
                st.EVENTTYPESEQ,
                st.PIPELINERUNSEQ,
                st.ORIGINTYPEID,
                st.COMPENSATIONDATE,
                st.BILLTOADDRESSSEQ,
                st.SHIPTOADDRESSSEQ,
                st.OTHERTOADDRESSSEQ,
                st.ISRUNNABLE,
                st.BUSINESSUNITMAP,
                st.ACCOUNTINGDATE,
                st.PRODUCTID,
                st.PRODUCTNAME,
                st.PRODUCTDESCRIPTION,
                st.NUMBEROFUNITS,
                st.UNITVALUE,
                st.UNITTYPEFORUNITVALUE,
                st.PREADJUSTEDVALUE,
                st.UNITTYPEFORPREADJUSTEDVALUE,
                st.VALUE,
                st.UNITTYPEFORVALUE,
                st.NATIVECURRENCY,
                st.NATIVECURRENCYAMOUNT,
                st.DISCOUNTPERCENT,
                st.DISCOUNTTYPE,
                st.PAYMENTTERMS,
                st.PONUMBER,
                st.CHANNEL,
                st.ALTERNATEORDERNUMBER,
                st.DATASOURCE,
                st.REASONSEQ,
                st.COMMENTS,
                st.GENERICATTRIBUTE1,
                st.GENERICATTRIBUTE2,
                st.GENERICATTRIBUTE3,
                st.GENERICATTRIBUTE4,
                st.GENERICATTRIBUTE5,
                st.GENERICATTRIBUTE6,
                st.GENERICATTRIBUTE7,
                st.GENERICATTRIBUTE8,
                st.GENERICATTRIBUTE9,
                st.GENERICATTRIBUTE10,
                st.GENERICATTRIBUTE11,
                st.GENERICATTRIBUTE12,
                st.GENERICATTRIBUTE13,
                st.GENERICATTRIBUTE14,
                st.GENERICATTRIBUTE15,
                st.GENERICATTRIBUTE16,
                st.GENERICATTRIBUTE17,
                st.GENERICATTRIBUTE18,
                st.GENERICATTRIBUTE19,
                st.GENERICATTRIBUTE20,
                st.GENERICATTRIBUTE21,
                st.GENERICATTRIBUTE22,
                st.GENERICATTRIBUTE23,
                st.GENERICATTRIBUTE24,
                st.GENERICATTRIBUTE25,
                st.GENERICATTRIBUTE26,
                st.GENERICATTRIBUTE27,
                st.GENERICATTRIBUTE28,
                st.GENERICATTRIBUTE29,
                st.GENERICATTRIBUTE30,
                st.GENERICATTRIBUTE31,
                st.GENERICATTRIBUTE32,
                st.GENERICNUMBER1,
                st.UNITTYPEFORGENERICNUMBER1,
                st.GENERICNUMBER2,
                st.UNITTYPEFORGENERICNUMBER2,
                st.GENERICNUMBER3,
                st.UNITTYPEFORGENERICNUMBER3,
                st.GENERICNUMBER4,
                st.UNITTYPEFORGENERICNUMBER4,
                st.GENERICNUMBER5,
                st.UNITTYPEFORGENERICNUMBER5,
                st.GENERICNUMBER6,
                st.UNITTYPEFORGENERICNUMBER6,
                st.GENERICDATE1,
                st.GENERICDATE2,
                st.GENERICDATE3,
                st.GENERICDATE4,
                st.GENERICDATE5,
                st.GENERICDATE6,
                st.GENERICBOOLEAN1,
                st.GENERICBOOLEAN2,
                st.GENERICBOOLEAN3,
                st.GENERICBOOLEAN4,
                st.GENERICBOOLEAN5,
                st.GENERICBOOLEAN6,
                st.PROCESSINGUNITSEQ,
                st.MODIFICATIONDATE,
                st.UNITTYPEFORLINENUMBER,
                st.UNITTYPEFORSUBLINENUMBER,
                st.UNITTYPEFORNUMBEROFUNITS,
                st.UNITTYPEFORDISCOUNTPERCENT,
                st.UNITTYPEFORNATIVECURRENCYAMT,
                st.MODELSEQ,
                et.eventtypeid,
                'SGY'||IFNULL(agy.agencyCode,'') AS wAgency,
                'SGY'||IFNULL(ldr.agencyCode,'') AS wAgencyLeader,
                ldr.agentRole AS wAgyLdrTitle,
                Ldr.District AS Wagyldrdistrict,
                agy.Classcode AS Wagtclass/* --  add by Nelson */, 'SGT'||IFNULL(ldr.Agentcode,'') AS wAgyLdrCde  /* -- add by Nelson */, /* --add version 8 */
                afy.AGY_agent,
                afy.AGY_agency
            FROM
                /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                EXT.Sh_Agent_Role AS Agy
            INNER JOIN
                EXT.Sh_Agent_Role AS Ldr
                ON Agy.Agencyleader = Ldr.Agentcode
                AND agy.agencycode = ldr.agencycode
            INNER JOIN
                cs_salestransaction AS st
                ON agy.effectiveEndDate > st.genericdate2
                AND ST.PROCESSINGUNITSEQ = :Gv_Processingunitseq
                AND st.tenantid = 'AIAS' 
                AND st.compensationDate >= :v_periodStartDate
                AND st.COMPENSATIONDATE < :v_periodEndDate
                AND st.businessunitmap IN (1,16)--add by nelson
                /* ORIGSQL: st.businessunitmap in (1,16) */
                AND st.genericAttribute17 = 'O' -- non reassignment transaction
                /* ORIGSQL: st.genericAttribute17 ='O' */
                AND St.Genericdate2 <= :v_cutOverDate
                AND agy.effectiveStartDate <= st.genericdate2
                AND Ldr.EffectiveStartdate = (
                    SELECT   /* ORIGSQL: (Select MAX(Effectivestartdate) From Sh_Agent_Role T Where Ldr.Agentcode=T.Agentcode And Ldr.Agencycode=T.Agencycode And T.Effectiveenddate<=St.Genericdate2) */
                        MAX(Effectivestartdate)
                    FROM
                        Sh_Agent_Role T
                    WHERE
                        Ldr.Agentcode = T.Agentcode
                        AND Ldr.Agencycode = T.Agencycode
                        AND T.Effectiveenddate <= St.Genericdate2
                )
                AND Ldr.Effectiveenddate <= St.Genericdate2 -- need to consider the version end date is same as issue date
                /* ORIGSQL: Ldr.Effectiveenddate<=St.Genericdate2 */
            LEFT OUTER JOIN
                EXT.AIA_FA_AGY_RELA_TMP AS afy
                ON 'SGT'||IFNULL(st.genericattribute12,'') = afy.FA_agent --version 8 for transfer agent
                /* ORIGSQL: 'SGT'||st.genericattribute12=afy.FA_agent(+) */
            INNER JOIN
                cs_eventtype AS et
                ON et.datatypeSeq = st.eventTypeSeq
                AND Et.Removedate = :cdt_EndOfTime
                AND et.eventTypeId IN ('RYC','API','IFYC','FYC', 'SSCP') --and st.genericattribute12=agy.agentCode
                /* ORIGSQL: et.eventTypeId in ('RYC','API','IFYC','FYC', 'SSCP') */
                AND MAP(afy.AGY_agent, NULL, st.genericattribute12, afy.AGY_agent) = agy.agentCode --version 8
                /* ORIGSQL: decode(afy.AGY_agent,null,st.genericattribute12,afy.AGY_agent) =agy.agentCode */
                --version 8
            WHERE
                /* RESOLVE: Decide where to place 1 predicate (see below): in ANSI ON-clause or ANSI WHERE-clause? */
                NOT EXISTS
                (
                    SELECT   /* ORIGSQL: (Select 1 From SH_QUERY_RESULT R Where Component In ('PI','AOR') And Periodseq=v_periodSeq And St.Salestransactionseq=R.Genericsequence1) */
                        1
                    FROM
                        SH_QUERY_RESULT R
                    WHERE
                        Component IN ('PI','AOR')
                        AND Periodseq = :v_periodSeq
                        AND St.Salestransactionseq = R.Genericsequence1
                );

        /* ORIGSQL: Log('9-1') */
        CALL Log('9-1');

        /* ORIGSQL: commit; */
        COMMIT;

        FOR c_txn AS dbmtk_cursor_54127
        DO
            /* ORIGSQL: v_AgyDistTrxn.Wagency:= */
            v_AgyDistTrxn_FIELD_Wagency = :C_Txn.wAgency;

            /* ORIGSQL: v_AgyDistTrxn.wAgencyLeader:= */
            v_AgyDistTrxn_FIELD_wAgencyLeader = :C_Txn.wAgencyLeader;

            /* ORIGSQL: v_AgyDistTrxn.Wagyldrtitle:= */
            v_AgyDistTrxn_FIELD_Wagyldrtitle = :C_Txn.wAgyLdrTitle;

            /* ORIGSQL: v_AgyDistTrxn.Wagyldrdistrict:= */
            v_AgyDistTrxn_FIELD_Wagyldrdistrict = :C_Txn.Wagyldrdistrict;

            /* ORIGSQL: v_AgyDistTrxn.Wagtclass:= */
            v_AgyDistTrxn_FIELD_Wagtclass = :C_Txn.Wagtclass;

            /* ORIGSQL: v_AgyDistTrxn.Policyissuedate:= */
            v_AgyDistTrxn_FIELD_Policyissuedate = :C_Txn.genericDate2;

            /* ORIGSQL: v_AgyDistTrxn.CurDistrict:= */
            v_AgyDistTrxn_FIELD_CurDistrict = :C_Txn.CurDistrict;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.LdrCurRole:= */
            v_AgyDistTrxn_FIELD_LdrCurRole = :C_Txn.LdrCurRole;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.wAgyLdrCde:= */
            v_AgyDistTrxn_FIELD_wAgyLdrCde = :C_Txn.wAgyLdrCde;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.setup:= */
            v_AgyDistTrxn_FIELD_setup = :C_Txn.setup;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.txnCode:= */
            v_AgyDistTrxn_FIELD_txnCode = :C_Txn.genericAttribute2;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.Salestransactionseq:= */
            v_AgyDistTrxn_FIELD_Salestransactionseq = :C_Txn.Salestransactionseq;

            /* ORIGSQL: v_AgyDistTrxn.SALESORDERSEQ:= */
            v_AgyDistTrxn_FIELD_SALESORDERSEQ = :C_Txn.SALESORDERSEQ;

            /* ORIGSQL: v_AgyDistTrxn.Compensationdate:= */
            v_AgyDistTrxn_FIELD_Compensationdate = :C_Txn.Compensationdate;

            /* ORIGSQL: v_AgyDistTrxn.Commissionagy:= */
            v_AgyDistTrxn_FIELD_Commissionagy = 'SGY'||IFNULL(:C_Txn.Genericattribute13,'');

            /* ORIGSQL: v_AgyDistTrxn.Runningtype:= */
            v_AgyDistTrxn_FIELD_Runningtype = 'Before Cutover - GA17=O - Ealier district';

            /* ORIGSQL: v_AgyDistTrxn.Eventtypeid:= */
            v_AgyDistTrxn_FIELD_Eventtypeid = :C_Txn.eventtypeid;

            /* ORIGSQL: v_AgyDistTrxn.Productname:= */
            v_AgyDistTrxn_FIELD_Productname = :C_Txn.Productname;

            /* ORIGSQL: v_AgyDistTrxn.Businessunitmap:= */
            v_AgyDistTrxn_FIELD_Businessunitmap = :C_Txn.Businessunitmap;

            /* ORIGSQL: v_AgyDistTrxn.Orphanpolicy:= */
            v_AgyDistTrxn_FIELD_Orphanpolicy = :C_Txn.Genericattribute17;

            /* ORIGSQL: v_AgyDistTrxn.Periodseq:= */
            v_AgyDistTrxn_FIELD_Periodseq = :v_periodSeq;

            /* ORIGSQL: v_AgyDistTrxn.Txnclasscode:= */
            v_AgyDistTrxn_FIELD_Txnclasscode = :C_Txn.genericAttribute14;

            ----DBMS_OUTPUT.put_line('start loop'||v_maxSetNumber);

            IF :C_Txn.Salestransactionseq = 14636699154312497
            THEN
                /* ORIGSQL: Comtransferpiaor_debug(V_Agydisttrxn) */
                CALL Comtransferpiaor_debug(
                        :v_AgyDistTrxn_FIELD_Salestransactionseq,
                        :v_AgyDistTrxn_FIELD_SALESORDERSEQ,
                        :v_AgyDistTrxn_FIELD_Wagency,
                        :v_AgyDistTrxn_FIELD_wAgencyLeader,
                        :v_AgyDistTrxn_FIELD_Wagyldrtitle,
                        :v_AgyDistTrxn_FIELD_LdrCurRole,
                        :v_AgyDistTrxn_FIELD_Wagyldrdistrict,
                        :v_AgyDistTrxn_FIELD_CurDistrict,
                        :v_AgyDistTrxn_FIELD_Policyissuedate,
                        :v_AgyDistTrxn_FIELD_Compensationdate,
                        :v_AgyDistTrxn_FIELD_Wagtclass,
                        :v_AgyDistTrxn_FIELD_Commissionagy,
                        :v_AgyDistTrxn_FIELD_Runningtype,
                        :v_AgyDistTrxn_FIELD_Eventtypeid,
                        :v_AgyDistTrxn_FIELD_Productname,
                        :v_AgyDistTrxn_FIELD_Businessunitmap,
                        :v_AgyDistTrxn_FIELD_Orphanpolicy,
                        :v_AgyDistTrxn_FIELD_Managerseq,
                        :v_AgyDistTrxn_FIELD_Agyspinoffindicator,
                        :v_AgyDistTrxn_FIELD_Agyspinoffflag,
                        :v_AgyDistTrxn_FIELD_Versioningdate,
                        :v_AgyDistTrxn_FIELD_Periodseq,
                        :v_AgyDistTrxn_FIELD_Spinstartdate,
                        :v_AgyDistTrxn_FIELD_Spindaterange,
                        :v_AgyDistTrxn_FIELD_Txnclasscode,
                        :v_AgyDistTrxn_FIELD_Spinenddate,
                        :v_AgyDistTrxn_FIELD_actualOrphanPolicy,
                        :v_AgyDistTrxn_FIELD_wAgyLdrCde,
                        :v_AgyDistTrxn_FIELD_setup,
                        :v_AgyDistTrxn_FIELD_txnCode
                    );
            ELSE 
                /* ORIGSQL: comTransferPIAOR(V_Agydisttrxn) */
                CALL Comtransferpiaor(
                        :v_AgyDistTrxn_FIELD_Salestransactionseq,
                        :v_AgyDistTrxn_FIELD_SALESORDERSEQ,
                        :v_AgyDistTrxn_FIELD_Wagency,
                        :v_AgyDistTrxn_FIELD_wAgencyLeader,
                        :v_AgyDistTrxn_FIELD_Wagyldrtitle,
                        :v_AgyDistTrxn_FIELD_LdrCurRole,
                        :v_AgyDistTrxn_FIELD_Wagyldrdistrict,
                        :v_AgyDistTrxn_FIELD_CurDistrict,
                        :v_AgyDistTrxn_FIELD_Policyissuedate,
                        :v_AgyDistTrxn_FIELD_Compensationdate,
                        :v_AgyDistTrxn_FIELD_Wagtclass,
                        :v_AgyDistTrxn_FIELD_Commissionagy,
                        :v_AgyDistTrxn_FIELD_Runningtype,
                        :v_AgyDistTrxn_FIELD_Eventtypeid,
                        :v_AgyDistTrxn_FIELD_Productname,
                        :v_AgyDistTrxn_FIELD_Businessunitmap,
                        :v_AgyDistTrxn_FIELD_Orphanpolicy,
                        :v_AgyDistTrxn_FIELD_Managerseq,
                        :v_AgyDistTrxn_FIELD_Agyspinoffindicator,
                        :v_AgyDistTrxn_FIELD_Agyspinoffflag,
                        :v_AgyDistTrxn_FIELD_Versioningdate,
                        :v_AgyDistTrxn_FIELD_Periodseq,
                        :v_AgyDistTrxn_FIELD_Spinstartdate,
                        :v_AgyDistTrxn_FIELD_Spindaterange,
                        :v_AgyDistTrxn_FIELD_Txnclasscode,
                        :v_AgyDistTrxn_FIELD_Spinenddate,
                        :v_AgyDistTrxn_FIELD_actualOrphanPolicy,
                        :v_AgyDistTrxn_FIELD_wAgyLdrCde,
                        :v_AgyDistTrxn_FIELD_setup,
                        :v_AgyDistTrxn_FIELD_txnCode
                    );
            END IF;
        END FOR;  /* ORIGSQL: end loop; */

        -- end c_txn

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: Log('9-2 Before Cutover ealier district,GA17 equal O') */
        CALL Log('9-2 Before Cutover ealier district ,GA17 equal O');

        --look into aia customer table ga12+13+ lastest version end

        --            vParName := segmentationutils.segmentname('CS_SalesTransaction', pProcessingUnitSeq, v_periodEndDate);

        ----------------------------------------------------Before cutover date end line-----------------------------------------------------

        ----------------------------------------------------After cutover date start line----------------------------------------------------

        --look into ods table start
        --version 10 long run tuning
        -- for c_txn in (
            -- with x as (
                --  select /*+ parallel(8) materialize */ st.*,et.eventtypeid,
                --    'SGY'||Agy.Genericattribute1 Wagency, -- add by nelson
                --    'SGY'||Ldr.Genericattribute1 Wagencyleader, --add by nelson
                --    Ldr.genericAttribute11 wAgyLdrTitle, --add by nelson
                --    Ldr.Genericattribute3 As Wagyldrdistrict, --add by nelson
                --    Agy.Genericattribute4 As Wagtclass, --add by nelson
                --    Ldr.Genericdate5 As Spinstartdate, --add by nelson
                --    Ldr.genericDate6 as spinEndDate, --add by nelson
                --    Ldr.name wAgyLdrCde, -- add by Nelson
                --  --add version 8
                --  afy.AGY_agent,
                --  afy.AGY_agency
                --    from cs_salestransaction st,
                --         cs_eventtype et,
                --         Cs_Position Agy,
                --         Cs_Position Ldr, -- add by Nelson
                --     AIA_FA_AGY_RELA_TMP afy  --version 8
                --   Where ST.tenantid='AIAS'
                -- and et.tenantid='AIAS'
                -- and Agy.tenantid='AIAS'
                -- and Ldr.tenantid='AIAS'
                -- and ST.PROCESSINGUNITSEQ=GV_PROCESSINGUNITSEQ
                -- AND st.compensationDate>=v_periodStartDate
                -- AND st.COMPENSATIONDATE < v_periodEndDate
                -- and st.businessunitmap in (1,16) --add by nelson
                -- and et.datatypeSeq=st.eventTypeSeq
                -- And Et.Removedate= Cdt_Endoftime
                -- and et.eventTypeId in ('RYC','API','IFYC','FYC', 'SSCP')
                --      --and 'SGT'||st.genericattribute12=agy.name -- add by Nelson
                -- and 'SGT'||st.genericattribute12=afy.FA_agent(+) --version 8 for transfer agent
                -- and decode(afy.AGY_agent,null,'SGT'||st.genericattribute12,afy.AGY_agent)=agy.name --version 8
                -- and agy.removeDate=cdt_EndofTime
                -- and agy.effectiveStartDate<=st.genericDate2
                -- and agy.effectiveEndDate>st.genericDate2
                -- And Ldr.Genericattribute11 In ('FSD','FSAD','AM','FSM') -- add by nelson
                -- and 'SGT'||Agy.Genericattribute2 = Ldr.name  --add by nelson
                -- and Ldr.removeDate=cdt_EndofTime --add by nelson
                -- and Ldr.effectiveStartDate<=st.genericDate2 --add by nelson
                -- and Ldr.effectiveEndDate>st.genericDate2 --add by nelson
                -- And (St.Genericattribute17='O'
                    -- or (st.genericAttribute17<>'O' and ST.Genericattribute14 in ('10','48'))
                --      )
                -- And St.Genericdate2>V_Cutoverdate
                -- and st.genericdate2 <  to_date('12/01/2015', 'mm/dd/yyyy')
            --   )

            /* ORIGSQL: execute immediate 'truncate table tmp_x'; */
            /* ORIGSQL: truncate table tmp_x ; */
            EXECUTE IMMEDIATE 'TRUNCATE TABLE tmp_x';

        /* ORIGSQL: insert / *+ APPEND * / */

        /* ORIGSQL: insert into tmp_x (TENANTID, SALESTRANSACTIONSEQ, SALESORDERSEQ, COMPENSATIONDATE, BUSINESSUNITMAP, PRODUCTNAME, GENERICATTRIBUTE2, GENERICATTRIBUTE12, GENERICATTRIBUTE13, GENERICATTRIBUTE14, GENERICA(...) */
        INSERT INTO tmp_x
            (
                TENANTID,
                SALESTRANSACTIONSEQ,
                SALESORDERSEQ,
                COMPENSATIONDATE,
                BUSINESSUNITMAP,
                PRODUCTNAME,
                GENERICATTRIBUTE2,
                GENERICATTRIBUTE12,
                GENERICATTRIBUTE13,
                GENERICATTRIBUTE14,
                GENERICATTRIBUTE17,
                GENERICDATE2,
                eventtypeid,
                Wagency,
                WagtLeader,
                Wagtclass,
                AGY_agent,
                AGY_agency
            )
            /* ORIGSQL: select / *+ materialize * / */
            SELECT   /* ORIGSQL: select st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.COMPENSATIONDATE, st.BUSINESSUNITMAP, st.PRODUCTNAME, st.GENERICATTRIBUTE2, st.GENERICATTRIBUTE12, st.GENERICATTRIBUTE13, st.GENERICATT(...) */
                st.TENANTID,
                st.SALESTRANSACTIONSEQ,
                st.SALESORDERSEQ,
                st.COMPENSATIONDATE,
                st.BUSINESSUNITMAP,
                st.PRODUCTNAME,
                st.GENERICATTRIBUTE2,
                st.GENERICATTRIBUTE12,
                st.GENERICATTRIBUTE13,
                st.GENERICATTRIBUTE14,
                st.GENERICATTRIBUTE17,
                st.GENERICDATE2,
                et.eventtypeid,
                'SGY'||IFNULL(Agy.Genericattribute1,'') AS Wagency,
                'SGT'||IFNULL(Agy.Genericattribute2,'') AS WagtLeader,
                Agy.Genericattribute4 AS Wagtclass,
                afy.AGY_agent,
                afy.AGY_agency
            FROM
                /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                EXT.Cs_Position AS Agy
            INNER JOIN
                cs_salestransaction AS st
                ON agy.effectiveEndDate > st.genericDate2
                AND ST.tenantid = 'AIAS' 
                AND Agy.tenantid = 'AIAS' 
                AND ST.PROCESSINGUNITSEQ = :Gv_Processingunitseq
                AND st.compensationDate >= :v_periodStartDate
                AND st.COMPENSATIONDATE < :v_periodEndDate
                AND st.businessunitmap IN (1,16)
                AND agy.removeDate = :cdt_EndOfTime
                AND (St.Genericattribute17 = 'O'
                    OR (st.genericAttribute17 <> 'O'
                        AND ST.Genericattribute14 IN ('10','48')))
                AND St.Genericdate2 > :v_cutOverDate
                AND st.genericdate2 < to_char('12/01/2015', 'mm/dd/yyyy')   /* ORIGSQL: to_date('12/01/2015', 'mm/dd/yyyy') */
                AND agy.effectiveStartDate <= st.genericDate2
            LEFT OUTER JOIN
                EXT.AIA_FA_AGY_RELA_TMP AS afy
                ON 'SGT'||IFNULL(st.genericattribute12,'') = afy.FA_agent  /* ORIGSQL: 'SGT'||st.genericattribute12=afy.FA_agent(+) */
            INNER JOIN
                cs_eventtype AS et
                ON et.datatypeSeq = st.eventTypeSeq
                AND et.tenantid = 'AIAS' 
                AND Et.Removedate = :cdt_EndOfTime
                AND et.eventTypeId IN ('RYC','API','IFYC','FYC', 'SSCP')
                AND MAP(afy.AGY_agent, NULL, 'SGT'||IFNULL(st.genericattribute12,''), afy.AGY_agent) = agy.name;  /* ORIGSQL: decode(afy.AGY_agent,null,'SGT'||st.genericattribute12,afy.AGY_agent) */

        /* ORIGSQL: log('10-1 get transaction afer cutover into tmp_x done') */
        CALL Log('10-1 get transaction afer cutover into tmp_x done');

        /* ORIGSQL: commit; */
        COMMIT;

        --get agency learder position version at policy issue date

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into tmp_x x using (SELECT * FROM cs_position where tenantid='AIAS' and removedate=cdt_EndofTime) AS Ldr on (Ldr.name = x.WagtLeader and Ldr.effectiveStartDate<=x.genericDate2 and Ldr.effectiveE(...) */
        MERGE INTO tmp_x AS x 
            USING
            (
                SELECT   /* ORIGSQL: (select * from cs_position where tenantid='AIAS' and removedate=cdt_EndofTime) */
                    *
                FROM
                    cs_position
                WHERE
                    tenantid = 'AIAS'
                    AND removedate = :cdt_EndOfTime
            ) AS Ldr
            ON (Ldr.name = x.WagtLeader
                AND Ldr.effectiveStartDate <= x.genericDate2
                AND Ldr.effectiveEndDate > x.genericDate2
                AND Ldr.Genericattribute11 IN ('FSD','FSAD','AM','FSM')
            )
        WHEN MATCHED THEN
            UPDATE
                SET x.Wagencyleader = 'SGY'||IFNULL(Ldr.Genericattribute1,''),
                x.wAgyLdrTitle = Ldr.genericAttribute11,
                x.Wagyldrdistrict = Ldr.Genericattribute3,
                x.Spinstartdate = Ldr.Genericdate5,
                x.spinEndDate = Ldr.genericDate6,
                x.wAgyLdrCde = Ldr.name;

        /* ORIGSQL: log('10-2 merge agent leader version into tmp_x done') */
        CALL Log('10-2 merge agent leader version into tmp_x done');

        /* ORIGSQL: commit; */
        COMMIT;

        FOR c_txn AS dbmtk_cursor_54130
        DO
            ----DBMS_OUTPUT.put_line('start loop'||v_maxSetNumber);

            /* ORIGSQL: v_AgyDistTrxn.Wagency:= */
            v_AgyDistTrxn_FIELD_Wagency = :c_txn.wAgency;

            /* ORIGSQL: v_AgyDistTrxn.wAgencyLeader:= */
            v_AgyDistTrxn_FIELD_wAgencyLeader = :c_txn.wAgencyLeader;

            /* ORIGSQL: v_AgyDistTrxn.Wagyldrtitle:= */
            v_AgyDistTrxn_FIELD_Wagyldrtitle = :c_txn.wAgyLdrTitle;

            /* ORIGSQL: v_AgyDistTrxn.Wagyldrdistrict:= */
            v_AgyDistTrxn_FIELD_Wagyldrdistrict = :c_txn.Wagyldrdistrict;

            /* ORIGSQL: v_AgyDistTrxn.Wagtclass:= */
            v_AgyDistTrxn_FIELD_Wagtclass = :c_txn.Wagtclass;

            /* ORIGSQL: v_AgyDistTrxn.Policyissuedate:= */
            v_AgyDistTrxn_FIELD_Policyissuedate = :c_txn.genericDate2;

            /* ORIGSQL: v_AgyDistTrxn.CurDistrict:= */
            v_AgyDistTrxn_FIELD_CurDistrict = :c_txn.CurDistrict;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.LdrCurRole:= */
            v_AgyDistTrxn_FIELD_LdrCurRole = :c_txn.LdrCurRole;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.wAgyLdrCde:= */
            v_AgyDistTrxn_FIELD_wAgyLdrCde = :c_txn.wAgyLdrCde;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.setup:= */
            v_AgyDistTrxn_FIELD_setup = :c_txn.setup;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.txnCode:= */
            v_AgyDistTrxn_FIELD_txnCode = :c_txn.genericAttribute2;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.Salestransactionseq:= */
            v_AgyDistTrxn_FIELD_Salestransactionseq = :c_txn.Salestransactionseq;

            /* ORIGSQL: v_AgyDistTrxn.SALESORDERSEQ:= */
            v_AgyDistTrxn_FIELD_SALESORDERSEQ = :c_txn.SALESORDERSEQ;

            /* ORIGSQL: v_AgyDistTrxn.Compensationdate:= */
            v_AgyDistTrxn_FIELD_Compensationdate = :c_txn.Compensationdate;

            /* ORIGSQL: v_AgyDistTrxn.Commissionagy:= */
            v_AgyDistTrxn_FIELD_Commissionagy = 'SGY'||IFNULL(:c_txn.Genericattribute13,'');

            /* ORIGSQL: v_AgyDistTrxn.Runningtype:= */
            v_AgyDistTrxn_FIELD_Runningtype = 'After Cutover - GA17='||IFNULL(:c_txn.genericattribute17,'');

            /* ORIGSQL: v_AgyDistTrxn.Eventtypeid:= */
            v_AgyDistTrxn_FIELD_Eventtypeid = :c_txn.eventtypeid;

            /* ORIGSQL: v_AgyDistTrxn.Productname:= */
            v_AgyDistTrxn_FIELD_Productname = :c_txn.Productname;

            /* ORIGSQL: v_AgyDistTrxn.Businessunitmap:= */
            v_AgyDistTrxn_FIELD_Businessunitmap = :c_txn.Businessunitmap;

            /* ORIGSQL: v_AgyDistTrxn.Orphanpolicy:= */
            v_AgyDistTrxn_FIELD_Orphanpolicy = 'O';

            /* ORIGSQL: v_AgyDistTrxn.actualOrphanPolicy:= */
            v_AgyDistTrxn_FIELD_actualOrphanPolicy = :c_txn.genericAttribute17;

            /* ORIGSQL: v_AgyDistTrxn.Periodseq:= */
            v_AgyDistTrxn_FIELD_Periodseq = :v_periodSeq;

            /* ORIGSQL: v_AgyDistTrxn.Spinstartdate:= */
            v_AgyDistTrxn_FIELD_Spinstartdate = :c_txn.Spinstartdate;

            /* ORIGSQL: v_AgyDistTrxn.Spinenddate:= */
            v_AgyDistTrxn_FIELD_Spinenddate = :c_txn.SpinEnddate;

            /* ORIGSQL: v_AgyDistTrxn.Spindaterange:= */
            v_AgyDistTrxn_FIELD_Spindaterange = CEILING(MONTHS_BETWEEN(:c_txn.genericDate2,:c_txn.Spinstartdate) /12);  /* ORIGSQL: Ceil(Months_Between(c_txn.genericDate2,C_Txn.Spinstartdate)/12) */

            /* ORIGSQL: v_AgyDistTrxn.Txnclasscode:= */
            v_AgyDistTrxn_FIELD_Txnclasscode = :c_txn.Genericattribute14;

            IF :c_txn.Salestransactionseq = 14636699154312497
            THEN
                /* ORIGSQL: Comtransferpiaor_debug(V_Agydisttrxn) */
                CALL Comtransferpiaor_debug(
                        :v_AgyDistTrxn_FIELD_Salestransactionseq,
                        :v_AgyDistTrxn_FIELD_SALESORDERSEQ,
                        :v_AgyDistTrxn_FIELD_Wagency,
                        :v_AgyDistTrxn_FIELD_wAgencyLeader,
                        :v_AgyDistTrxn_FIELD_Wagyldrtitle,
                        :v_AgyDistTrxn_FIELD_LdrCurRole,
                        :v_AgyDistTrxn_FIELD_Wagyldrdistrict,
                        :v_AgyDistTrxn_FIELD_CurDistrict,
                        :v_AgyDistTrxn_FIELD_Policyissuedate,
                        :v_AgyDistTrxn_FIELD_Compensationdate,
                        :v_AgyDistTrxn_FIELD_Wagtclass,
                        :v_AgyDistTrxn_FIELD_Commissionagy,
                        :v_AgyDistTrxn_FIELD_Runningtype,
                        :v_AgyDistTrxn_FIELD_Eventtypeid,
                        :v_AgyDistTrxn_FIELD_Productname,
                        :v_AgyDistTrxn_FIELD_Businessunitmap,
                        :v_AgyDistTrxn_FIELD_Orphanpolicy,
                        :v_AgyDistTrxn_FIELD_Managerseq,
                        :v_AgyDistTrxn_FIELD_Agyspinoffindicator,
                        :v_AgyDistTrxn_FIELD_Agyspinoffflag,
                        :v_AgyDistTrxn_FIELD_Versioningdate,
                        :v_AgyDistTrxn_FIELD_Periodseq,
                        :v_AgyDistTrxn_FIELD_Spinstartdate,
                        :v_AgyDistTrxn_FIELD_Spindaterange,
                        :v_AgyDistTrxn_FIELD_Txnclasscode,
                        :v_AgyDistTrxn_FIELD_Spinenddate,
                        :v_AgyDistTrxn_FIELD_actualOrphanPolicy,
                        :v_AgyDistTrxn_FIELD_wAgyLdrCde,
                        :v_AgyDistTrxn_FIELD_setup,
                        :v_AgyDistTrxn_FIELD_txnCode
                    );
            ELSE 
                /* ORIGSQL: comTransferPIAOR(V_Agydisttrxn) */
                CALL Comtransferpiaor(
                        :v_AgyDistTrxn_FIELD_Salestransactionseq,
                        :v_AgyDistTrxn_FIELD_SALESORDERSEQ,
                        :v_AgyDistTrxn_FIELD_Wagency,
                        :v_AgyDistTrxn_FIELD_wAgencyLeader,
                        :v_AgyDistTrxn_FIELD_Wagyldrtitle,
                        :v_AgyDistTrxn_FIELD_LdrCurRole,
                        :v_AgyDistTrxn_FIELD_Wagyldrdistrict,
                        :v_AgyDistTrxn_FIELD_CurDistrict,
                        :v_AgyDistTrxn_FIELD_Policyissuedate,
                        :v_AgyDistTrxn_FIELD_Compensationdate,
                        :v_AgyDistTrxn_FIELD_Wagtclass,
                        :v_AgyDistTrxn_FIELD_Commissionagy,
                        :v_AgyDistTrxn_FIELD_Runningtype,
                        :v_AgyDistTrxn_FIELD_Eventtypeid,
                        :v_AgyDistTrxn_FIELD_Productname,
                        :v_AgyDistTrxn_FIELD_Businessunitmap,
                        :v_AgyDistTrxn_FIELD_Orphanpolicy,
                        :v_AgyDistTrxn_FIELD_Managerseq,
                        :v_AgyDistTrxn_FIELD_Agyspinoffindicator,
                        :v_AgyDistTrxn_FIELD_Agyspinoffflag,
                        :v_AgyDistTrxn_FIELD_Versioningdate,
                        :v_AgyDistTrxn_FIELD_Periodseq,
                        :v_AgyDistTrxn_FIELD_Spinstartdate,
                        :v_AgyDistTrxn_FIELD_Spindaterange,
                        :v_AgyDistTrxn_FIELD_Txnclasscode,
                        :v_AgyDistTrxn_FIELD_Spinenddate,
                        :v_AgyDistTrxn_FIELD_actualOrphanPolicy,
                        :v_AgyDistTrxn_FIELD_wAgyLdrCde,
                        :v_AgyDistTrxn_FIELD_setup,
                        :v_AgyDistTrxn_FIELD_txnCode
                    );
            END IF;
        END FOR;  /* ORIGSQL: end loop; */

        -- end c_txn

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: Log('10-3 after cutover befor 2015-12-1 GA17 equal O or GA17 not equal O and GA14 in 10,48') */
        CALL Log('10-3 after cutover befor 2015-12-1 GA17 equal O or GA17 not equal O and GA14 in 10,48');

        --look into ods table end

        /* ORIGSQL: comDebugger('SQL Performance','PIAOR_Calculation[SP_TXA_PIAOR]-SQL4 START:' ||SYSDATE) */
        CALL comDebugger('SQL Performance', 'PIAOR_Calculation[SP_TXA_PIAOR]-SQL4 START:'||CURRENT_TIMESTAMP 
        );  /* ORIGSQL: SYSDATE */

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: execute immediate 'truncate table tmp_x_St'; */
        /* ORIGSQL: truncate table tmp_x_St ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE tmp_x_St';

        /* ORIGSQL: Log('11 Truncate tmp_x_St done') */
        CALL Log('11 Truncate tmp_x_St done');

        /* ORIGSQL: insert / *+ APPEND * / */

        /* ORIGSQL: insert into tmp_x_St select st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.LINENUMBER, st.SUBLINENUMBER, st.EVENTTYPESEQ, st.PIPELINERUNSEQ, st.ORIGINTYPEID, st.COMPENSATIONDATE, st.BILLTOA(...) */
        INSERT INTO tmp_x_St
            SELECT   /* ORIGSQL: select st.TENANTID, st.SALESTRANSACTIONSEQ, st.SALESORDERSEQ, st.LINENUMBER, st.SUBLINENUMBER, st.EVENTTYPESEQ, st.PIPELINERUNSEQ, st.ORIGINTYPEID, st.COMPENSATIONDATE, st.BILLTOADDRESSSEQ, st.SHIPTOA(...) */
                st.TENANTID,
                st.SALESTRANSACTIONSEQ,
                st.SALESORDERSEQ,
                st.LINENUMBER,
                st.SUBLINENUMBER,
                st.EVENTTYPESEQ,
                st.PIPELINERUNSEQ,
                st.ORIGINTYPEID,
                st.COMPENSATIONDATE,
                st.BILLTOADDRESSSEQ,
                st.SHIPTOADDRESSSEQ,
                st.OTHERTOADDRESSSEQ,
                st.ISRUNNABLE,
                st.BUSINESSUNITMAP,
                st.ACCOUNTINGDATE,
                st.PRODUCTID,
                st.PRODUCTNAME,
                st.PRODUCTDESCRIPTION,
                st.NUMBEROFUNITS,
                st.UNITVALUE,
                st.UNITTYPEFORUNITVALUE,
                st.PREADJUSTEDVALUE,
                st.UNITTYPEFORPREADJUSTEDVALUE,
                st.VALUE,
                st.UNITTYPEFORVALUE,
                st.NATIVECURRENCY,
                st.NATIVECURRENCYAMOUNT,
                st.DISCOUNTPERCENT,
                st.DISCOUNTTYPE,
                st.PAYMENTTERMS,
                st.PONUMBER,
                st.CHANNEL,
                st.ALTERNATEORDERNUMBER,
                st.DATASOURCE,
                st.REASONSEQ,
                st.COMMENTS,
                st.GENERICATTRIBUTE1,
                st.GENERICATTRIBUTE2,
                st.GENERICATTRIBUTE3,
                st.GENERICATTRIBUTE4,
                st.GENERICATTRIBUTE5,
                st.GENERICATTRIBUTE6,
                st.GENERICATTRIBUTE7,
                st.GENERICATTRIBUTE8,
                st.GENERICATTRIBUTE9,
                st.GENERICATTRIBUTE10,
                st.GENERICATTRIBUTE11,
                st.GENERICATTRIBUTE12,
                st.GENERICATTRIBUTE13,
                st.GENERICATTRIBUTE14,
                st.GENERICATTRIBUTE15,
                st.GENERICATTRIBUTE16,
                st.GENERICATTRIBUTE17,
                st.GENERICATTRIBUTE18,
                st.GENERICATTRIBUTE19,
                st.GENERICATTRIBUTE20,
                st.GENERICATTRIBUTE21,
                st.GENERICATTRIBUTE22,
                st.GENERICATTRIBUTE23,
                st.GENERICATTRIBUTE24,
                st.GENERICATTRIBUTE25,
                st.GENERICATTRIBUTE26,
                st.GENERICATTRIBUTE27,
                st.GENERICATTRIBUTE28,
                st.GENERICATTRIBUTE29,
                st.GENERICATTRIBUTE30,
                st.GENERICATTRIBUTE31,
                st.GENERICATTRIBUTE32,
                st.GENERICNUMBER1,
                st.UNITTYPEFORGENERICNUMBER1,
                st.GENERICNUMBER2,
                st.UNITTYPEFORGENERICNUMBER2,
                st.GENERICNUMBER3,
                st.UNITTYPEFORGENERICNUMBER3,
                st.GENERICNUMBER4,
                st.UNITTYPEFORGENERICNUMBER4,
                st.GENERICNUMBER5,
                st.UNITTYPEFORGENERICNUMBER5,
                st.GENERICNUMBER6,
                st.UNITTYPEFORGENERICNUMBER6,
                st.GENERICDATE1,
                st.GENERICDATE2,
                st.GENERICDATE3,
                st.GENERICDATE4,
                st.GENERICDATE5,
                st.GENERICDATE6,
                st.GENERICBOOLEAN1,
                st.GENERICBOOLEAN2,
                st.GENERICBOOLEAN3,
                st.GENERICBOOLEAN4,
                st.GENERICBOOLEAN5,
                st.GENERICBOOLEAN6,
                st.PROCESSINGUNITSEQ,
                st.MODIFICATIONDATE,
                st.UNITTYPEFORLINENUMBER,
                st.UNITTYPEFORSUBLINENUMBER,
                st.UNITTYPEFORNUMBEROFUNITS,
                st.UNITTYPEFORDISCOUNTPERCENT,
                st.UNITTYPEFORNATIVECURRENCYAMT,
                st.MODELSEQ,
                'SGT'||IFNULL(st.genericattribute12,''),
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                et.eventtypeid,
                NULL,
                /* --add version 8 */
                afy.AGY_agent,
                afy.AGY_agency
            FROM
                cs_Salestransaction st
            INNER JOIN
                cs_eventtype et
                ON et.datatypeseq = st.eventtypeseq
                AND et.removedate = :cdt_EndOfTime
            LEFT OUTER JOIN
                AIA_FA_AGY_RELA_TMP afy
                ON 'SGT'||IFNULL(st.genericattribute12,'') = afy.FA_agent--version 8 
            WHERE
                st.tenantid = 'AIAS'
                AND et.tenantid = 'AIAS'
                AND ST.PROCESSINGUNITSEQ = :Gv_Processingunitseq
                AND st.compensationDate >= :v_periodStartDate
                AND st.COMPENSATIONDATE < :v_periodEndDate
                AND st.businessunitmap IN (1,16)
                AND eventtypeid IN ('RYC',
                    'API',
                    'IFYC',
                    'FYC',
                    'SSCP',
                'ORYC')
                AND NOT EXISTS
                (
                    SELECT   /* ORIGSQL: (Select 1 From SH_QUERY_RESULT R Where Component In ('PI', 'AOR') And Periodseq = v_periodSeq And St.Salestransactionseq = R.Genericsequence1) */
                        1
                    FROM
                        SH_QUERY_RESULT R
                    WHERE
                        Component IN ('PI',
                        'AOR')
                        AND Periodseq = :v_periodSeq
                        AND St.Salestransactionseq = R.Genericsequence1
                )
                AND st.genericdate2 < to_char('12/01/2015','mm/dd/yyyy');  /* ORIGSQL: to_date('12/01/2015','mm/dd/yyyy') */

        /* ORIGSQL: Log('12 Insert tmp_x_St Done') */
        CALL Log('12 Insert tmp_x_St Done');

        /* ORIGSQL: DBMS_STATS.GATHER_TABLE_STATS(ownname => 'AIASEXT', tabname => 'tmp_x_St', method_opt => 'FOR ALL INDEXED COLUMNS SIZE AUTO', estimate_percent => dbms_stats.auto_sample_size, degree => dbms_stats.defa(...) */
        EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| 'AIASEXT'|| '.'|| 'tmp_x_St';

        /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */

        /* ORIGSQL: Log('13 gather tmp_x_St Stats Done') */
        CALL Log('13 gather tmp_x_St Stats Done');

        --search transaction commission agent information,during compensatindate

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into tmp_x_St tgt using (SELECT * FROM cs_position where tenantid='AIAS' and removedate=cdt_EndofTime) AS src on (src.name = decode(tgt.AGY_agent,null,tgt.sgtga12,'SGT'||AGY_agent) and tgt.compe(...) */
        MERGE INTO tmp_x_St AS tgt 
            USING
            (
                SELECT   /* ORIGSQL: (select * from cs_position where tenantid='AIAS' and removedate=cdt_EndofTime) */
                    *
                FROM
                    cs_position
                WHERE
                    tenantid = 'AIAS'
                    AND removedate = :cdt_EndOfTime
            ) AS src
            --on (src.name = tgt.sgtga12 and   --st.genericattribute12
                ON (src.name = MAP(tgt.AGY_agent, NULL, tgt.sgtga12, 'SGT'||IFNULL(AGY_agent,'')) --add version 8
                    /* ORIGSQL: decode(tgt.AGY_agent,null,tgt.sgtga12,'SGT'||AGY_agent) */
                    AND tgt.compensationdate BETWEEN src.effectivestartdate AND TO_DATE(ADD_SECONDS(src.effectiveenddate,(86400*-1)))   /* ORIGSQL: src.effectiveenddate-1 */
                AND src.tenantid = tgt.tenantid)
        WHEN MATCHED THEN
            UPDATE
                SET tgt.wagency = 'SGY'|| IFNULL(src.Genericattribute1,'')
                ,tgt.wagtclass = src.Genericattribute4
                , tgt.sgtga2 = 'SGT'||IFNULL(src.genericattribute2,'');
        --commission agent leader code

        /* ORIGSQL: Log('14 Merge tmp_x_St 1 Done') */
        CALL Log('14 Merge tmp_x_St 1 Done');

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into tmp_x_St tgt using (SELECT * FROM cs_position p where tenantid='AIAS' and removedate=cdt_EndofTime and Genericattribute11 In ('FSD', 'FSAD', 'AM', 'FSM')) AS src on (src.name = tgt.sgtga2 a(...) */
        MERGE INTO tmp_x_St AS tgt 
            USING
            (
                SELECT   /* ORIGSQL: (select * from cs_position p where tenantid='AIAS' and removedate=cdt_EndofTime and Genericattribute11 In ('FSD', 'FSAD', 'AM', 'FSM')) */
                    *
                FROM
                    cs_position p
                WHERE
                    tenantid = 'AIAS'
                    AND removedate = :cdt_EndOfTime
                    AND Genericattribute11 IN ('FSD',
                        'FSAD',
                        'AM',
                    'FSM')
            ) AS src
            ON (src.name = tgt.sgtga2
                AND tgt.compensationdate BETWEEN src.effectivestartdate AND TO_DATE(ADD_SECONDS(src.effectiveenddate,(86400*-1))))  /* ORIGSQL: src.effectiveenddate-1 */
        WHEN MATCHED THEN
            UPDATE
                SET tgt.wagencyleader = 'SGY'|| IFNULL(src.Genericattribute1,'')
                ,tgt.wAgyLdrTitle = src.genericAttribute11
                ,tgt.Wagyldrdistrict = src.Genericattribute3
                ,tgt.Spinstartdate = src.genericdate5
                ,tgt.SpinEnddate = src.genericdate6
                ,tgt.wAgyLdrCode = src.name;

        /* ORIGSQL: Log('15 Merge  tmp_x_St 2 Done') */
        CALL Log('15 Merge  tmp_x_St 2 Done'); 

        /* ORIGSQL: update tmp_x_St x SET setup = nvl((select stp.txtagt from In_Pi_Aor_Setup stp where 'SGT' || to_number(stp.txtagt) = x.wAgyLdrCode and stp.dtecycle = v_periodenddate-1 and stp.txttype in('C', 'D') and(...) */
        UPDATE tmp_x_St x
            SET
            /* ORIGSQL: setup = */
            setup = IFNULL(  /* ORIGSQL: nvl((select stp.txtagt from In_Pi_Aor_Setup stp where 'SGT' || to_number(stp.txtagt) = x.wAgyLdrCode and stp.dtecycle = v_periodenddate-1 and stp.txttype in('C', 'D') and rownum = 1), 'X') */
                (
                    /* RESOLVE: Review semantics in source vs. target DBMS: LIMIT/OFFSET without ORDER BY: consistent results not guaranteed */
                    SELECT   /* ORIGSQL: (select stp.txtagt from In_Pi_Aor_Setup stp where 'SGT'|| to_number(stp.txtagt) = x.wAgyLdrCode and stp.dtecycle = v_periodenddate-1 and stp.txttype in('C', 'D') LIMIT 1) rownum = 1 */
                        stp.txtagt
                    FROM
                        In_Pi_Aor_Setup stp
                    WHERE
                        'SGT'|| IFNULL(TO_VARCHAR(TO_DECIMAL(stp.txtagt,38,18)),'') = x.wAgyLdrCode  /* ORIGSQL: to_number(stp.txtagt) */
                        AND stp.dtecycle = TO_DATE(ADD_SECONDS(:v_periodEndDate,(86400*-1)))   /* ORIGSQL: v_periodenddate-1 */
                        AND stp.txttype IN('C', 'D')
                    LIMIT 1  /* ORIGSQL: rownum = 1 */
                ),
            'X');

        /* ORIGSQL: Log('16 Update tmp_x_St Done') */
        CALL Log('16 Update tmp_x_St Done');

        FOR c_txn AS dbmtk_cursor_54133
        DO
            /* ORIGSQL: v_AgyDistTrxn.Wagency:= */
            v_AgyDistTrxn_FIELD_Wagency = :c_txn.wAgency;

            /* ORIGSQL: v_AgyDistTrxn.wAgencyLeader:= */
            v_AgyDistTrxn_FIELD_wAgencyLeader = :c_txn.wAgencyLeader;

            /* ORIGSQL: v_AgyDistTrxn.Wagyldrtitle:= */
            v_AgyDistTrxn_FIELD_Wagyldrtitle = :c_txn.wAgyLdrTitle;

            /* ORIGSQL: v_AgyDistTrxn.Wagyldrdistrict:= */
            v_AgyDistTrxn_FIELD_Wagyldrdistrict = :c_txn.Wagyldrdistrict;

            /* ORIGSQL: v_AgyDistTrxn.Wagtclass:= */
            v_AgyDistTrxn_FIELD_Wagtclass = :c_txn.Wagtclass;

            /* ORIGSQL: v_AgyDistTrxn.Policyissuedate:= */
            v_AgyDistTrxn_FIELD_Policyissuedate = :c_txn.genericDate2;

            /* ORIGSQL: v_AgyDistTrxn.CurDistrict:= */
            v_AgyDistTrxn_FIELD_CurDistrict = :c_txn.CurDistrict;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.LdrCurRole:= */
            v_AgyDistTrxn_FIELD_LdrCurRole = :c_txn.LdrCurRole;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.wAgyLdrCde:= */
            v_AgyDistTrxn_FIELD_wAgyLdrCde = :c_txn.wAgyLdrCde;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.setup:= */
            v_AgyDistTrxn_FIELD_setup = :c_txn.setup;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.txnCode:= */
            v_AgyDistTrxn_FIELD_txnCode = :c_txn.genericAttribute2;
            -- add by Nelson

            /* ORIGSQL: v_AgyDistTrxn.Salestransactionseq:= */
            v_AgyDistTrxn_FIELD_Salestransactionseq = :c_txn.Salestransactionseq;

            /* ORIGSQL: v_AgyDistTrxn.SALESORDERSEQ:= */
            v_AgyDistTrxn_FIELD_SALESORDERSEQ = :c_txn.SALESORDERSEQ;

            /* ORIGSQL: v_AgyDistTrxn.Compensationdate:= */
            v_AgyDistTrxn_FIELD_Compensationdate = :c_txn.Compensationdate;

            /* ORIGSQL: v_AgyDistTrxn.Commissionagy:= */
            v_AgyDistTrxn_FIELD_Commissionagy = 'SGY'||IFNULL(:c_txn.Genericattribute13,'');

            /* ORIGSQL: v_AgyDistTrxn.Runningtype:= */
            v_AgyDistTrxn_FIELD_Runningtype = 'After Cutover - GA17<>O';

            /* ORIGSQL: v_AgyDistTrxn.Eventtypeid:= */
            v_AgyDistTrxn_FIELD_Eventtypeid = :c_txn.eventtypeid;

            /* ORIGSQL: v_AgyDistTrxn.Productname:= */
            v_AgyDistTrxn_FIELD_Productname = :c_txn.Productname;

            /* ORIGSQL: v_AgyDistTrxn.Businessunitmap:= */
            v_AgyDistTrxn_FIELD_Businessunitmap = :c_txn.Businessunitmap;

            /* ORIGSQL: v_AgyDistTrxn.Orphanpolicy:= */
            v_AgyDistTrxn_FIELD_Orphanpolicy = 'X'|| IFNULL(:c_txn.Genericattribute17,'');

            ---those ga17=o, but not able get version by policy issue date, will trade as ga17<>O

            /* ORIGSQL: v_AgyDistTrxn.actualOrphanPolicy:= */
            v_AgyDistTrxn_FIELD_actualOrphanPolicy = :c_txn.genericAttribute17;

            /* ORIGSQL: v_AgyDistTrxn.Periodseq:= */
            v_AgyDistTrxn_FIELD_Periodseq = :v_periodSeq;

            /* ORIGSQL: v_AgyDistTrxn.Spinstartdate:= */
            v_AgyDistTrxn_FIELD_Spinstartdate = :c_txn.Spinstartdate;

            /* ORIGSQL: v_AgyDistTrxn.Spinenddate:= */
            v_AgyDistTrxn_FIELD_Spinenddate = :c_txn.SpinEnddate;

            /* ORIGSQL: v_AgyDistTrxn.Spindaterange:= */
            v_AgyDistTrxn_FIELD_Spindaterange = CEILING(MONTHS_BETWEEN(:c_txn.genericDate2,:c_txn.Spinstartdate) /12);  /* ORIGSQL: Ceil(Months_Between(c_txn.genericDate2,C_Txn.Spinstartdate)/12) */

            /* ORIGSQL: v_AgyDistTrxn.Txnclasscode:= */
            v_AgyDistTrxn_FIELD_Txnclasscode = :c_txn.Genericattribute14;

            --comTransferPIAOR(V_Agydisttrxn) ;

            IF :c_txn.Salestransactionseq = 14636699154312497
            THEN
                /* ORIGSQL: Comtransferpiaor_debug(V_Agydisttrxn) */
                CALL Comtransferpiaor_debug(
                        :v_AgyDistTrxn_FIELD_Salestransactionseq,
                        :v_AgyDistTrxn_FIELD_SALESORDERSEQ,
                        :v_AgyDistTrxn_FIELD_Wagency,
                        :v_AgyDistTrxn_FIELD_wAgencyLeader,
                        :v_AgyDistTrxn_FIELD_Wagyldrtitle,
                        :v_AgyDistTrxn_FIELD_LdrCurRole,
                        :v_AgyDistTrxn_FIELD_Wagyldrdistrict,
                        :v_AgyDistTrxn_FIELD_CurDistrict,
                        :v_AgyDistTrxn_FIELD_Policyissuedate,
                        :v_AgyDistTrxn_FIELD_Compensationdate,
                        :v_AgyDistTrxn_FIELD_Wagtclass,
                        :v_AgyDistTrxn_FIELD_Commissionagy,
                        :v_AgyDistTrxn_FIELD_Runningtype,
                        :v_AgyDistTrxn_FIELD_Eventtypeid,
                        :v_AgyDistTrxn_FIELD_Productname,
                        :v_AgyDistTrxn_FIELD_Businessunitmap,
                        :v_AgyDistTrxn_FIELD_Orphanpolicy,
                        :v_AgyDistTrxn_FIELD_Managerseq,
                        :v_AgyDistTrxn_FIELD_Agyspinoffindicator,
                        :v_AgyDistTrxn_FIELD_Agyspinoffflag,
                        :v_AgyDistTrxn_FIELD_Versioningdate,
                        :v_AgyDistTrxn_FIELD_Periodseq,
                        :v_AgyDistTrxn_FIELD_Spinstartdate,
                        :v_AgyDistTrxn_FIELD_Spindaterange,
                        :v_AgyDistTrxn_FIELD_Txnclasscode,
                        :v_AgyDistTrxn_FIELD_Spinenddate,
                        :v_AgyDistTrxn_FIELD_actualOrphanPolicy,
                        :v_AgyDistTrxn_FIELD_wAgyLdrCde,
                        :v_AgyDistTrxn_FIELD_setup,
                        :v_AgyDistTrxn_FIELD_txnCode
                    );
            ELSE 
                /* ORIGSQL: comTransferPIAOR(V_Agydisttrxn) */
                CALL Comtransferpiaor(
                        :v_AgyDistTrxn_FIELD_Salestransactionseq,
                        :v_AgyDistTrxn_FIELD_SALESORDERSEQ,
                        :v_AgyDistTrxn_FIELD_Wagency,
                        :v_AgyDistTrxn_FIELD_wAgencyLeader,
                        :v_AgyDistTrxn_FIELD_Wagyldrtitle,
                        :v_AgyDistTrxn_FIELD_LdrCurRole,
                        :v_AgyDistTrxn_FIELD_Wagyldrdistrict,
                        :v_AgyDistTrxn_FIELD_CurDistrict,
                        :v_AgyDistTrxn_FIELD_Policyissuedate,
                        :v_AgyDistTrxn_FIELD_Compensationdate,
                        :v_AgyDistTrxn_FIELD_Wagtclass,
                        :v_AgyDistTrxn_FIELD_Commissionagy,
                        :v_AgyDistTrxn_FIELD_Runningtype,
                        :v_AgyDistTrxn_FIELD_Eventtypeid,
                        :v_AgyDistTrxn_FIELD_Productname,
                        :v_AgyDistTrxn_FIELD_Businessunitmap,
                        :v_AgyDistTrxn_FIELD_Orphanpolicy,
                        :v_AgyDistTrxn_FIELD_Managerseq,
                        :v_AgyDistTrxn_FIELD_Agyspinoffindicator,
                        :v_AgyDistTrxn_FIELD_Agyspinoffflag,
                        :v_AgyDistTrxn_FIELD_Versioningdate,
                        :v_AgyDistTrxn_FIELD_Periodseq,
                        :v_AgyDistTrxn_FIELD_Spinstartdate,
                        :v_AgyDistTrxn_FIELD_Spindaterange,
                        :v_AgyDistTrxn_FIELD_Txnclasscode,
                        :v_AgyDistTrxn_FIELD_Spinenddate,
                        :v_AgyDistTrxn_FIELD_actualOrphanPolicy,
                        :v_AgyDistTrxn_FIELD_wAgyLdrCde,
                        :v_AgyDistTrxn_FIELD_setup,
                        :v_AgyDistTrxn_FIELD_txnCode
                    );
            END IF;
        END FOR;  /* ORIGSQL: end loop; */

        -- end c_txn

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: Log('17 befor 2015-12-1 but not exists befor step') */
        CALL Log('17 befor 2015-12-1 but not exists befor step');

        /* ORIGSQL: comDebugger('SQL Performance','PIAOR_Calculation[SP_TXA_PIAOR]-SQL5 START:' ||SYSDATE) */
        CALL comDebugger('SQL Performance', 'PIAOR_Calculation[SP_TXA_PIAOR]-SQL5 START:'||CURRENT_TIMESTAMP 
        );  /* ORIGSQL: SYSDATE */

        /* ORIGSQL: Log('18') */
        CALL Log('18');

        --REMOVE NA district result
        /* ORIGSQL: Delete / *+ * / */

        /* ORIGSQL: Delete From SH_QUERY_RESULT where component in (V_Componentvalue_Pi,V_Componentvalue_Aor) And Periodseq=v_periodSeq and genericAttribute4='NA'; */
        DELETE
        FROM
            SH_QUERY_RESULT
        WHERE
            component IN (:V_Componentvalue_Pi,:v_componentValue_aor)
            AND Periodseq = :v_periodSeq
            AND genericAttribute4 = 'NA';

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: Log('18') */
        CALL Log('18');

        --Added by Suresh 20180129

        --genericDate1,    --policyIssueDate
        --genericDate2,     --compensationDate

        /* ORIGSQL: EXECUTE IMMEDIATE 'truncate table SH_QUERY_RESULT_TMP35'; */
        /* ORIGSQL: truncate table SH_QUERY_RESULT_TMP35 ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE SH_QUERY_RESULT_TMP35';

        /* ORIGSQL: Insert / *+ APPEND * / */

        /* ORIGSQL: Insert into SH_QUERY_RESULT_TMP35 Select R.Component,r.periodseq,R.Genericsequence1, Cgp.Genericdate13 As Crossoverstartdate, Cgp.Genericdate14 As Crossoverenddate, Cgp.Genericdate15 As Demotionstartd(...) */
        INSERT INTO SH_QUERY_RESULT_TMP35
            /* ORIGSQL: Select / *+ index(R SH_QUERY_RESULT_IDX) index(CP CS_POSITION_AK1) index(CGP CS_GAPOSITION_PK)* / */
            SELECT   /* ORIGSQL: Select R.Component,r.periodseq,R.Genericsequence1, Cgp.Genericdate13 As Crossoverstartdate, Cgp.Genericdate14 As Crossoverenddate, Cgp.Genericdate15 As Demotionstartdate, Cgp.Genericdate16 As Demotion(...) */
                R.Component,
                r.periodseq,
                R.Genericsequence1,
                Cgp.Genericdate13 AS Crossoverstartdate,
                Cgp.Genericdate14 AS Crossoverenddate,
                Cgp.Genericdate15 AS Demotionstartdate,
                Cgp.Genericdate16 AS DemotionEndDate,
                CASE
                    /* ----crossover date chking */
                    /* --case#1 */
                        WHEN Cgp.Genericdate13 IS NOT NULL
                        AND Cgp.Genericdate14 IS NULL
                        AND Cgp.Genericdate15 IS NULL
                        AND Cgp.Genericdate16 IS NULL
                        THEN
                        CASE
                            WHEN R.Genericdate1 < Cgp.Genericdate13
                            AND R.genericDate2 >= Cgp.Genericdate13
                            THEN REPLACE(R.Genericattribute5,'Direct Team','Crossover')
                            ELSE R.Genericattribute5
                        END
                        /* --case#2 */
                            WHEN Cgp.Genericdate13 IS NOT NULL
                            AND Cgp.Genericdate14 IS NOT NULL
                            AND Cgp.Genericdate15 IS NULL
                            AND Cgp.Genericdate16 IS NULL
                            THEN
                            CASE
                                WHEN R.Genericdate1 < Cgp.Genericdate13
                                AND R.genericDate2 >= Cgp.Genericdate13
                                AND R.Genericdate2 < Cgp.Genericdate14
                                THEN REPLACE(R.Genericattribute5,'Direct Team','Crossover')
                                ELSE R.Genericattribute5
                            END
                            /* --demotion date chking */
                            /* --case#3 */
                                WHEN Cgp.Genericdate13 IS NULL
                                AND Cgp.Genericdate14 IS NULL
                                AND Cgp.Genericdate15 IS NOT NULL
                                AND Cgp.Genericdate16 IS NULL
                                THEN
                                CASE
                                    WHEN R.Genericdate1 < Cgp.Genericdate15
                                    AND R.Genericdate2 >= Cgp.Genericdate15
                                    THEN REPLACE(R.Genericattribute5,'AOR - Direct Team','AOR - Crossover')
                                    ELSE R.Genericattribute5
                                END
                                /* --case#4 */
                                    WHEN Cgp.Genericdate13 IS NULL
                                    AND Cgp.Genericdate14 IS NULL
                                    AND Cgp.Genericdate15 IS NOT NULL
                                    AND Cgp.Genericdate16 IS NOT NULL
                                    THEN
                                    CASE
                                        WHEN R.Genericdate1 < Cgp.Genericdate15
                                        AND R.Genericdate2 >= Cgp.Genericdate15
                                        AND R.Genericdate2 < Cgp.Genericdate16
                                        THEN REPLACE(R.Genericattribute5,'AOR - Direct Team','AOR - Crossover')
                                        ELSE R.Genericattribute5
                                    END
                                    /* --case#5 */
                                        WHEN Cgp.Genericdate13 IS NOT NULL
                                        AND Cgp.Genericdate14 IS NULL
                                        AND Cgp.Genericdate15 IS NOT NULL
                                        AND Cgp.Genericdate16 IS NULL
                                        AND Cgp.Genericdate13 < Cgp.Genericdate15
                                        THEN
                                        CASE
                                            WHEN R.Genericdate1 < Cgp.Genericdate13
                                            AND R.Genericdate2 >= Cgp.Genericdate15
                                            THEN REPLACE(R.Genericattribute5,'Direct Team','Crossover')
                                            WHEN R.Genericdate1 >= Cgp.Genericdate13
                                            AND R.Genericdate1 < Cgp.Genericdate15
                                            AND R.Genericdate2 >= Cgp.Genericdate15
                                            THEN REPLACE(R.Genericattribute5,'AOR - Direct Team','AOR - Crossover')
                                            ELSE R.Genericattribute5
                                        END
                                        /* --case#6 */
                                            WHEN Cgp.Genericdate13 IS NOT NULL
                                            AND Cgp.Genericdate14 IS NULL
                                            AND Cgp.Genericdate15 IS NOT NULL
                                            AND Cgp.Genericdate16 IS NULL
                                            AND Cgp.Genericdate13 > Cgp.Genericdate15
                                            THEN
                                            CASE
                                                WHEN R.Genericdate1 < Cgp.Genericdate15
                                                AND R.Genericdate2 >= Cgp.Genericdate15
                                                AND R.Genericdate2 < Cgp.Genericdate13
                                                THEN REPLACE(R.Genericattribute5,'AOR - Direct Team','AOR - Crossover')
                                                WHEN R.Genericdate1 < Cgp.Genericdate15
                                                AND R.Genericdate2 >= Cgp.Genericdate13
                                                THEN REPLACE(R.Genericattribute5,'Direct Team','Crossover')
                                                WHEN R.Genericdate1 >= Cgp.Genericdate15
                                                AND R.Genericdate1 < Cgp.Genericdate13
                                                AND R.Genericdate2 >= Cgp.Genericdate13
                                                THEN REPLACE(R.Genericattribute5,'Direct Team','Crossover')
                                                ELSE R.Genericattribute5
                                            END
                                            /* --case#7 */
                                                WHEN Cgp.Genericdate13 IS NOT NULL
                                                AND Cgp.Genericdate14 IS NULL
                                                AND Cgp.Genericdate15 IS NOT NULL
                                                AND Cgp.Genericdate16 IS NULL
                                                AND Cgp.Genericdate13 = Cgp.Genericdate15
                                                THEN REPLACE(R.Genericattribute5,'Direct Team','Crossover')
                                                ELSE R.Genericattribute5
                                            END
                                            AS rule
                                        FROM
                                            SH_QUERY_RESULT R,
                                            cs_position cp,
                                            cs_gaposition cgp
                                        WHERE
                                            cp.tenantid = 'AIAS'
                                            AND cgp.tenantid = 'AIAS'
                                            AND R.Component IN ('PI','AOR')
                                            AND r.periodseq = :v_periodSeq
                                            AND r.genericAttribute5 IN ('PI - Direct Team', 'AOR - Direct Team')
                                            AND R.Genericattribute1 = Cp.Name
                                            AND Cp.Removedate = :cdt_EndOfTime
                                            AND Cp.Effectivestartdate <= R.Genericdate2
                                            AND Cp.Effectiveenddate > R.Genericdate2
                                            AND Cp.Ruleelementownerseq = Cgp.Ruleelementownerseq
                                            AND Cgp.Removedate = :cdt_EndOfTime
                                            AND Cgp.Effectivestartdate <= R.Genericdate2
                                            AND Cgp.Effectiveenddate > R.Genericdate2
                                            AND Cgp.Pagenumber = 0
                                            AND (Cgp.Genericdate13 IS NOT NULL
                                                OR Cgp.Genericdate14 IS NOT NULL
                                                OR Cgp.Genericdate15 IS NOT NULL
                                            OR Cgp.Genericdate16 IS NOT NULL)
                                            AND r.genericAttribute11 <> 'XO';--if xo, means the GA17=0 trxn cant find a matched dirstrict by policy issue date in both aia and tc table
        /* ORIGSQL: COMMIT; */
        COMMIT;

        /* ORIGSQL: Log('19 match Crossover data') */
        CALL Log('19 match Crossover data');

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into SH_QUERY_RESULT m Using (SELECT * FROM SH_QUERY_RESULT_TMP35) AS T On (T.Genericsequence1=M.Genericsequence1 And T.Component=M.Component and t.periodseq=m.periodseq) When Matched Then Updat(...) */
        MERGE INTO SH_QUERY_RESULT AS m
            USING
            (
                /* ORIGSQL: select / *+ * / */
                SELECT   /* ORIGSQL: (select * from SH_QUERY_RESULT_TMP35) */
                    *
                FROM
                    SH_QUERY_RESULT_TMP35
            ) AS T
            ON (T.Genericsequence1 = M.Genericsequence1 --salestransactionSeq
                AND T.Component = M.Component
                AND t.periodseq = m.periodseq
            )
        WHEN MATCHED THEN
            UPDATE
                SET M.Genericattribute5 = T.Rule,
                M.Genericdate3 = T.Crossoverstartdate,
                M.Genericdate4 = T.Crossoverenddate,
                M.Genericdate5 = T.Demotionstartdate,
                m.Genericdate6 = T.DemotionEnddate;
        --end by Suresh 20180129

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: Log('20 match Crossover data') */
        CALL Log('20 match Crossover data');

        -- comDebugger('piaor','merge1 done!!'||i_periodSeq);

        --when completed data gathering from kinds of scenario, then insert the assignment data to cs_txnassignment
        --delete pi/aor assignment data which not same as standard agency
        /*** the deletion is deined here, because assignment will be clean up by comCleanAssignment()
         delete /-+parallel(Ta,8)-/ from
          cs_transactionAssignment ta
          where 1=1
         And (Ta.Genericattribute4 Like '%PI%' Or Ta.Genericattribute4 Like '%AOR%')
         And Ta.Genericattribute4 Not Like 'NADOR%'
         and ta.setnumber>2
         AND ta.genericAttribute9 IS NULL
         and ta.compensationDate>=v_periodStartDate
         And Ta.Compensationdate<V_Periodenddate
          --ensure only delete those trxn is in current pu
         And Exists (
              Select 1 From Cs_Salestransaction
              Where Salestransactionseq=Ta.Salestransactionseq
             and processingUnitSeq=gv_processingUnitSeq
          )
         And Not Exists (Select 1 From Cs_Salestransaction
              Where Salestransactionseq=Ta.Salestransactionseq
             and processingUnitSeq=gv_processingUnitSeq
             AND EVENTTYPESEQ=GV_HRYC
          );
        
          */

        -- commit;

        --version 8  comment,beause table PIAOR_ASSIGNMENT will truncate when the procedure start
        --reset ga4 of standard agency assignment which is shared for pi or aor
        --Log('36');
        --Update /* parallel(Ta,8)*/Cs_Transactionassignment Ta
        /*
          set ta.genericattribute4=decode(substrc(ta.genericattribute4,1,5),'NADOR','NADOR',''),
        ta.genericAttribute5=null,
        ta.genericAttribute6=null,
        ta.genericAttribute7=null,
        ta.genericAttribute8=null,
        ta.genericAttribute10=null
        Where  (Ta.Genericattribute4 Like '%PI%' Or Ta.Genericattribute4 Like '%AOR%')
        and ta.positionname like 'SGY%'
        and ta.genericAttribute9 IS NULL
        and ta.compensationDate>=v_periodStartDate
        and ta.compensationDate<v_periodEndDate
        And Exists (
            Select 1 From Cs_Salestransaction
            Where Salestransactionseq=Ta.Salestransactionseq
            And Processingunitseq=Gv_Processingunitseq
        )
        And Not Exists (Select 1 From Cs_Salestransaction
            Where Salestransactionseq=Ta.Salestransactionseq
            And Eventtypeseq=GV_HRYC
        );
        
        
        commit;
        Log('36');
          */

        /*version 8 comment
        Log('37');
        --delete from sh_sequence where seqtype in ('AOR_TRXNSEQ', 'PI_TRXNSEQ');
        execute immediate 'truncate table sh_sequence';  --add by nelson for performance tune
        
        COMMIT;
        Log('37');
          */

        --different PI agency

        --  Log('38');

        --insert ALL
        --WHEN standardAgency<>positionName THEN
        --INTO Cs_Transactionassignment
        --  (Tenantid,salesTransactionSeq,salesOrderSeq,setNumber,compensationDate,positionName,payeeId,genericAttribute4,genericAttribute5,genericAttribute6,
        --  genericAttribute7,genericAttribute8,processingunitseq)
        --  VALUES ('AIAS',salesTransactionSeq, salesOrderSeq, setNumber,compensationDate, positionName,null,assignmentType,ruleIndicator,
        --  wAgency,wAgyLdrTitle,wAgyLdrDistrict,processingunitseq)
        --  WHEN standardAgency=positionName THEN
        --  INTO sh_sequence (businessSeq,seqType) values (salesTransactionSeq,'PI_TRXNSEQ')
        --  select /*+ INDEX(ta.AIAS_TXNASSIGN_PNAME) parallel(8) */ r.genericSequence1 as salesTransactionSeq,r.genericSequence2 as salesOrderSeq,r.genericNumber1 as setNumber,
        --         r.genericDate1 as policyIssuedDate,r.genericDate2 as compensationDate,
        --         r.genericAttribute1 as positionName ,r.genericAttribute2 as wAgyLdrTitle,
        --         r.genericAttribute3 as wAgency, r.genericAttribute4 as wAgyLdrDistrict,
        --         r.genericAttribute5 as ruleIndicator, r.genericAttribute6 as businessUnitMap,
        --         r.component as assignmentType,nvl(ta.positionName,'#') standardAgency,ta.processingunitseq as processingunitseq
        --    from sh_query_result r,Cs_Transactionassignment  ta
        --   Where ta.tenantid='AIAS' and R.Component ='PI'
        -- and r.periodseq=gv_periodseq
        -- and ta.salestransactionseq=r.genericSequence1
        -- and Ta.Setnumber=1
        -- aND r.genericAttribute11 <>'XO';
        -- -- and ta.positionName <> r.genericAttribute1
        --  commit;

        --  version 8 select PI data from SH_QUERY_RESULT

        /* ORIGSQL: insert INTO PIAOR_ASSIGNMENT (Tenantid, salesTransactionSeq, salesOrderSeq, setNumber, compensationDate, positionName, payeeId, genericAttribute4, genericAttribute5, genericAttribute6, genericAttribut(...) */
        INSERT INTO PIAOR_ASSIGNMENT
            (
                Tenantid,
                salesTransactionSeq,
                salesOrderSeq,
                setNumber,
                compensationDate,
                positionName,
                payeeId,
                genericAttribute4,
                genericAttribute5,
                genericAttribute6,
                genericAttribute7,
                genericAttribute8,
                processingunitseq
            )
            SELECT   /* ORIGSQL: select 'AIAS', r.genericSequence1 as salesTransactionSeq, r.genericSequence2 as salesOrderSeq, r.genericNumber1 as setNumber, r.genericDate2 as compensationDate, r.genericAttribute1 as positionName, n(...) */
                'AIAS',
                r.genericSequence1 AS salesTransactionSeq,
                r.genericSequence2 AS salesOrderSeq,
                r.genericNumber1 AS setNumber,
                r.genericDate2 AS compensationDate,
                r.genericAttribute1 AS positionName,
                NULL,
                r.component AS assignmentType,
                r.genericAttribute5 AS ruleIndicator,
                r.genericAttribute3 AS wAgency,
                r.genericAttribute2 AS wAgyLdrTitle,
                r.genericAttribute4 AS wAgyLdrDistrict,
                :Gv_Processingunitseq AS processingunitseq
            FROM
                SH_QUERY_RESULT r
            WHERE
                R.Component = 'PI'
                AND r.periodseq = :v_periodSeq
                AND r.genericAttribute11 <> 'XO';

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: Log('21 update PI assignment') */
        CALL Log('21 update PI assignment');

        /* version 8 comment,due to assiment rewirt and insert into table PIAOR_ASSIGNMENT
            DBMS_STATS.GATHER_TABLE_STATS (
                    ownname          => 'AIASEXT',
                    tabname          => 'SH_SEQUENCE',
                    method_opt       => 'FOR ALL INDEXED COLUMNS SIZE AUTO',
                    estimate_percent => dbms_stats.auto_sample_size,
                    degree           => dbms_stats.default_degree,
                    cascade          => true
              );
        
        Log('39');
        */

        --version 8 comment,due to assiment rewirt and insert into table PIAOR_ASSIGNMENT
        -- Merge /*+ INDEX(ta AIAS_TXNASSIGN_PNAME) */ Into Cs_Transactionassignment Ta
        -- Using
        -- (Select Salestransactionseq, Salesorderseq, Setnumber,Policyissueddate,
            -- Compensationdate,Positionname,Wagyldrtitle,Wagency,Wagyldrdistrict,Ruleindicator,Businessunitmap
            -- from (SELECT /*+ LEADING(r,s) index(R SH_QUERY_RESULT_IDX2) */s.businessSeq businessSeq,
                --              r.genericSequence1 as salesTransactionSeq,r.genericSequence2 as salesOrderSeq,
                --              r.genericNumber1 as setNumber,
                --              r.genericDate1 as policyIssuedDate,r.genericDate2 as compensationDate,
                --              r.genericAttribute1 as positionName ,r.genericAttribute2 as wAgyLdrTitle,
                --              r.genericAttribute3 as wAgency, r.genericAttribute4 as wAgyLdrDistrict,
                --              R.Genericattribute5 As Ruleindicator, R.Genericattribute6 As Businessunitmap
                --       from sh_query_result r left join sh_sequence s
                --         on s.businessSeq=r.genericSequence1
                -- and s.Seqtype='PI_TRXNSEQ'
                --      Where R.Component ='PI'
                -- And R.Periodseq=Gv_Periodseq
                -- And R.Genericattribute11 <>'XO'
            --      )R
            --      where r.businessSeq IS NOT NULL
        -- ) t
        -- on
        -- ( t.salestransactionSeq=ta.salestransactionseq
            -- and t.positionName=ta.positionName
            -- and ta.setnumber=1
        -- )
        -- When Matched Then Update Set
        -- ta.genericAttribute4=decode( nvl(ta.genericAttribute4,'Standard'),'Standard', 'Standard_PI', ta.genericAttribute4||'_PI'),
        -- ta.genericAttribute5=t.ruleIndicator,
        -- ta.genericAttribute6=t.wAgency,
        -- ta.genericAttribute7=t.wAgyLdrTitle,
        -- ta.genericAttribute8=t.wAgyLdrDistrict
        -- ;
        --
        --
        -- commit;
        --
        -- Log('39');

        --  Log('40');

        --deal with AOR data ,and if exists PI at the same time
        --version 8 comment ,due to assiment rewirt and insert into table PIAOR_ASSIGNMENT
        -- insert /*+ append */ ALL
        -- WHEN standardAgency<>positionName and PIPositionName<>positionName THEN
        -- INTO Cs_Transactionassignment
        --   (tenantid,salesTransactionSeq,salesOrderSeq,setNumber,compensationDate,positionName,payeeId,genericAttribute4,genericAttribute10,genericAttribute6,
        --   genericAttribute7,genericAttribute8,PROCESSINGUNITSEQ)
        --   VALUES ('AIAS',salesTransactionSeq, salesOrderSeq, setNumber,compensationDate, positionName,null,assignmentType,ruleIndicator,
        --   wAgency,wAgyLdrTitle,wAgyLdrDistrict,PROCESSINGUNITSEQ)
        --   WHEN standardAgency=positionName or PIPositionName=positionName THEN
        --   INTO sh_sequence (businessSeq,seqType) values (salesTransactionSeq,'AOR_TRXNSEQ')
        --   Select /*+  INDEX(ta AIAS_TXNASSIGN_PNAME)  PARALLEL(r,8) */
        --   R.Genericsequence1 As Salestransactionseq,R.Genericsequence2 As Salesorderseq,
        --    r.genericnumber1 as setNumber,
        --   r.genericDate1 as policyIssuedDate,r.genericDate2 as compensationDate,
        --  r.genericAttribute1 as positionName ,r.genericAttribute2 as wAgyLdrTitle,
        --  r.genericAttribute3 as wAgency, r.genericAttribute4 as wAgyLdrDistrict,
        --  r.genericAttribute5 as ruleIndicator, r.genericAttribute6 as businessUnitMap,
        --  r.component as assignmentType,nvl(ta.positionName,'#') standardAgency, nvl(rpi.genericattribute1,'#') as PIPositionName,TA.PROCESSINGUNITSEQ as PROCESSINGUNITSEQ
        --   from sh_query_result r,Cs_Transactionassignment ta, sh_query_result rpi
        --   Where ta.tenantid='AIAS' and ta.PROCESSINGUNITSEQ=GV_PROCESSINGUNITSEQ and R.Component ='AOR'
        -- And R.Periodseq=Gv_Periodseq
        -- AND r.genericAttribute11 <>'XO'
        -- and ta.salestransactionseq=r.genericSequence1
        -- and ta.setnumber=1
        -- and rpi.component(+)='PI'
        -- And R.Genericsequence1=Rpi.Genericsequence1(+)
        -- and rpi.periodseq(+)=gv_periodseq ;

        --FOR RERUN
        /* ORIGSQL: DELETE FROM SH_SEQUENCE WHERE seqType='AOR_TRXNSEQ'; */
        DELETE
        FROM
            SH_SEQUENCE
        WHERE
            seqType = 'AOR_TRXNSEQ';

        /* ORIGSQL: insert / *+ append * / */
        /* RESOLVE: Syntax not supported in target DBMS: INSERT-WHEN-INTO; not supported in target DBMS, manual conversion required */
    /*Deepan : Commenting out Insert ALL statement and replacing with HANA equivalent below*/
    
    --     insert ALL
    --         WHEN PIPositionName <> positionName THEN
    --     INTO PIAOR_ASSIGNMENT
    --         (tenantid,salesTransactionSeq,salesOrderSeq,setNumber,compensationDate,positionName,payeeId,genericAttribute4,genericAttribute10,genericAttribute6,
    --         genericAttribute7,genericAttribute8,PROCESSINGUNITSEQ)
    --     VALUES ('AIAS',salesTransactionSeq, salesOrderSeq, setNumber,compensationDate, positionName,NULL,assignmentType,ruleIndicator,
    --     wAgency,wAgyLdrTitle,wAgyLdrDistrict,PROCESSINGUNITSEQ)
    --     WHEN PIPositionName = positionName THEN
    -- INTO SH_SEQUENCE (businessSeq,seqType) VALUES (salesTransactionSeq,'AOR_TRXNSEQ')
    --     SELECT   /* ORIGSQL: Select 'AIAS' as tenantid, R.Genericsequence1 As Salestransactionseq, R.Genericsequence2 As Salesorderseq, r.genericnumber1 as setNumber, r.genericDate1 as policyIssuedDate, r.genericDate2 as compensa(...) */
    --         'AIAS' AS tenantid,
    --         R.Genericsequence1 AS Salestransactionseq,
    --         R.Genericsequence2 AS Salesorderseq,
    --         r.genericnumber1 AS setNumber,
    --         r.genericDate1 AS policyIssuedDate,
    --         r.genericDate2 AS compensationDate,
    --         r.genericAttribute1 AS positionName,
    --         r.genericAttribute2 AS wAgyLdrTitle,
    --         r.genericAttribute3 AS wAgency,
    --         r.genericAttribute4 AS wAgyLdrDistrict,
    --         r.genericAttribute5 AS ruleIndicator,
    --         r.genericAttribute6 AS businessUnitMap,
    --         r.component AS assignmentType,
    --         IFNULL(rpi.genericattribute1,'#') AS PIPositionName,  /* ORIGSQL: nvl(rpi.genericattribute1,'#') */
    --         :Gv_Processingunitseq AS PROCESSINGUNITSEQ
    --     FROM
    --         /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
    --         EXT.SH_QUERY_RESULT AS r
    --     LEFT OUTER JOIN
    --         EXT.SH_QUERY_RESULT AS rpi
    --         ON R.Genericsequence1 = rpi.Genericsequence1  /* ORIGSQL: R.Genericsequence1=rpi.Genericsequence1(+) */
    --         AND rpi.component = 'PI'   /* ORIGSQL: rpi.component(+)='PI' */
    --         AND rpi.periodseq = :v_periodSeq  /* ORIGSQL: rpi.periodseq(+)=v_periodSeq */
    --     WHERE
    --         R.Component = 'AOR'
    --         AND R.genericAttribute11 <> 'XO'
    --         AND R.Periodseq = :v_periodSeq;

    --     /* ORIGSQL: commit; */
    --     COMMIT;

/*Deepan : Replacement for Insert All */
-- First Insert based on the condition: PIPositionName <> positionName
INSERT INTO EXT.PIAOR_ASSIGNMENT
    (tenantid, salesTransactionSeq, salesOrderSeq, setNumber, compensationDate, positionName, payeeId, genericAttribute4, genericAttribute10, genericAttribute6, 
    genericAttribute7, genericAttribute8, PROCESSINGUNITSEQ)
SELECT 
    'AIAS' AS tenantid, 
    R.Genericsequence1 AS salesTransactionSeq, 
    R.Genericsequence2 AS salesOrderSeq, 
    r.genericnumber1 AS setNumber, 
    r.genericDate2 AS compensationDate, 
    r.genericAttribute1 AS positionName, 
    NULL AS payeeId, 
    r.component AS assignmentType, 
    r.genericAttribute5 AS ruleIndicator, 
    r.genericAttribute3 AS wAgency, 
    r.genericAttribute2 AS wAgyLdrTitle, 
    r.genericAttribute4 AS wAgyLdrDistrict, 
    :Gv_Processingunitseq AS PROCESSINGUNITSEQ
FROM 
    EXT.SH_QUERY_RESULT AS r
LEFT OUTER JOIN 
    EXT.SH_QUERY_RESULT AS rpi
    ON R.Genericsequence1 = rpi.Genericsequence1
    AND rpi.component = 'PI'
    AND rpi.periodseq = :v_periodSeq
WHERE 
    R.Component = 'AOR'
    AND R.genericAttribute11 <> 'XO'
    AND R.Periodseq = :v_periodSeq
    AND IFNULL(rpi.genericattribute1,'#') <> r.genericAttribute1; -- Condition: PIPositionName <> positionName

-- Second Insert based on the condition: PIPositionName = positionName
INSERT INTO EXT.SH_SEQUENCE 
    (businessSeq, seqType)
SELECT 
    R.Genericsequence1 AS salesTransactionSeq, 
    'AOR_TRXNSEQ' AS seqType
FROM 
    EXT.SH_QUERY_RESULT AS r
LEFT OUTER JOIN 
    EXT.SH_QUERY_RESULT AS rpi
    ON R.Genericsequence1 = rpi.Genericsequence1
    AND rpi.component = 'PI'
    AND rpi.periodseq = :v_periodSeq
WHERE 
    R.Component = 'AOR'
    AND R.genericAttribute11 <> 'XO'
    AND R.Periodseq = :v_periodSeq
    AND IFNULL(rpi.genericattribute1,'#') = r.genericAttribute1; -- Condition: PIPositionName = positionName

COMMIT;


        /* ORIGSQL: Log('22-1 updae AOR assignment') */
        CALL Log('22-1 updae AOR assignment');
        BEGIN
            /* ORIGSQL: DBMS_STATS.GATHER_TABLE_STATS(ownname => 'AIASEXT', tabname => 'SH_SEQUENCE', method_opt => 'FOR ALL INDEXED COLUMNS SIZE AUTO', estimate_percent => dbms_stats.auto_sample_size, degree => dbms_stats.d(...) */
            EXECUTE IMMEDIATE 'CREATE STATISTICS ON '|| 'AIASEXT'|| '.'|| 'SH_SEQUENCE';

            /* RESOLVE: Review semantics in source vs. target DBMS: Verify conversion from DBMS_STATS.GATHER_TABLE_STATS() to CREATE STATISTICS */
        END;

        --comDebugger('piaor','merge2 start piaor'||i_periodSeq);
        --update standard_pi_aor
        /* ORIGSQL: Log('22-2 gather sequence status information') */
        CALL Log('22-2 gather sequence status information');

        --version 8 comment
        --  merge /*+ INDEX(ta AIAS_TXNASSIGN_PNAME) */ into Cs_Transactionassignment ta
        --  using
        --  (Select Salestransactionseq, Salesorderseq, Setnumber,Policyissueddate,
            --          Compensationdate,Positionname,Wagyldrtitle,Wagency,Wagyldrdistrict,Ruleindicator,Businessunitmap
            --     from (SELECT /*+ LEADING(r,s) index(R SH_QUERY_RESULT_IDX2) */s.businessSeq businessSeq,
                --                r.genericSequence1 as salesTransactionSeq,r.genericSequence2 as salesOrderSeq,
                --                r.genericNumber1 as setNumber,
                --                r.genericDate1 as policyIssuedDate,r.genericDate2 as compensationDate,
                --                r.genericAttribute1 as positionName ,r.genericAttribute2 as wAgyLdrTitle,
                --                r.genericAttribute3 as wAgency, r.genericAttribute4 as wAgyLdrDistrict,
                --                R.Genericattribute5 As Ruleindicator, R.Genericattribute6 As Businessunitmap
                --             from sh_query_result r left join sh_sequence s
                --               on s.businessSeq=r.genericSequence1
                -- and S.Seqtype='AOR_TRXNSEQ'
                --            Where R.Component ='AOR'
                -- And R.Periodseq=Gv_Periodseq
                -- And r.Genericattribute11 <>'XO'
            --          ) R
            --          where r.businessSeq IS NOT NULL
        --  ) t
        --  on
        --  ( t.salestransactionSeq=ta.salestransactionseq
            -- And Ta.Positionname=T.Positionname
            --   -- and ta.tenantid='AIAS'
        --  )
        --  when matched then update set
        --    ta.genericAttribute4=decode( nvl(ta.genericAttribute4,'Standard'),'Standard', 'Standard_AOR', ta.genericAttribute4||'_AOR'),
        --    ta.genericAttribute10=t.ruleIndicator,
        --    ta.genericAttribute6=t.wAgency,
        --    ta.genericAttribute7=t.wAgyLdrTitle,
        --    ta.genericAttribute8=t.wAgyLdrDistrict;

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: merge into PIAOR_ASSIGNMENT ta using (SELECT Salestransactionseq, Salesorderseq, Setnumber, Policyissueddate, Compensationdate, Positionname, Wagyldrtitle, Wagency, Wagyldrdistrict, Ruleindicator, Bus(...) */
        MERGE INTO PIAOR_ASSIGNMENT AS ta
            USING
            (
                SELECT   /* ORIGSQL: (Select Salestransactionseq, Salesorderseq, Setnumber,Policyissueddate, Compensationdate,Positionname,Wagyldrtitle,Wagency,Wagyldrdistrict,Ruleindicator,Businessunitmap from (SELECT s.businessSeq busi(...) */
                    Salestransactionseq,
                    Salesorderseq,
                    Setnumber,
                    Policyissueddate,
                    Compensationdate,
                    Positionname,
                    Wagyldrtitle,
                    Wagency,
                    Wagyldrdistrict,
                    Ruleindicator,
                    Businessunitmap
                FROM
                    (
                        /* ORIGSQL: SELECT / *+ LEADING(r,s) index(R SH_QUERY_RESULT_IDX2) * / */
                        SELECT   /* ORIGSQL: (SELECT s.businessSeq businessSeq, r.genericSequence1 as salesTransactionSeq,r.genericSequence2 as salesOrderSeq, r.genericNumber1 as setNumber, r.genericDate1 as policyIssuedDate,r.genericDate2 as co(...) */
                            s.businessSeq AS businessSeq,
                            r.genericSequence1 AS salesTransactionSeq,
                            r.genericSequence2 AS salesOrderSeq,
                            r.genericNumber1 AS setNumber,
                            r.genericDate1 AS policyIssuedDate,
                            r.genericDate2 AS compensationDate,
                            r.genericAttribute1 AS positionName,
                            r.genericAttribute2 AS wAgyLdrTitle,
                            r.genericAttribute3 AS wAgency,
                            r.genericAttribute4 AS wAgyLdrDistrict,
                            R.Genericattribute5 AS Ruleindicator,
                            R.Genericattribute6 AS Businessunitmap
                        FROM
                            SH_QUERY_RESULT r
                        LEFT OUTER JOIN
                            SH_SEQUENCE s
                            ON s.businessSeq = r.genericSequence1
                            AND S.Seqtype = 'AOR_TRXNSEQ'
                        WHERE
                            R.Component = 'AOR'
                            AND R.Periodseq = :v_periodSeq
                            AND r.Genericattribute11 <> 'XO'
                    ) AS R
                WHERE
                    r.businessSeq IS NOT NULL
            ) AS t
            ON (t.salestransactionSeq = ta.salestransactionseq
                AND Ta.Positionname = T.Positionname
                -- and ta.tenantid='AIAS'
            )
        WHEN MATCHED THEN
            UPDATE SET
                ta.genericAttribute4 =MAP(IFNULL(ta.genericAttribute4,'Standard'), 'Standard', 'Standard_AOR', IFNULL(ta.genericAttribute4,'')||'_AOR'),  /* ORIGSQL: decode(nvl(ta.genericAttribute4,'Standard'),'Standard', 'Standard_AOR', ta.genericAttribute4||'_AOR') */
                ta.genericAttribute10 = t.ruleIndicator,
                ta.genericAttribute6 = t.wAgency,
                ta.genericAttribute7 = t.wAgyLdrTitle,
                ta.genericAttribute8 = t.wAgyLdrDistrict;

        /* ORIGSQL: Commit; */
        COMMIT;

        /* ORIGSQL: Log('22-3 updae AOR flag to PI assignment record') */
        CALL Log('22-3 updae AOR flag to PI assignment record');

        /* ORIGSQL: log('SP_TXA_PIAOR: end') */
        CALL Log('SP_TXA_PIAOR: end');

        --UPDATE XO result with to trxn.eb4
        /*Arjun 0520 - temporary patch*/
        /*update cs_transactionassignment ta
        set genericattribute4 = 'NADOR_Standard_AOR'
        where setnumber=1 and processingunitseq=gv_processingunitseq and tenantid='AIAS'
        and   ta.compensationDate>=v_periodStartDate
         and ta.compensationDate<v_periodEndDate
        and genericattribute4 = 'NADOR_AOR'
        ; */

        /* ORIGSQL: commit; */
        COMMIT;

        /*genericBoolean4 comment beacuse aiasadmin has not insufficient privileges*/
        --  Log('42');
        --  Merge /*+ INDEX(gst AIA_Cs_gaSalestransaction_SEQ) */ Into Cs_Gasalestransaction gst
        --  Using (
            --    Select Distinct Genericsequence1 As Salestransactionseq, --due to pi-aor might share one trxn seq, so need distinct here.
            --    0 as pagenumber
            --    From SH_QUERY_RESULT
            --    where Component in ('AOR','PI')
            -- And Periodseq=i_periodSeq
            -- AND genericAttribute11 ='XO'
        --  ) T
        --  On (T.Salestransactionseq=Gst.Salestransactionseq
            -- and t.pagenumber=gst.pagenumber
            -- and gst.tenantid='AIAS'
        --  )
        --  When Matched Then Update Set
        --       genericBoolean4=1;
        --
        --  commit;
        --  Log('42');

        /* ORIGSQL: exception when others then */
    END;

    --version 8 add monthly procedure

    --********** Creating procedure 'EXT.PK_PIAOR_CALCULATION:SP_MONTHLY_AGGREGATE' ********************
    /* ORIGSQL: procedure SP_MONTHLY_AGGREGATE as v_periodSeq int; */
PUBLIC PROCEDURE SP_MONTHLY_AGGREGATE
()
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE v_periodSeq BIGINT;  /* ORIGSQL: v_periodSeq int; */
    DECLARE v_periodStartDate TIMESTAMP;  /* ORIGSQL: v_periodStartDate date; */
    DECLARE v_Periodenddate TIMESTAMP;  /* ORIGSQL: v_Periodenddate date; */
    DECLARE v_PeriodName VARCHAR(50);  /* ORIGSQL: v_PeriodName VARCHAR2(50); */
    DECLARE v_piaor_year VARCHAR(50);  /* ORIGSQL: v_piaor_year varchar2(50); */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: When Others Then */
        BEGIN
            /* ORIGSQL: COMDEBUGGER('SP_MONTHLY_AGGREGATE error: ', sqlerrm) */
            CALL comDebugger('SP_MONTHLY_AGGREGATE error: ', ::SQL_ERROR_MESSAGE 
            );  /* ORIGSQL: sqlerrm */

            /* sapdbmtk: Closing RETURN in exception handler commented out, not supported in HANA */
            --RETURN;
        END;

        /* initialize library variables, if not yet done */
        CALL init_session_global();

        /* ORIGSQL: LOG('SP_MONTHLY_AGGREGATE: start') */
        CALL Log('SP_MONTHLY_AGGREGATE: start');

        /* ORIGSQL: log('gv_CYCLE_DATE: '||gv_CYCLE_DATE) */
        CALL Log('gv_CYCLE_DATE: '||IFNULL(:gv_CYCLE_DATE,''));

        /* ORIGSQL: log('gv_calendarSeq: '||gv_calendarSeq) */
        CALL Log('gv_calendarSeq: '||IFNULL(TO_VARCHAR(:gv_calendarSeq),''));

        --get period startDate, endDate  

        SELECT
            cp.PERIODSEQ,
            cp.name,
            cp.startDate,
            cp.endDate
        INTO
            v_periodSeq,
            v_PeriodName,
            v_periodStartDate,
            v_Periodenddate
        FROM
            CS_PERIOD cp,
            cs_periodtype pt
        WHERE
            cp.tenantid = 'AIAS'
            AND cp.REMOVEDATE = :cdt_EndOfTime
            AND cp.CALENDARSEQ = :gv_calendarSeq
            AND cp.startdate <= to_char(:gv_CYCLE_DATE,'yyyy-mm-dd')  /* ORIGSQL: to_date(gv_CYCLE_DATE,'yyyy-mm-dd') */
            AND cp.enddate > to_char(:gv_CYCLE_DATE,'yyyy-mm-dd')  /* ORIGSQL: to_date(gv_CYCLE_DATE,'yyyy-mm-dd') */
            AND pt.name = 'month'
            AND pt.periodtypeseq = cp.periodtypeseq;

        IF :v_periodStartDate >= TO_DATE('2016-12-01')   /* ORIGSQL: date '2016-12-01' */
        AND :v_Periodenddate <= TO_DATE('2018-01-01') --for 2017
        /* ORIGSQL: date '2018-01-01' */
        THEN   
            SELECT
                SUBSTRING(C.NAME,1,4)  /* ORIGSQL: SUBSTR(C.NAME,1,4) */
            INTO
                v_piaor_year
            FROM
                CS_PERIOD A,
                CS_PERIOD B,
                CS_PERIOD C
            WHERE
                A.tenantid = 'AIAS'
                AND B.tenantid = 'AIAS'
                AND C.tenantid = 'AIAS'
                AND A.removeDate = :cdt_EndOfTime
                AND B.removeDate = :cdt_EndOfTime
                AND C.removeDate = :cdt_EndOfTime
                AND A.PERIODSEQ = :v_periodSeq
                AND A.calendarSeq = B.calendarSeq
                AND A.PARENTSEQ = B.PERIODSEQ
                AND B.calendarSeq = C.calendarSeq
                AND B.PARENTSEQ = C.PERIODSEQ;
        ELSE  
            SELECT
                EXTRACT(YEAR FROM startdate)
            INTO
                v_piaor_year
            FROM
                cs_period
            WHERE
                tenantid = 'AIAS'
                AND removeDate = :cdt_EndOfTime
                AND PERIODSEQ = :v_periodSeq;
        END IF;

        /* ORIGSQL: log('v_periodSeq: ' ||v_periodSeq) */
        CALL Log('v_periodSeq: '||IFNULL(TO_VARCHAR(:v_periodSeq),''));

        /* ORIGSQL: log('v_PeriodName: '||v_PeriodName) */
        CALL Log('v_PeriodName: '||IFNULL(:v_PeriodName,''));

        /* ORIGSQL: log('v_periodStartDate: '||v_periodStartDate) */
        CALL Log('v_periodStartDate: '||IFNULL(TO_VARCHAR(:v_periodStartDate),''));

        /* ORIGSQL: log('v_periodEndDate: '||v_periodEndDate) */
        CALL Log('v_periodEndDate: '||IFNULL(TO_VARCHAR(:v_Periodenddate),''));

        /* ORIGSQL: log('v_piaor_year: '||v_piaor_year) */
        CALL Log('v_piaor_year: '||IFNULL(:v_piaor_year,''));

        /* ORIGSQL: commit; */
        COMMIT;

        --init target table
        /* ORIGSQL: log('23 init table') */
        CALL Log('23 init table');

        /* ORIGSQL: execute immediate 'truncate table AIA_PIAOR_TRAN_TMP'; */
        /* ORIGSQL: truncate table AIA_PIAOR_TRAN_TMP ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_PIAOR_TRAN_TMP';

        /* ORIGSQL: delete from PIAOR_DETAIL where period=v_periodSeq; */
        DELETE
        FROM
            PIAOR_DETAIL
        WHERE
            period = :v_periodSeq;

        /* ORIGSQL: INSERT INTO AIA_PIAOR_TRAN_TMP select v_periodSeq, 'PI', v.genericAttribute3 wAgency, s.genericattribute12 wAgent, v.genericAttribute1 payee, v.genericAttribute5 rule, s.PRODUCTNAME, SUM(s.value) valu(...) */
        INSERT INTO AIA_PIAOR_TRAN_TMP
            SELECT   /* ORIGSQL: select v_periodSeq, 'PI', v.genericAttribute3 wAgency, s.genericattribute12 wAgent, v.genericAttribute1 payee, v.genericAttribute5 rule, s.PRODUCTNAME, SUM(s.value) value from SH_QUERY_RESULT v, cs_sa(...) */
                /* + parallel(18) */
                :v_periodSeq,
                'PI',
                v.genericAttribute3 AS wAgency,
                s.genericattribute12 AS wAgent,
                v.genericAttribute1 AS payee,
                v.genericAttribute5 AS rule,
                /* --'' PIB,--AOR only */
                /* --sum(s.value) RYC */
                s.PRODUCTNAME  /* --version 9 add */, SUM(s.value) AS value
            FROM
                SH_QUERY_RESULT v,
                cs_salestransaction s
            WHERE
                v.component = 'PI'
                AND s.salestransactionseq = v.genericSequence1
                AND v.periodseq = :v_periodSeq
                AND ((v.genericattribute11 = 'XO'
                    AND s.genericattribute17 <> 'O')
                OR v.genericattribute11 <> 'XO')
                AND s.compensationdate >= :v_periodStartDate
                AND s.compensationdate < :v_Periodenddate
                AND v.genericAttribute6 IN (1,16) -- correct
                AND ((s.productname IN ('LF','HS')
                        AND s.genericattribute2 IN ('PAY2','PAY3','PAY4','PAY5','PAY6')))
            GROUP BY
                :v_periodSeq,v.genericAttribute3,s.genericattribute12,v.genericAttribute1,v.genericAttribute5,s.PRODUCTNAME;

        /* ORIGSQL: log('24 Sum PI RYC : '||SQL%ROWCOUNT) */
        CALL Log('24 Sum PI RYC : '||::ROWCOUNT);  

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: insert into AIA_PIAOR_TRAN_TMP select v_periodSeq, 'AOR', v.genericAttribute3 wAgency, s.genericattribute12 wAgent, v.genericAttribute1 payee, v.genericAttribute5 rule, s.PRODUCTNAME|| CASE WHEN v.gen(...) */
        /* RESOLVE: Identifier not found: Table/view 'cs_GASALESTRANSACTION' not found */
        INSERT INTO AIA_PIAOR_TRAN_TMP
            SELECT   /* ORIGSQL: select v_periodSeq, 'AOR', v.genericAttribute3 wAgency, s.genericattribute12 wAgent, v.genericAttribute1 payee, v.genericAttribute5 rule, s.PRODUCTNAME|| CASE WHEN v.genericattribute7 = 'RYC' THEN '_R(...) */
                /* + parallel(12) */
                :v_periodSeq,
                'AOR',
                v.genericAttribute3 AS wAgency,
                s.genericattribute12 AS wAgent,
                v.genericAttribute1 AS payee,
                v.genericAttribute5 AS rule,
                /* --version 9 add aor only */
                /* --sum(case when v.genericattribute7 <> 'RYC' then s.value+ nvl(gs.genericnumber3,0) else 0 end ) PIB , */
                /* --sum(case when v.genericattribute7 = 'RYC' then s.value else 0 end ) RYC, */
                IFNULL(s.PRODUCTNAME,'')||
                CASE 
                    WHEN v.genericattribute7 = 'RYC'
                    THEN '_RYC'
                    ELSE '_PIB'
                END
                AS PRODUCTNAME,
                SUM(
                    CASE 
                        WHEN v.genericattribute7 = 'RYC'
                        THEN s.value
                        ELSE s.value+ IFNULL(gs.genericnumber3,0)  /* ORIGSQL: nvl(gs.genericnumber3,0) */
                    END
                ) AS value
            FROM
                SH_QUERY_RESULT v,
                cs_salestransaction s,
                cs_gasalestransaction gs
            WHERE
                v.component = 'AOR'
                AND s.salestransactionseq = v.genericSequence1
                AND s.salestransactionseq = gs.salestransactionseq
                AND v.periodseq = :v_periodSeq
                AND s.compensationdate >= :v_periodStartDate
                AND s.compensationdate < :v_Periodenddate
                AND v.genericAttribute6 IN (1,16) -- correct
                AND ((v.genericattribute11 = 'XO'
                    AND s.genericattribute17 <> 'O')
                OR v.genericattribute11 <> 'XO')
                AND (v.genericattribute7 IN ('API','IFYC','FYC', 'SSCP')
                    OR (v.genericattribute7 = 'RYC'
                        AND ((s.productname IN ('LF','HS')
                                AND s.genericattribute2 IN ('PAY2','PAY3','PAY4','PAY5','PAY6'))
                            OR s.productname IN ('PA')
                        )
                    )
                )
            GROUP BY
                :v_periodSeq,v.genericAttribute3,s.genericattribute12,v.genericAttribute1,v.genericAttribute5,
                s.PRODUCTNAME,v.genericattribute7;

        /* ORIGSQL: log('25 Sum AOR RYC and PIB : '||SQL%ROWCOUNT) */
        CALL Log('25 Sum AOR RYC and PIB : '||::ROWCOUNT);  

        /* ORIGSQL: commit; */
        COMMIT;

        --insert into target table

        /* ORIGSQL: INSERT INTO PIAOR_DETAIL(Period, PIAOR_Year, component, Wri_Agency, Wri_Agent, Payee_Agency, Rule, PA_RYC, LF_RYC, RYC) select v_periodSeq, v_piaor_year, f.component, f.wagency, f.wagent, f.payee, f.r(...) */
        INSERT INTO PIAOR_DETAIL
            (
                Period,
                PIAOR_Year,
                component,
                Wri_Agency,
                Wri_Agent,
                Payee_Agency,
                Rule,
                /* --PIB, version 9 comment */
                PA_RYC,
                LF_RYC,
                RYC
            )
            SELECT   /* ORIGSQL: select v_periodSeq, v_piaor_year, f.component, f.wagency, f.wagent, f.payee, f.rule, SUM(CASE WHEN f.PRODUCTNAME='PA' THEN f.value ELSE 0 END) PA_RYC, SUM(CASE WHEN f.PRODUCTNAME<>'PA' THEN f.value EL(...) */
                /*leading(f) */
                :v_periodSeq,
                :v_piaor_year,
                f.component,
                f.wagency,
                f.wagent,
                f.payee,
                f.rule,
                /* --version 9 */
                /* --sum(PIB), */
                /* --sum(ryc) */
                SUM(
                    CASE 
                        WHEN f.PRODUCTNAME = 'PA'
                        THEN f.value
                        ELSE 0
                    END
                ) AS PA_RYC,
                SUM(
                    CASE 
                        WHEN f.PRODUCTNAME <> 'PA'
                        THEN f.value
                        ELSE 0
                    END
                ) AS LF_RYC,
                SUM(value)
                /* --version 9 end */
            FROM
                AIA_PIAOR_TRAN_TMP f
            WHERE
                COMPONENT = 'PI'   --version 9 add just for PI calculate
            GROUP BY
                f.component,f.wagency,f.wagent,f.payee,f.rule;

        /* ORIGSQL: log('26 classify PI : '||SQL%ROWCOUNT) */
        CALL Log('26 classify PI : '||::ROWCOUNT);  

        /* ORIGSQL: commit; */
        COMMIT;

        --version 9 add for AOR
        /* ORIGSQL: INSERT INTO PIAOR_DETAIL(Period, PIAOR_Year, component, Wri_Agency, Wri_Agent, Payee_Agency, Rule, PA_FYC, CS_FYC, LF_FYC, PIB, PA_RYC, LF_RYC, RYC) select v_periodSeq, v_piaor_year, f.component, f.wa(...) */
        INSERT INTO PIAOR_DETAIL
            (
                Period,
                PIAOR_Year,
                component,
                Wri_Agency,
                Wri_Agent,
                Payee_Agency,
                Rule,
                PA_FYC,
                CS_FYC,
                LF_FYC,
                PIB,
                PA_RYC,
                LF_RYC,
                RYC
            )
            SELECT   /* ORIGSQL: select v_periodSeq, v_piaor_year, f.component, f.wagency, f.wagent, f.payee, f.rule, SUM(CASE WHEN f.PRODUCTNAME='PA_PIB' THEN f.value ELSE 0 END) PA_FYC, SUM(CASE WHEN f.PRODUCTNAME='CS_PIB' THEN f.v(...) */
                /*leading(f) */
                :v_periodSeq,
                :v_piaor_year,
                f.component,
                f.wagency,
                f.wagent,
                f.payee,
                f.rule,
                SUM(
                    CASE 
                        WHEN f.PRODUCTNAME = 'PA_PIB'
                        THEN f.value
                        ELSE 0
                    END
                ) AS PA_FYC,
                SUM(
                    CASE 
                        WHEN f.PRODUCTNAME = 'CS_PIB'
                        THEN f.value
                        ELSE 0
                    END
                ) AS CS_FYC,
                SUM(
                    CASE 
                        WHEN f.PRODUCTNAME LIKE '%PIB'
                        AND PRODUCTNAME NOT IN ('PA_PIB','CS_PIB')
                        THEN f.value
                        ELSE 0
                    END
                ) AS LF_FYC,
                SUM(
                    CASE 
                        WHEN f.PRODUCTNAME LIKE '%PIB'
                        THEN f.value
                        ELSE 0
                    END
                ) AS PIB,
                SUM(
                    CASE 
                        WHEN f.PRODUCTNAME = 'PA_RYC'
                        THEN f.value
                        ELSE 0
                    END
                ) AS PA_RYC,
                SUM(
                    CASE 
                        WHEN f.PRODUCTNAME LIKE '%RYC'
                        AND f.PRODUCTNAME <> 'PA_RYC'
                        THEN f.value
                        ELSE 0
                    END
                ) AS LF_RYC,
                SUM(
                    CASE 
                        WHEN f.PRODUCTNAME LIKE '%RYC'
                        THEN f.value
                        ELSE 0
                    END
                ) AS RYC
            FROM
                AIA_PIAOR_TRAN_TMP f
            WHERE
                COMPONENT = 'AOR'
            GROUP BY
                f.component,f.wagency,f.wagent,f.payee,f.rule;

        /* ORIGSQL: log('26-1 classify AOR : '||SQL%ROWCOUNT) */
        CALL Log('26-1 classify AOR : '||::ROWCOUNT);  

        /* ORIGSQL: commit; */
        COMMIT;

        --version 9 end

        /* ORIGSQL: LOG('SP_MONTHLY_AGGREGATE: end') */
        CALL Log('SP_MONTHLY_AGGREGATE: end');

        /* ORIGSQL: Exception When Others Then */
    END;

    --********** Creating procedure 'EXT.PK_PIAOR_CALCULATION:SP_TXNTXA_YREND_PI' ********************
    /* ORIGSQL: procedure SP_TXNTXA_YREND_PI as v_periodSeq int; */
PUBLIC PROCEDURE SP_TXNTXA_YREND_PI
()
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE v_periodSeq BIGINT;  /* ORIGSQL: v_periodSeq int; */
    DECLARE v_yrStartDate TIMESTAMP;  /* ORIGSQL: v_yrStartDate date; */
    DECLARE v_yrEndDate TIMESTAMP;  /* ORIGSQL: v_yrEndDate date; */
    DECLARE v_yrEndEventTypeSeq BIGINT = 0;  /* ORIGSQL: v_yrEndEventTypeSeq int := 0; */
    DECLARE v_modificationTime TIMESTAMP;
    DECLARE v_periodStartDate TIMESTAMP;  /* ORIGSQL: v_periodStartDate date; */
    DECLARE v_periodEndDate TIMESTAMP;  /* ORIGSQL: v_periodEndDate date; */
    DECLARE v_compDate TIMESTAMP;  /* ORIGSQL: v_compDate date; */
    DECLARE Vseq DECIMAL(38,10);  /* ORIGSQL: Vseq Number; */
    DECLARE v_txnSeq DECIMAL(38,10);  /* ORIGSQL: v_txnSeq number; */
    DECLARE v_piaor_year VARCHAR(50);  /* ORIGSQL: v_piaor_year varchar2(50); */
    DECLARE NO_YRENDFIXEDVALUE_FOUND CONDITION;  /* ORIGSQL: NO_YRENDFIXEDVALUE_FOUND EXCEPTION; */
    DECLARE NO_YRENDEVENTTYPE_FOUND CONDITION;  /* ORIGSQL: NO_YRENDEVENTTYPE_FOUND EXCEPTION; */
    DECLARE INVALID_PERIODDATE CONDITION;  /* ORIGSQL: INVALID_PERIODDATE EXCEPTION; */
    DECLARE v1 DECIMAL(38,10);  /* ORIGSQL: v1 number; */
    DECLARE v2 DECIMAL(38,10);  /* ORIGSQL: v2 number; */
    DECLARE v3 DECIMAL(38,10);  /* ORIGSQL: v3 number; */
    DECLARE v_rtn BIGINT = 0;  /* ORIGSQL: v_rtn int := 0; */

    DECLARE EXIT HANDLER FOR NO_YRENDEVENTTYPE_FOUND
        /* ORIGSQL: when NO_YRENDEVENTTYPE_FOUND then */
        BEGIN
            gv_error = 'Error [SP_TXNTXA_YREND_PI]: the PI_Year_End event type is not found - ' ||

            /* RESOLVE: Standard Package call(not converted): 'dbms_utility.format_error_backtrace' not supported, manual conversion required */
            dbms_utility.format_error_backtrace;

            /* ORIGSQL: raise_application_error(-20000, gv_error) */
            -- sapdbmtk: mapped error code -20000 => 10000: (ABS(-20000)%10000)+10000
            SIGNAL SQL_ERROR_CODE 10000 SET MESSAGE_TEXT = :gv_error;
        END;



    DECLARE EXIT HANDLER FOR INVALID_PERIODDATE
        /* ORIGSQL: when INVALID_PERIODDATE then */
        BEGIN
            gv_error = 'Error [SP_TXNTXA_YREND_PI]: the year start date  date are invalid - ' ||

            /* RESOLVE: Standard Package call(not converted): 'dbms_utility.format_error_backtrace' not supported, manual conversion required */
            dbms_utility.format_error_backtrace;

            /* ORIGSQL: raise_application_error(-20000, gv_error) */
            -- sapdbmtk: mapped error code -20000 => 10000: (ABS(-20000)%10000)+10000
            SIGNAL SQL_ERROR_CODE 10000 SET MESSAGE_TEXT = :gv_error;
        END;



    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: when others then */
        BEGIN
            /* ORIGSQL: rollback; */
            ROLLBACK;

            gv_error = 'Error [SP_TXNTXA_YREND_PI]: ' || ::SQL_ERROR_MESSAGE || ' - ' ||  /* ORIGSQL: sqlerrm */

            /* RESOLVE: Standard Package call(not converted): 'dbms_utility.format_error_backtrace' not supported, manual conversion required */
            dbms_utility.format_error_backtrace;

            /* ORIGSQL: comDebugger('PIAOR YR DEBUGGER', 'ERROR' || gv_error) */
            CALL comDebugger('PIAOR YR DEBUGGER', 'ERROR'|| IFNULL(:gv_error,''));

            /* ORIGSQL: raise_application_error(-20000, gv_error) */
            -- sapdbmtk: mapped error code -20000 => 10000: (ABS(-20000)%10000)+10000
            SIGNAL SQL_ERROR_CODE 10000 SET MESSAGE_TEXT = :gv_error;
        END;

        /* initialize library variables, if not yet done */
        CALL init_session_global();

        /* ORIGSQL: v_modificationTime timestamp := gv_plstartTime - interval '1' second; */
        v_modificationTime = TO_DATE(ADD_SECONDS(:gv_plStartTime,0));  

        --year end process

        --dbms_output.put_line('get fixed value');
        /* ORIGSQL: log('SP_TXNTXA_YREND_PI: start') */
        CALL Log('SP_TXNTXA_YREND_PI: start');

        --get period startDate, endDate  

        SELECT
            cp.PERIODSEQ,
            cp.startDate,
            cp.endDate
        INTO
            v_periodSeq,
            v_periodStartDate,
            v_periodEndDate
        FROM
            CS_PERIOD cp,
            cs_periodtype pt
        WHERE
            cp.tenantid = 'AIAS'
            AND cp.REMOVEDATE = :cdt_EndOfTime
            AND cp.CALENDARSEQ = :gv_calendarSeq
            AND cp.startdate <= to_char(:gv_CYCLE_DATE,'yyyy-mm-dd')  /* ORIGSQL: to_date(gv_CYCLE_DATE,'yyyy-mm-dd') */
            AND cp.enddate > to_char(:gv_CYCLE_DATE,'yyyy-mm-dd')  /* ORIGSQL: to_date(gv_CYCLE_DATE,'yyyy-mm-dd') */
            AND pt.name = 'month'
            AND pt.periodtypeseq = cp.periodtypeseq;

        /* ORIGSQL: commit; */
        COMMIT;

        v_rtn = comGetYrLastMonth(:v_periodSeq);

        IF :v_rtn < 1
        THEN
            /* ORIGSQL: log('SP_TXNTXA_YREND_PI: not year end month') */
            CALL Log('SP_TXNTXA_YREND_PI: not year end month'); 

            RETURN;
        END IF;

        Gv_Periodseq = :v_periodSeq;

        -- init

        /* ORIGSQL: log('27 init table ') */
        CALL Log('27 init table ');

        /* ORIGSQL: execute immediate 'truncate table AIA_YrEnd_Tran_rela'; */
        /* ORIGSQL: truncate table AIA_YrEnd_Tran_rela ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_YrEnd_Tran_rela';
        BEGIN 
            DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
                /* ORIGSQL: when no_data_found then */
                BEGIN
                    /* ORIGSQL: raise NO_YRENDEVENTTYPE_FOUND; */
                    SIGNAL NO_YRENDEVENTTYPE_FOUND;
                END;



            /* RESOLVE: Identifier not found: Table/view 'EXT.CS_EVENTTYPE' not found */

            SELECT
                IFNULL(dataTypeSeq, 0)  /* ORIGSQL: nvl(dataTypeSeq, 0) */
            INTO
                v_yrEndEventTypeSeq
            FROM
                cs_eventType
            WHERE
                tenantid = 'AIAS'
                AND eventTypeId = 'PI_Year_End'
                AND removeDate = :cdt_EndOfTime;

            --dbms_output.put_line('get event date');

            /* ORIGSQL: exception when no_data_found then */
        END;

        -- dbms_output.put_line('get start date');
        --version 8

        --begin
        --  select y.startDate, y.endDate
        --    into v_yrStartDate, v_yrEndDate
        --    from cs_periodtype pt, cs_period y, cs_period p
        --   where pt.tenantid='AIAS'
        -- and y.tenantid='AIAS'
        -- and p.tenantid='AIAS'
        -- and pt.name = 'year'
        -- and pt.removeDate = cdt_EndOfTime
        -- and p.removeDate = cdt_EndOfTime
        -- and y.removeDate = cdt_EndOfTime
        -- and p.periodSeq = v_periodSeq
        -- and y.startDate <= p.startDate
        -- and y.endDate > p.startDate
        -- and y.calendarSeq = p.calendarSeq
        -- and y.periodTypeSeq = pt.periodTypeSeq;
        --exception
        --  when no_data_found then
        --    raise INVALID_PERIODDATE;
        --end;

        IF :v_periodStartDate >= TO_DATE('2016-12-01')   /* ORIGSQL: date '2016-12-01' */
        AND :v_periodEndDate <= TO_DATE('2018-01-01') --for 2017
        /* ORIGSQL: date '2018-01-01' */
        THEN
            SELECT
                ADD_MONTHS(:v_periodStartDate,-12)
            INTO
                v_yrStartDate
            FROM
                SYS.DUMMY;  /* ORIGSQL: FROM DUAL ; */

            SELECT
                SUBSTRING(C.NAME,1,4),  /* ORIGSQL: SUBSTR(C.NAME,1,4) */
                C.ENDDATE,
                TO_DATE(ADD_SECONDS(C.ENDDATE,(86400*-1)))   /* ORIGSQL: C.ENDDATE-1 */
            INTO
                v_piaor_year,
                v_yrEndDate,
                v_compDate
            FROM
                CS_PERIOD A,
                CS_PERIOD B,
                CS_PERIOD C
            WHERE
                A.tenantid = 'AIAS'
                AND B.tenantid = 'AIAS'
                AND C.tenantid = 'AIAS'
                AND A.removeDate = :cdt_EndOfTime
                AND B.removeDate = :cdt_EndOfTime
                AND C.removeDate = :cdt_EndOfTime
                AND A.PERIODSEQ = :v_periodSeq
                AND A.calendarSeq = B.calendarSeq
                AND A.PARENTSEQ = B.PERIODSEQ
                AND B.calendarSeq = C.calendarSeq
                AND B.PARENTSEQ = C.PERIODSEQ;
        ELSE  
            SELECT
                -- EXTRACT(YEAR FROM startdate),
                -- sapdbmtk.sp_f_dbmtk_truncate_datetime(startdate,'YYYY'),  /* ORIGSQL: trunc(startdate,'YYYY') */
                -- ADD_MONTHS(sapdbmtk.sp_f_dbmtk_truncate_datetime(startdate,'yyyy'),12),  /* ORIGSQL: trunc(startdate,'yyyy') */
                -- TO_DATE(ADD_SECONDS(ADD_MONTHS(sapdbmtk.sp_f_dbmtk_truncate_datetime(startdate,'yyyy'),12),(86400*-1)))   /* ORIGSQL: trunc(startdate,'yyyy') */
            /*Deepan : Replaced the above date transformation with the below logic*/
                extract (year from startdate ) ,
                to_date(to_varchar(startdate, 'YYYY'), 'YYYY') ,
                ADD_MONTHS(to_date(to_varchar(startdate, 'YYYY'), 'YYYY'),12) ,
                ADD_DAYS(ADD_MONTHS(to_date(to_varchar(startdate, 'YYYY'), 'YYYY'),12) ,-1)
            
            INTO
                v_piaor_year,
                v_yrStartDate,
                v_yrEndDate,
                v_compDate
            FROM
                cs_period
            WHERE
                tenantid = 'AIAS'
                AND removeDate = :cdt_EndOfTime
                AND PERIODSEQ = :v_periodSeq;
        END IF;

        --version 8 end

        /* ORIGSQL: Log('28') */
        CALL Log('28');

        /* ORIGSQL: delete / *+ FULL(PIAOR_Assignment) * / */

        /* ORIGSQL: delete from PIAOR_Assignment where tenantid='AIAS' and genericAttribute4 like '%PI%' and processingunitseq=GV_PROCESSINGUNITSEQ and Genericattribute9 = 'YE REASSIGN TO DISTRICT' AND COMPENSATIONDATE =(...) */
        DELETE
        FROM
            PIAOR_Assignment
        WHERE
            tenantid = 'AIAS'
            AND genericAttribute4 LIKE '%PI%'
            AND processingunitseq = :Gv_Processingunitseq
            AND Genericattribute9 = 'YE REASSIGN TO DISTRICT'
            AND COMPENSATIONDATE = :v_compDate;

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: Log('29 delete ye reassign for rerun') */
        CALL Log('29 delete ye reassign for rerun');
        --version 8 comment
        --Log('51');
        --
        --delete from cs_gasalestransaction ga
        -- Where tenantid='AIAS' and Exists
        --       (Select 1
            --          From Cs_Salestransaction st
            --         where
            --               st.tenantid='AIAS'
            -- And Processingunitseq = Gv_Processingunitseq
            -- And Compensationdate = V_Compdate
            -- And Ga.Salestransactionseq = St.Salestransactionseq
            -- and Eventtypeseq = V_Yrendeventtypeseq
        --           );
        --
        --commit;
        --Log('51');

        --version 8 comment
        --Log('52');
        --delete /*+ parallel(8) FULL(cs_salestransaction)  */ from cs_salestransaction
        -- Where
        -- tenantid='AIAS'
        -- and processingUnitSeq = gv_processingUnitSeq
        -- and COMPENSATIONDATE = v_compDate
        -- and Eventtypeseq = V_Yrendeventtypeseq;
        --
        --
        --
        --commit;
        --Log('52');

        --Vseq:=Sequencegenpkg.Getnextfullseq('auditLogSeq', Classid.Cidauditlog);
        --v_txnSeq := Sequencegenpkg.Getnextfullseq('salesTransactionSeq',
        --                                          Classid.Cidsalestransaction);

        /*Arjun 20170509
        
        The issue is that there are many salestransactionseqs that were deleted from cs_salestransaction,
        but not from cs_Gasalestransaction and Cs_transactionassignment.
        
        The logic the SH uses is to get the max STSEQ from CS_Salestransaction, and then adds to that
        before inserting the new Year End records. These clash with the existing records.
        I can change the logic to get the maximum seq from the SalesTransaction, GA and Assignment tables
        and use that as a base, but ideally, if we?re deleting transactions, they should be deleted from all the tables.
        
        */
        -- select /*+ INDEX(CS_salestransaction,AIA_CS_SALESTRANSACTION_SEQ) */  MAX(salesTransactionSeq)+1 into v_txnSeq
        --  from CS_salestransaction
        SELECT
            MAX(salestransactionseq) +1
        INTO
            v_txnSeq
            /* RESOLVE: Identifier not found: Table/view 'EXT.CS_SALESTRANSACTION' not found */
        FROM
            (
                /* ORIGSQL: select / *+ INDEX(CS_salestransaction,AIA_CS_SALESTRANSACTION_SEQ) * / */
                SELECT   /* ORIGSQL: (select MAX(salesTransactionSeq) salestransactionseq from CS_salestransaction) */
                    MAX(salesTransactionSeq) AS salestransactionseq
                FROM
                    CS_salestransaction
            UNION ALL
                SELECT   /* ORIGSQL: select MAX(salestransactionseq) from PIAOR_ASSIGNMENT */
                    MAX(salestransactionseq)
                FROM
                    PIAOR_ASSIGNMENT
            UNION ALL
                SELECT   /* ORIGSQL: select MAX(salestransactionseq) from cs_gasalestransaction) ; */
                    MAX(salestransactionseq)
                FROM
                    cs_gasalestransaction
            ) AS dbmtk_corrname_54238;

        --CREATE NEW TXN

        v1 = Comgeteventtypeseq('RYC');

        v2 = Comgeteventtypeseq('H_RYC');

        v3 = Comgeteventtypeseq('ORYC');

        -- et.eventTypeId in ('RYC', 'H_RYC', 'ORYC')

        /**
         create table AIA_MAX_sublinenumber tablespace tallydata
         as
         select *+ index(cs_salestransaction AIA_salestransaction_orderline) * salesorderseq,max(sublinenumber) as maxsublinenumber
                  from cs_salestransaction
                 where salesorderseq = st.salesorderseq and st.tenantid='AIAS' and 1=0 group by salesorderseq
        
                 create index AIA_MAX_sublinenumber_idx on AIA_MAX_sublinenumber(salesorderseq,maxsublinenumber) tablespace tallyindex
          */

        /* ORIGSQL: execute immediate 'truncate table  AIA_MAX_sublinenumber'; */
        /* ORIGSQL: truncate table AIA_MAX_sublinenumber ; */
        EXECUTE IMMEDIATE 'TRUNCATE TABLE AIA_MAX_sublinenumber';

        /* ORIGSQL: insert into AIA_MAX_sublinenumber select ta.salesorderseq,MAX(sublinenumber) as maxsublinenumber from cs_salestransaction st, PIAOR_ASSIGNMENT ta where ta.compensationDate >= v_yrStartDate And ta.Comp(...) */
        INSERT INTO AIA_MAX_sublinenumber
            /* ORIGSQL: select / *+ leading(ta,st) index(cs_salestransaction AIA_salestransaction_orderline) index(ta AIA_CS_TRANSACTIONASSIGN_IDX2) * / */
            SELECT   /* ORIGSQL: select ta.salesorderseq,MAX(sublinenumber) as maxsublinenumber from cs_salestransaction st, PIAOR_ASSIGNMENT ta where ta.compensationDate >= v_yrStartDate And ta.Compensationdate < V_Yrenddate and ta.(...) */
                ta.salesorderseq,
                MAX(sublinenumber) AS maxsublinenumber
            FROM
                cs_salestransaction st,
                PIAOR_ASSIGNMENT ta
            WHERE
                ta.compensationDate >= :v_yrStartDate
                AND ta.Compensationdate < :v_yrEndDate
                AND ta.tenantid = 'AIAS'
                AND ta.processingunitseq = :Gv_Processingunitseq
                AND Ta.Genericattribute5 = 'PI - Direct Team' -- changed check ga5, and only create trxn for pi-direct team
                AND ta.genericAttribute7 = 'FSAD'
                AND st.salestransactionseq = ta.salestransactionseq
                AND ta.processingunitseq = st.processingunitseq
                AND ta.Compensationdate = st.Compensationdate
            GROUP BY
                ta.salesorderseq;

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: Log('30 '||v_yrStartDate ||' ' ||v_yrEndDate || ' ' || v_compdate) */
        CALL Log('30 '||IFNULL(TO_VARCHAR(:v_yrStartDate),'') ||' '||IFNULL(TO_VARCHAR(:v_yrEndDate),'') || ' '|| IFNULL(TO_VARCHAR(:v_compDate),''));

        --version 8 comment ,no sufficient privileges
        --insert all when salestransactionSeq > 0 then into cs_salestransaction
        --  (tenantid,salestransactionseq,
            --   salesOrderSeq,
            --   linenumber,
            --   sublinenumber,
            --   eventtypeseq,
            --   compensationDate,
            --   value,
            --   unittypeforvalue,
            --   modificationDate,
            --   isRunnable,
            --   ORIGINTYPEID,
            --   PREADJUSTEDVALUE,
            --   UNITTYPEFORPREADJUSTEDVALUE,
            --   PROCESSINGUNITSEQ,
            --   genericDate6,
            --   pipelineRunSeq,
            --   unittypeForLineNumber,
            --   unitTypeForSubLineNumber,
            --   businessUnitMap,
            --   --GENERIC FIELDS
            --   productId,
            --   productName,
            --   productDescription,
            --   dataSource,
            --   genericAttribute1,
            --   genericAttribute2,
            --   genericAttribute3,
            --   genericAttribute4,
            --   genericAttribute5,
            --   genericAttribute6,
            --   genericAttribute7,
            --   genericAttribute8,
            --   genericAttribute9,
            --   genericAttribute10,
            --   genericAttribute11,
            --   genericAttribute12,
            --   genericAttribute13,
            --   genericAttribute14,
            --   genericAttribute15,
            --   genericAttribute16,
            --   genericAttribute17,
            --   genericAttribute18,
            --   genericAttribute19,
            --   genericAttribute20,
            --   genericAttribute21,
            --   genericAttribute22,
            --   genericAttribute23,
            --   genericAttribute24,
            --   genericAttribute25,
            --   genericAttribute26,
            --   genericAttribute27,
            --   genericAttribute28,
            --   genericAttribute29,
            --   genericAttribute30,
            --   genericAttribute31,
            --   genericAttribute32,
            --   genericNumber1,
            --   genericNumber2,
            --   genericNumber3,
            --   genericNumber4,
            --   genericNumber5,
            --   genericNumber6,
            --   unitTypeForGenericNumber1,
            --   unitTypeForGenericNumber2,
            --   unitTypeForGenericNumber3,
            --   unitTypeForGenericNumber4,
            --   unitTypeForGenericNumber5,
            --   unitTypeForGenericNumber6,
            --   genericDate1,
            --   genericDate2,
            --   genericDate3,
            --   genericDate4,
            --   genericDate5,
            --   genericBoolean1,
            --   genericBoolean2,
            --   genericBoolean3,
            --   genericBoolean4,
            --   genericBoolean5,
            --   genericBoolean6
            --   ---
        --   )
        --values
        --  ('AIAS',salesTransactionSeq,
            --   salesOrderSeq,
            --   linenumber,
            --   sublinenumber,
            --   v_yrEndEventTypeSeq,
            --   v_compDate,
            --   value,
            --   unittypeforvalue,
            --   modificationDate,
            --   isRunnable,
            --   ORIGINTYPEID,
            --   PREADJUSTEDVALUE,
            --   UNITTYPEFORPREADJUSTEDVALUE,
            --   PROCESSINGUNITSEQ,
            --   compensationDate,
            --   gv_pipelineRunSeq,
            --   unittypeForLineNumber,
            --   unitTypeForSubLineNumber,
            --   businessUnitMap,
            --   --GENERIC FIELDS
            --   productId,
            --   productName,
            --   productDescription,
            --   dataSource,
            --   genericAttribute1,
            --   genericAttribute2,
            --   genericAttribute3,
            --   genericAttribute4,
            --   genericAttribute5,
            --   genericAttribute6,
            --   genericAttribute7,
            --   genericAttribute8,
            --   genericAttribute9,
            --   genericAttribute10,
            --   genericAttribute11,
            --   genericAttribute12,
            --   genericAttribute13,
            --   genericAttribute14,
            --   genericAttribute15,
            --   genericAttribute16,
            --   genericAttribute17,
            --   genericAttribute18,
            --   genericAttribute19,
            --   genericAttribute20,
            --   genericAttribute21,
            --   genericAttribute22,
            --   genericAttribute23,
            --   genericAttribute24,
            --   genericAttribute25,
            --   genericAttribute26,
            --   genericAttribute27,
            --   genericAttribute28,
            --   genericAttribute29,
            --   genericAttribute30,
            --   genericAttribute31,
            --   genericAttribute32,
            --   genericNumber1,
            --   genericNumber2,
            --   genericNumber3,
            --   genericNumber4,
            --   genericNumber5,
            --   genericNumber6,
            --   unitTypeForGenericNumber1,
            --   unitTypeForGenericNumber2,
            --   unitTypeForGenericNumber3,
            --   unitTypeForGenericNumber4,
            --   unitTypeForGenericNumber5,
            --   unitTypeForGenericNumber6,
            --   genericDate1,
            --   genericDate2,
            --   genericDate3,
            --   genericDate4,
            --   genericDate5,
            --   genericBoolean1,
            --   genericBoolean2,
            --   genericBoolean3,
            --   genericBoolean4,
            --   genericBoolean5,
            --   GENERICBOOLEAN6
            --   --
        --   )
        ----gaSalestransaction
        --WHEN salestransactionSeq > 0 then into cs_gasalestransaction
        --  (tenantid, salestransactionSeq,
            --   pagenumber,
            -- --Added by Suresh 10292017
            --   PROCESSINGUNITSEQ,
            --   compensationDate,
            --   --End by Suresh 10292017
            --   GENERICATTRIBUTE1,
            --   GENERICATTRIBUTE2,
            --   GENERICATTRIBUTE3,
            --   GENERICATTRIBUTE4,
            --   GENERICATTRIBUTE5,
            --   GENERICATTRIBUTE6,
            --   GENERICATTRIBUTE7,
            --   GENERICATTRIBUTE8,
            --   GENERICATTRIBUTE9,
            --   GENERICATTRIBUTE10,
            --   GENERICATTRIBUTE11,
            --   GENERICATTRIBUTE12,
            --   GENERICATTRIBUTE13,
            --   GENERICATTRIBUTE14,
            --   GENERICATTRIBUTE15,
            --   GENERICATTRIBUTE16,
            --   GENERICATTRIBUTE17,
            --   GENERICATTRIBUTE18,
            --   GENERICATTRIBUTE19,
            --   GENERICATTRIBUTE20,
            --   GENERICDATE1,
            --   GENERICDATE2,
            --   GENERICDATE3,
            --   GENERICDATE4,
            --   GENERICDATE5,
            --   GENERICDATE6,
            --   GENERICDATE7,
            --   GENERICDATE8,
            --   GENERICDATE9,
            --   GENERICDATE10,
            --   GENERICDATE11,
            --   GENERICDATE12,
            --   GENERICDATE13,
            --   GENERICDATE14,
            --   GENERICDATE15,
            --   GENERICDATE16,
            --   GENERICDATE17,
            --   GENERICDATE18,
            --   GENERICDATE19,
            --   GENERICDATE20,
            --   GENERICBOOLEAN1,
            --   GENERICBOOLEAN2,
            --   GENERICBOOLEAN3,
            --   GENERICBOOLEAN4,
            --   GENERICBOOLEAN5,
            --   GENERICBOOLEAN6,
            --   GENERICBOOLEAN7,
            --   GENERICBOOLEAN8,
            --   GENERICBOOLEAN9,
            --   GENERICBOOLEAN10,
            --   GENERICBOOLEAN11,
            --   GENERICBOOLEAN12,
            --   GENERICBOOLEAN13,
            --   GENERICBOOLEAN14,
            --   GENERICBOOLEAN15,
            --   GENERICBOOLEAN16,
            --   GENERICBOOLEAN17,
            --   GENERICBOOLEAN18,
            --   GENERICBOOLEAN19,
            --   GENERICBOOLEAN20,
            --   GENERICNUMBER1,
            --   UNITTYPEFORGENERICNUMBER1,
            --   GENERICNUMBER2,
            --   UNITTYPEFORGENERICNUMBER2,
            --   GENERICNUMBER3,
            --   UNITTYPEFORGENERICNUMBER3,
            --   GENERICNUMBER4,
            --   UNITTYPEFORGENERICNUMBER4,
            --   GENERICNUMBER5,
            --   UNITTYPEFORGENERICNUMBER5,
            --   GENERICNUMBER6,
            --   UNITTYPEFORGENERICNUMBER6,
            --   GENERICNUMBER7,
            --   UNITTYPEFORGENERICNUMBER7,
            --   GENERICNUMBER8,
            --   UNITTYPEFORGENERICNUMBER8,
            --   GENERICNUMBER9,
            --   UNITTYPEFORGENERICNUMBER9,
            --   GENERICNUMBER10,
            --   UNITTYPEFORGENERICNUMBER10,
            --   GENERICNUMBER11,
            --   UNITTYPEFORGENERICNUMBER11,
            --   GENERICNUMBER12,
            --   UNITTYPEFORGENERICNUMBER12,
            --   GENERICNUMBER13,
            --   UNITTYPEFORGENERICNUMBER13,
            --   GENERICNUMBER14,
            --   UNITTYPEFORGENERICNUMBER14,
            --   GENERICNUMBER15,
            --   UNITTYPEFORGENERICNUMBER15,
            --   GENERICNUMBER16,
            --   UNITTYPEFORGENERICNUMBER16,
            --   GENERICNUMBER17,
            --   UNITTYPEFORGENERICNUMBER17,
            --   GENERICNUMBER18,
            --   UNITTYPEFORGENERICNUMBER18,
            --   GENERICNUMBER19,
            --   UNITTYPEFORGENERICNUMBER19,
            --   GENERICNUMBER20,
            --   UNITTYPEFORGENERICNUMBER20
        --   )
        --values
        --  ('AIAS',salestransactionseq,
            --   0,
            -- --Added by Suresh 10292017
            --   PROCESSINGUNITSEQ,
            --   compensationDate,
            --   --End by Suresh 10292017
            --   GA1,
            --   GA2,
            --   GA3,
            --   GA4,
            --   GA5,
            --   GA6,
            --   GA7,
            --   GA8,
            --   GA9,
            --   GA10,
            --   GA11,
            --   GA12,
            --   GA13,
            --   GA14,
            --   GA15,
            --   GA16,
            --   GA17,
            --   GA18,
            --   GA19,
            --   GA20,
            --   GD1,
            --   GD2,
            --   GD3,
            --   GD4,
            --   GD5,
            --   GD6,
            --   GD7,
            --   GD8,
            --   GD9,
            --   GD10,
            --   GD11,
            --   GD12,
            --   GD13,
            --   GD14,
            --   GD15,
            --   GD16,
            --   GD17,
            --   GD18,
            --   GD19,
            --   GD20,
            --   GB1,
            --   GB2,
            --   GB3,
            --   GB4,
            --   GB5,
            --   GB6,
            --   GB7,
            --   GB8,
            --   GB9,
            --   GB10,
            --   GB11,
            --   GB12,
            --   GB13,
            --   GB14,
            --   GB15,
            --   GB16,
            --   GB17,
            --   GB18,
            --   GB19,
            --   GB20,
            --   GN1,
            --   UNITTYPEFORGN1,
            --   GN2,
            --   UNITTYPEFORGN2,
            --   GN3,
            --   UNITTYPEFORGN3,
            --   GN4,
            --   UNITTYPEFORGN4,
            --   GN5,
            --   UNITTYPEFORGN5,
            --   GN6,
            --   UNITTYPEFORGN6,
            --   GN7,
            --   UNITTYPEFORGN7,
            --   GN8,
            --   UNITTYPEFORGN8,
            --   GN9,
            --   UNITTYPEFORGN9,
            --   GN10,
            --   UNITTYPEFORGN10,
            --   GN11,
            --   UNITTYPEFORGN11,
            --   GN12,
            --   UNITTYPEFORGN12,
            --   GN13,
            --   UNITTYPEFORGN13,
            --   GN14,
            --   UNITTYPEFORGN14,
            --   GN15,
            --   UNITTYPEFORGN15,
            --   GN16,
            --   UNITTYPEFORGN16,
            --   GN17,
            --   UNITTYPEFORGN17,
            --   GN18,
            --   UNITTYPEFORGN18,
            --   GN19,
            --   UNITTYPEFORGN19,
            --   GN20,
        --   UNITTYPEFORGN20)
        --create new writing agency txta

        --version 8 only insert into piaor_assignment
        /* RESOLVE: Syntax not supported in target DBMS: INSERT-WHEN-INTO; not supported in target DBMS, manual conversion required */
      
      /*Deepan : Replacing insert all with individual inserts for HANA*/
        -- insert ALL WHEN salestransactionSeq > 0 THEN into PIAOR_Assignment
        --     (tenantid,salestransactionSeq,
        --         salesOrderSeq,
        --         setNumber,
        --         positionName,
        --         compensationDate,
        --         Genericattribute4,
        --         Genericattribute5,
        --         Genericattribute6,
        --         Genericattribute7,
        --         Genericattribute8,
        --         Genericattribute9,
        --         Genericdate6,
        --     PROCESSINGUNITSEQ)
        -- VALUES
        --     ('AIAS',Salestransactionseq,
        --         Salesorderseq,
        --         1,
        --         Positionname,
        --         :v_compDate,
        --         'PI',
        --         'PI - Direct Team',
        --         Ga6_Wagency,
        --         Ga7_Incepttitle,
        --         Ga8_Wdistrict,
        --         Ga9_Yrend,
        --         Compensationdate,
        --     PROCESSINGUNITSEQ)
        --     --create new writing distrcit txta
        --     WHEN salestransactionSeq > 0 THEN into PIAOR_Assignment--cs_transactionAssignment
        --     (tenantid, salestransactionSeq,
        --         salesOrderSeq,
        --         setNumber,
        --         positionName,
        --         compensationDate,
        --         Genericattribute4,
        --         Genericattribute5,
        --         Genericattribute6,
        --         Genericattribute7,
        --         Genericattribute8,
        --         Genericattribute9,
        --     Genericdate6,PROCESSINGUNITSEQ)
        -- VALUES
        --     ('AIAS', Salestransactionseq,
        --         Salesorderseq,
        --         2,
        --         Ga8_Wdistrict,
        --         :v_compDate,
        --         'PI',
        --         'PI - Indirect Team',
        --         Ga6_Wagency,
        --         Ga7_Incepttitle,
        --         Ga8_Wdistrict,
        --         Ga9_Yrend,
        --     Compensationdate, processingunitseq)
        --     WHEN salestransactionSeq > 0 THEN into AIA_YrEnd_Tran_rela
        -- VALUES
        --     (Salestransactionseq,
        --         oldtrxnseq
        --     )
        --     /* ORIGSQL: Select / *+ LEADING(ta,st,gata) USE_NL(ta,st) USE_NL(ta,cp) INDEX(ta,aia_cs_transactionassign_idx2) no_expand * / */
        --     SELECT   /* ORIGSQL: Select ROW_NUMBER() OVER (ORDER BY 0*0) as rn, v_txnSeq + ROW_NUMBER() OVER (ORDER BY 0*0) As Salestransactionseq, st.salestransactionseq as oldtrxnseq, ta.salesorderseq, ta.compensationDate, nvl(v_mo(...) */
        --         ROW_NUMBER() OVER (ORDER BY 0*0) AS rn,  /* ORIGSQL: rownum */
        --         :v_txnSeq + ROW_NUMBER() OVER (ORDER BY 0*0) AS Salestransactionseq,  /* ORIGSQL: rownum */
        --         st.salestransactionseq AS oldtrxnseq,
        --         ta.salesorderseq,
        --         ta.compensationDate,
        --         IFNULL(:v_modificationTime, st.modificationDate) AS modificationDate,  /* ORIGSQL: nvl(v_modificationTime, st.modificationDate) */
        --         ta.PositionName,
        --         Ta.Genericattribute4 AS Ga4_Piaor,
        --         Ta.Genericattribute6 AS Ga6_Wagency,
        --         'FSAD' AS GA7_inceptTitle /* --only assign as FSAD title */, ta.genericAttribute8 AS GA8_WDistrict,
        --         'YE REASSIGN TO DISTRICT' AS GA9_yrEnd,
        --         cp.genericAttribute11 AS curTitle /* -- new title */, cpa.genericAttribute1 AS status,
        --         st.eventtypeSeq,
        --         st.linenumber,
        --         maxsublinenumber + ROW_NUMBER() OVER (ORDER BY 0*0) AS SUBLINENUMBER,  /* ORIGSQL: rownum */
        --         st.value,
        --         st.unittypeforvalue,
        --         1 AS isRunnable,
        --         st.ORIGINTYPEID,
        --         st.PREADJUSTEDVALUE,
        --         st.UNITTYPEFORPREADJUSTEDVALUE,
        --         st.PROCESSINGUNITSEQ,
        --         st.BusinessUnitMap,
        --         :Gv_Pipelinerunseq AS pipelineRunSeq,
        --         st.unittypeForLineNumber,
        --         st.unitTypeForSubLineNumber,
        --         /* --genericFields */
        --         st.productId,
        --         st.productName,
        --         st.productDescription,
        --         st.dataSource,
        --         st.genericAttribute1,
        --         st.genericAttribute2,
        --         st.genericAttribute3,
        --         st.genericAttribute4,
        --         st.genericAttribute5,
        --         st.genericAttribute6,
        --         st.genericAttribute7,
        --         st.genericAttribute8,
        --         st.genericAttribute9,
        --         st.genericAttribute10,
        --         st.genericAttribute11,
        --         st.genericAttribute12,
        --         st.genericAttribute13,
        --         st.genericAttribute14,
        --         st.genericAttribute15,
        --         st.genericAttribute16,
        --         st.genericAttribute17,
        --         st.genericAttribute18,
        --         st.genericAttribute19,
        --         st.genericAttribute20,
        --         st.genericAttribute21,
        --         st.genericAttribute22,
        --         st.genericAttribute23,
        --         st.genericAttribute24,
        --         st.genericAttribute25,
        --         st.genericAttribute26,
        --         st.genericAttribute27,
        --         st.genericAttribute28,
        --         st.genericAttribute29,
        --         st.genericAttribute30,
        --         st.genericAttribute31,
        --         st.genericAttribute32,
        --         st.genericNumber1,
        --         st.genericNumber2,
        --         st.genericNumber3,
        --         st.genericNumber4,
        --         st.genericNumber5,
        --         st.genericNumber6,
        --         st.unitTypeForGenericNumber1,
        --         st.unitTypeForGenericNumber2,
        --         st.unitTypeForGenericNumber3,
        --         st.unitTypeForGenericNumber4,
        --         st.unitTypeForGenericNumber5,
        --         st.unitTypeForGenericNumber6,
        --         st.genericDate1,
        --         st.genericDate2,
        --         st.genericDate3,
        --         st.genericDate4,
        --         st.genericDate5,
        --         st.genericBoolean1,
        --         st.genericBoolean2,
        --         st.genericBoolean3,
        --         st.genericBoolean4,
        --         st.genericBoolean5,
        --         st.GENERICBOOLEAN6,
        --         /* ----extend generic fields */
        --         gata.GENERICATTRIBUTE1 AS GA1,
        --         gata.GENERICATTRIBUTE2 AS GA2,
        --         gata.GENERICATTRIBUTE3 AS GA3,
        --         gata.GENERICATTRIBUTE4 AS GA4,
        --         gata.GENERICATTRIBUTE5 AS GA5,
        --         gata.GENERICATTRIBUTE6 AS GA6,
        --         gata.GENERICATTRIBUTE7 AS GA7,
        --         gata.GENERICATTRIBUTE8 AS GA8,
        --         gata.GENERICATTRIBUTE9 AS GA9,
        --         gata.GENERICATTRIBUTE10 AS GA10,
        --         gata.GENERICATTRIBUTE11 AS GA11,
        --         gata.GENERICATTRIBUTE12 AS GA12,
        --         gata.GENERICATTRIBUTE13 AS GA13,
        --         gata.GENERICATTRIBUTE14 AS GA14,
        --         gata.GENERICATTRIBUTE15 AS GA15,
        --         gata.GENERICATTRIBUTE16 AS GA16,
        --         gata.GENERICATTRIBUTE17 AS GA17,
        --         gata.GENERICATTRIBUTE18 AS GA18,
        --         gata.GENERICATTRIBUTE19 AS GA19,
        --         gata.GENERICATTRIBUTE20 AS GA20,
        --         gata.GENERICDATE1 AS GD1,
        --         gata.GENERICDATE2 AS GD2,
        --         gata.GENERICDATE3 AS GD3,
        --         gata.GENERICDATE4 AS GD4,
        --         gata.GENERICDATE5 AS GD5,
        --         gata.GENERICDATE6 AS GD6,
        --         gata.GENERICDATE7 AS GD7,
        --         gata.GENERICDATE8 AS GD8,
        --         gata.GENERICDATE9 AS GD9,
        --         gata.GENERICDATE10 AS GD10,
        --         gata.GENERICDATE11 AS GD11,
        --         gata.GENERICDATE12 AS GD12,
        --         gata.GENERICDATE13 AS GD13,
        --         gata.GENERICDATE14 AS GD14,
        --         gata.GENERICDATE15 AS GD15,
        --         gata.GENERICDATE16 AS GD16,
        --         gata.GENERICDATE17 AS GD17,
        --         gata.GENERICDATE18 AS GD18,
        --         gata.GENERICDATE19 AS GD19,
        --         gata.GENERICDATE20 AS GD20,
        --         gata.GENERICBOOLEAN1 AS GB1,
        --         gata.GENERICBOOLEAN2 AS GB2,
        --         gata.GENERICBOOLEAN3 AS GB3,
        --         gata.GENERICBOOLEAN4 AS GB4,
        --         gata.GENERICBOOLEAN5 AS GB5,
        --         gata.GENERICBOOLEAN6 AS GB6,
        --         gata.GENERICBOOLEAN7 AS GB7,
        --         gata.GENERICBOOLEAN8 AS GB8,
        --         gata.GENERICBOOLEAN9 AS GB9,
        --         gata.GENERICBOOLEAN10 AS GB10,
        --         gata.GENERICBOOLEAN11 AS GB11,
        --         gata.GENERICBOOLEAN12 AS GB12,
        --         gata.GENERICBOOLEAN13 AS GB13,
        --         gata.GENERICBOOLEAN14 AS GB14,
        --         gata.GENERICBOOLEAN15 AS GB15,
        --         gata.GENERICBOOLEAN16 AS GB16,
        --         gata.GENERICBOOLEAN17 AS GB17,
        --         gata.GENERICBOOLEAN18 AS GB18,
        --         gata.GENERICBOOLEAN19 AS GB19,
        --         gata.GENERICBOOLEAN20 AS GB20,
        --         gata.GENERICNUMBER1 AS GN1,
        --         gata.UNITTYPEFORGENERICNUMBER1 AS UNITTYPEFORGN1,
        --         gata.GENERICNUMBER2 AS GN2,
        --         gata.UNITTYPEFORGENERICNUMBER2 AS UNITTYPEFORGN2,
        --         gata.GENERICNUMBER3 AS GN3,
        --         gata.UNITTYPEFORGENERICNUMBER3 AS UNITTYPEFORGN3,
        --         gata.GENERICNUMBER4 AS GN4,
        --         gata.UNITTYPEFORGENERICNUMBER4 AS UNITTYPEFORGN4,
        --         gata.GENERICNUMBER5 AS GN5,
        --         gata.UNITTYPEFORGENERICNUMBER5 AS UNITTYPEFORGN5,
        --         gata.GENERICNUMBER6 AS GN6,
        --         gata.UNITTYPEFORGENERICNUMBER6 AS UNITTYPEFORGN6,
        --         gata.GENERICNUMBER7 AS GN7,
        --         gata.UNITTYPEFORGENERICNUMBER7 AS UNITTYPEFORGN7,
        --         gata.GENERICNUMBER8 AS GN8,
        --         gata.UNITTYPEFORGENERICNUMBER8 AS UNITTYPEFORGN8,
        --         gata.GENERICNUMBER9 AS GN9,
        --         gata.UNITTYPEFORGENERICNUMBER9 AS UNITTYPEFORGN9,
        --         gata.GENERICNUMBER10 AS GN10,
        --         gata.UNITTYPEFORGENERICNUMBER10 AS UNITTYPEFORGN10,
        --         gata.GENERICNUMBER11 AS GN11,
        --         gata.UNITTYPEFORGENERICNUMBER11 AS UNITTYPEFORGN11,
        --         gata.GENERICNUMBER12 AS GN12,
        --         gata.UNITTYPEFORGENERICNUMBER12 AS UNITTYPEFORGN12,
        --         gata.GENERICNUMBER13 AS GN13,
        --         gata.UNITTYPEFORGENERICNUMBER13 AS UNITTYPEFORGN13,
        --         gata.GENERICNUMBER14 AS GN14,
        --         gata.UNITTYPEFORGENERICNUMBER14 AS UNITTYPEFORGN14,
        --         gata.GENERICNUMBER15 AS GN15,
        --         gata.UNITTYPEFORGENERICNUMBER15 AS UNITTYPEFORGN15,
        --         gata.GENERICNUMBER16 AS GN16,
        --         gata.UNITTYPEFORGENERICNUMBER16 AS UNITTYPEFORGN16,
        --         gata.GENERICNUMBER17 AS GN17,
        --         gata.UNITTYPEFORGENERICNUMBER17 AS UNITTYPEFORGN17,
        --         gata.GENERICNUMBER18 AS GN18,
        --         gata.UNITTYPEFORGENERICNUMBER18 AS UNITTYPEFORGN18,
        --         gata.GENERICNUMBER19 AS GN19,
        --         gata.UNITTYPEFORGENERICNUMBER19 AS UNITTYPEFORGN19,
        --         gata.GENERICNUMBER20 AS GN20,
        --         gata.UNITTYPEFORGENERICNUMBER20 AS UNITTYPEFORGN20
        --     FROM
        --         /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
        --         EXT.Cs_Gasalestransaction AS Gata
        --     RIGHT OUTER JOIN
        --         EXT.cs_salestransaction AS st
        --         ON St.Salestransactionseq = Gata.Salestransactionseq  /* ORIGSQL: St.Salestransactionseq = Gata.Salestransactionseq(+) */
        --         AND gata.pagenumber = 0 -- and st.salestransactionseq=14636698977976073
        --         /* ORIGSQL: gata.pagenumber(+) = 0 */
        --         AND st.tenantid = gata.tenantid  /* ORIGSQL: st.tenantid = gata.tenantid(+) */
        --     INNER JOIN
        --         EXT.PIAOR_ASSIGNMENT AS ta
        --         ON st.salesTransactionSeq = ta.salesTransactionSeq
        --         AND ta.compensationDate >= :v_yrStartDate
        --         AND ta.Compensationdate < :v_yrEndDate
        --         AND ta.tenantid = 'AIAS' 
        --         AND ta.processingunitseq = :Gv_Processingunitseq
        --         AND Ta.Genericattribute5 = 'PI - Direct Team' -- changed check ga5, and only create trxn for pi-direct team
        --         /* ORIGSQL: Ta.Genericattribute5 = 'PI - Direct Team' */
        --         AND ta.genericAttribute7 = 'FSAD' 
        --         AND st.compensationdate = ta.compensationdate
        --     INNER JOIN
        --         EXT.cs_position AS cp
        --         ON ta.positionName = cp.name
        --         AND cp.tenantid = 'AIAS' 
        --         AND Cp.Effectivestartdate <= :v_compDate
        --         AND cp.effectiveEndDate > :v_compDate
        --         AND cp.removeDate = :cdt_EndOfTime
        --     INNER JOIN
        --         EXT.AIA_MAX_sublinenumber
        --         ON -- et.tenantid='AIAS'
        --         -- and et.eventTypeId in ('RYC', 'H_RYC', 'ORYC')
        --         -- AND et.removeDate = cdt_EndOfTime
        --         -- and et.tenantid=st.tenantid
        --         -- And Et.Datatypeseq = St.Eventtypeseq
        --         -- AND
        --         AIA_MAX_sublinenumber.salesorderseq = st.salesorderseq  /* ORIGSQL: AIA_MAX_sublinenumber.salesorderseq=st.salesorderseq */
        --     INNER JOIN
        --         EXT.cs_participant AS cpa
        --         ON ((Cp.Genericattribute11 IN ('FSAD', 'FSD')
        --                 AND Cpa.Genericattribute1 NOT IN ('00', '0'))
        --             OR (CP.Genericattribute11 IN ('AM', 'FSM', 'FSC')
        --                 AND CPa.Genericattribute1 IN ('00', '0')))
        --         AND cpa.tenantid = 'AIAS' 
        --         AND Cpa.Effectivestartdate <= :v_compDate
        --         AND Cpa.Effectiveenddate > :v_compDate
        --         AND cpa.removeDate = :cdt_EndOfTime
        --         AND Cp.Payeeseq = Cpa.Payeeseq
        --         --             cs_eventType             et,
        --     WHERE
        --         St.Compensationdate < :v_yrEndDate
        --         AND St.Eventtypeseq IN (:v1,:v2,:v3)
        --         AND st.compensationDate >= :v_yrStartDate
        --         AND st.genericdate2 < to_char('12/1/2015', 'mm/dd/yyyy')  /* ORIGSQL: to_date('12/1/2015', 'mm/dd/yyyy') */
        --         AND st.processingunitseq = :Gv_Processingunitseq
        --         AND st.tenantid = 'AIAS';

/*Deepan : New logic replacing insert all with individual inserts for HANA*/
-- First Insert Statement: PIAOR_Assignment for Direct Team
INSERT INTO PIAOR_Assignment
    (tenantid, salesTransactionSeq, salesOrderSeq, setNumber, positionName, compensationDate, 
    Genericattribute4, Genericattribute5, Genericattribute6, Genericattribute7, 
    Genericattribute8, Genericattribute9, Genericdate6, PROCESSINGUNITSEQ)
SELECT 
    'AIAS' AS tenantid,
    ROW_NUMBER() OVER (ORDER BY 0*0) + :v_txnSeq AS Salestransactionseq, -- Adjusted txnSeq
    ta.salesorderseq AS salesOrderSeq,
    1 AS setNumber,
    ta.PositionName,
    :v_compDate AS compensationDate,
    'PI' AS Genericattribute4,
    'PI - Direct Team' AS Genericattribute5,
    Ga6_Wagency,
    Ga7_Incepttitle,
    Ga8_Wdistrict,
    Ga9_Yrend,
    Compensationdate,
    ta.PROCESSINGUNITSEQ
FROM 
    cs_salestransaction AS st
INNER JOIN 
    EXT.PIAOR_ASSIGNMENT AS ta 
    ON st.salesTransactionSeq = ta.salesTransactionSeq
    AND ta.compensationDate >= :v_yrStartDate
    AND ta.Compensationdate < :v_yrEndDate
    AND ta.tenantid = 'AIAS'
    AND ta.processingunitseq = :Gv_Processingunitseq
    AND Ta.Genericattribute5 = 'PI - Direct Team'
    AND ta.genericAttribute7 = 'FSAD' 
    AND st.compensationdate = ta.compensationdate
INNER JOIN 
    cs_position AS cp 
    ON ta.positionName = cp.name
    AND cp.tenantid = 'AIAS'
    AND Cp.Effectivestartdate <= :v_compDate
    AND cp.effectiveEndDate > :v_compDate
    AND cp.removeDate = :cdt_EndOfTime
INNER JOIN 
    EXT.AIA_MAX_sublinenumber 
    ON AIA_MAX_sublinenumber.salesorderseq = st.salesorderseq
INNER JOIN 
    cs_participant AS cpa
    ON ((Cp.Genericattribute11 IN ('FSAD', 'FSD') AND Cpa.Genericattribute1 NOT IN ('00', '0'))
    OR (CP.Genericattribute11 IN ('AM', 'FSM', 'FSC') AND CPa.Genericattribute1 IN ('00', '0')))
    AND cpa.tenantid = 'AIAS'
    AND Cpa.Effectivestartdate <= :v_compDate
    AND Cpa.Effectiveenddate > :v_compDate
    AND cpa.removeDate = :cdt_EndOfTime
    AND Cp.Payeeseq = Cpa.Payeeseq
WHERE 
    st.Compensationdate < :v_yrEndDate
    AND St.Eventtypeseq IN (:v1, :v2, :v3)
    AND st.compensationDate >= :v_yrStartDate
    AND st.genericdate2 < TO_TIMESTAMP('2015-12-01', 'YYYY-MM-DD')  -- Converted date format for HANA
    AND st.processingunitseq = :Gv_Processingunitseq
    AND st.tenantid = 'AIAS';
        /* ORIGSQL: Log('31 get year end reassign data : '||SQL%ROWCOUNT) */
    CALL Log('PIAOR_Assignment for Direct Team- 31 get year end reassign data : '||::ROWCOUNT);  

-- Second Insert Statement: PIAOR_Assignment for Indirect Team
INSERT INTO PIAOR_Assignment
    (tenantid, salesTransactionSeq, salesOrderSeq, setNumber, positionName, compensationDate, 
    Genericattribute4, Genericattribute5, Genericattribute6, Genericattribute7, 
    Genericattribute8, Genericattribute9, Genericdate6, PROCESSINGUNITSEQ)
SELECT 
    'AIAS' AS tenantid,
    ROW_NUMBER() OVER (ORDER BY 0*0) + :v_txnSeq AS Salestransactionseq,
    ta.salesorderseq AS salesOrderSeq,
    2 AS setNumber,
    Ga8_Wdistrict AS PositionName,
    :v_compDate AS compensationDate,
    'PI' AS Genericattribute4,
    'PI - Indirect Team' AS Genericattribute5,
    Ga6_Wagency,
    Ga7_Incepttitle,
    Ga8_Wdistrict,
    Ga9_Yrend,
    Compensationdate,
    ta.PROCESSINGUNITSEQ
FROM 
    cs_salestransaction AS st
INNER JOIN 
    EXT.PIAOR_ASSIGNMENT AS ta
    ON st.salesTransactionSeq = ta.salesTransactionSeq
    AND ta.compensationDate >= :v_yrStartDate
    AND ta.Compensationdate < :v_yrEndDate
    AND ta.tenantid = 'AIAS'
    AND ta.processingunitseq = :Gv_Processingunitseq
    AND Ta.Genericattribute5 = 'PI - Direct Team'
    AND ta.genericAttribute7 = 'FSAD'
    AND st.compensationdate = ta.compensationdate
INNER JOIN 
    cs_position AS cp 
    ON ta.positionName = cp.name
    AND cp.tenantid = 'AIAS'
    AND Cp.Effectivestartdate <= :v_compDate
    AND cp.effectiveEndDate > :v_compDate
    AND cp.removeDate = :cdt_EndOfTime
INNER JOIN 
    EXT.AIA_MAX_sublinenumber 
    ON AIA_MAX_sublinenumber.salesorderseq = st.salesorderseq
INNER JOIN 
    cs_participant AS cpa
    ON ((Cp.Genericattribute11 IN ('FSAD', 'FSD') AND Cpa.Genericattribute1 NOT IN ('00', '0'))
    OR (CP.Genericattribute11 IN ('AM', 'FSM', 'FSC') AND CPa.Genericattribute1 IN ('00', '0')))
    AND cpa.tenantid = 'AIAS'
    AND Cpa.Effectivestartdate <= :v_compDate
    AND Cpa.Effectiveenddate > :v_compDate
    AND cpa.removeDate = :cdt_EndOfTime
    AND Cp.Payeeseq = Cpa.Payeeseq
WHERE 
    st.Compensationdate < :v_yrEndDate
    AND St.Eventtypeseq IN (:v1, :v2, :v3)
    AND st.compensationDate >= :v_yrStartDate
    AND st.genericdate2 < TO_TIMESTAMP('2015-12-01', 'YYYY-MM-DD')
    AND st.processingunitseq = :Gv_Processingunitseq
    AND st.tenantid = 'AIAS';

        /* ORIGSQL: Log('31 get year end reassign data : '||SQL%ROWCOUNT) */
    CALL Log('PIAOR_Assignment for Indirect Team-31 get year end reassign data : '||::ROWCOUNT);  

-- Third Insert Statement: AIA_YrEnd_Tran_rela
INSERT INTO AIA_YrEnd_Tran_rela
    (Salestransactionseq, oldtrxnseq)
SELECT 
    ROW_NUMBER() OVER (ORDER BY 0*0) + :v_txnSeq AS Salestransactionseq,
    st.salestransactionseq AS oldtrxnseq
FROM 
    cs_salestransaction AS st
INNER JOIN 
    EXT.PIAOR_ASSIGNMENT AS ta
    ON st.salesTransactionSeq = ta.salesTransactionSeq
    AND ta.compensationDate >= :v_yrStartDate
    AND ta.Compensationdate < :v_yrEndDate
    AND ta.tenantid = 'AIAS'
    AND ta.processingunitseq = :Gv_Processingunitseq
    AND Ta.Genericattribute5 = 'PI - Direct Team'
    AND ta.genericAttribute7 = 'FSAD'
    AND st.compensationdate = ta.compensationdate
WHERE 
    st.Compensationdate < :v_yrEndDate
    AND St.Eventtypeseq IN (:v1, :v2, :v3)
    AND st.compensationDate >= :v_yrStartDate
    AND st.genericdate2 < TO_TIMESTAMP('2015-12-01', 'YYYY-MM-DD')
    AND st.processingunitseq = :Gv_Processingunitseq
    AND st.tenantid = 'AIAS';


        /* ORIGSQL: Log('31 get year end reassign data : '||SQL%ROWCOUNT) */
    CALL Log('AIA_YrEnd_Tran_rela-Team-31 get year end reassign data : '||::ROWCOUNT); 

        /* ORIGSQL: commit; */
    COMMIT;

        --add value for new payee which contribute from old payee
        --   execute immediate 'delete from PIAOR_DETAIL where Component =''PI REASSIGN''';

        --version 9 add

        /* ORIGSQL: delete from PIAOR_DETAIL where period=v_periodSeq and Component ='PI REASSIGN'; */
        DELETE
        FROM
            PIAOR_DETAIL
        WHERE
            period = :v_periodSeq
            AND Component = 'PI REASSIGN';

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: insert into PIAOR_DETAIL(Period, PIAOR_Year, Component, Wri_Agency, Wri_Agent, Payee_Agency, Rule, PA_RYC, LF_RYC, RYC, Yearend_old_Payee) SELECT v_periodSeq, v_piaor_year, 'PI REASSIGN', f.Genericatt(...) */
        INSERT INTO PIAOR_DETAIL
            (
                Period,
                PIAOR_Year,
                Component,
                Wri_Agency,
                Wri_Agent,
                Payee_Agency,
                Rule,
                PA_RYC/* --version 9 */,
                LF_RYC/* --version 9 */,
                RYC,
                Yearend_old_Payee
            )
            SELECT   /* ORIGSQL: SELECT v_periodSeq, v_piaor_year, 'PI REASSIGN', f.Genericattribute6, s.genericAttribute12, f.positionName, f.genericAttribute5, sum(CASE WHEN s.PRODUCTNAME='PA' and f.setnumber = 1 THEN 0-s.value WHE(...) */
                :v_periodSeq,
                :v_piaor_year,
                'PI REASSIGN',
                f.Genericattribute6  /* --Wri_Agency */, s.genericAttribute12  /* --Wri_Agent */, f.positionName,
                f.genericAttribute5,
                /* --version 9 add production breakdown value */
                SUM(
                    CASE   /* ORIGSQL: sum(CASE WHEN s.PRODUCTNAME='PA' and f.setnumber = 1 THEN 0-s.value WHEN s.PRODUCTNAME='PA' and f.setnumber <>1 THEN s.value ELSE 0 END) */
                        WHEN s.PRODUCTNAME = 'PA'
                        AND F.SETNUMBER = 1
                        THEN 0-s.value /* --old payee PA value */
                        WHEN s.PRODUCTNAME = 'PA'
                        AND F.SETNUMBER <> 1
                        THEN s.value /* --new payee PA value */
                        ELSE 0
                    END
                ) AS PA_RYC,
                SUM(
                    CASE   /* ORIGSQL: sum(CASE WHEN s.PRODUCTNAME<>'PA' and f.setnumber = 1 THEN 0-s.value WHEN s.PRODUCTNAME<>'PA' and f.setnumber <>1 THEN s.value ELSE 0 END) */
                        WHEN s.PRODUCTNAME <> 'PA'
                        AND F.SETNUMBER = 1
                        THEN 0-s.value /* --old payee LF value */
                        WHEN s.PRODUCTNAME <> 'PA'
                        AND F.SETNUMBER <> 1
                        THEN s.value /* --new payee LF value */
                        ELSE 0
                    END
                ) AS LF_RYC,
                SUM(
                    CASE 
                        WHEN F.SETNUMBER = 1
                        THEN 0-s.value
                        ELSE s.value
                    END
                ),
                CASE
                    WHEN F.SETNUMBER = 2
                    THEN f2.positionName
                    ELSE NULL
                END
            FROM
                PIAOR_ASSIGNMENT f,
                cs_salestransaction s,
                PIAOR_ASSIGNMENT f2,
                AIA_YrEnd_Tran_rela rela
            WHERE
                f.genericAttribute4 LIKE '%PI%'
                AND f.Genericattribute9 = 'YE REASSIGN TO DISTRICT'
                AND f.COMPENSATIONDATE = :v_compDate
                --and f.salestransactionseq = s.salestransactionseq
                AND f.salestransactionseq = rela.salestransactionseq
                AND rela.oldtrxnseq = s.salestransactionseq
                AND f2.genericAttribute4 LIKE '%PI%'
                AND f2.Genericattribute9 = 'YE REASSIGN TO DISTRICT'
                AND f2.COMPENSATIONDATE = :v_compDate
                AND f2.salestransactionseq = f.salestransactionseq
                AND F2.SETNUMBER = 1
            GROUP BY
                f.Genericattribute6,
                s.genericAttribute12,
                f.positionName,
                f.genericAttribute5,
                CASE
                    WHEN F.SETNUMBER = 2
                    THEN f2.positionName
                    ELSE NULL
                END;

        /* ORIGSQL: log('32 update year end new payee data : '||SQL%ROWCOUNT) */
        CALL Log('32 update year end new payee data : '||::ROWCOUNT);  

        /* ORIGSQL: commit; */
        COMMIT;

        --UPDATE PIAOR PAYEE AGENT

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: Merge into PIAOR_DETAIL pd using (SELECT d.name, d.genericattribute2 AS Payee_agent FROM cs_position d where d.removedate = Cdt_Endoftime and d.effectivestartdate <= V_Compdate and d.effectiveenddate (...) */
        MERGE INTO PIAOR_DETAIL AS pd 
            USING
            (
                SELECT   /* ORIGSQL: (select d.name, d.genericattribute2 as Payee_agent from cs_position d where d.removedate = Cdt_Endoftime and d.effectivestartdate <= V_Compdate and d.effectiveenddate > V_Compdate) */
                    d.name,
                    d.genericattribute2 AS Payee_agent
                FROM
                    cs_position d
                WHERE
                    d.removedate = :cdt_EndOfTime
                    AND d.effectivestartdate <= :v_compDate
                    AND d.effectiveenddate > :v_compDate
            ) AS t
            ON (pd.Payee_Agency = t.name
            AND pd.piaor_year = :v_piaor_year)
        WHEN MATCHED THEN
            UPDATE SET pd.PAYEE_AGENT = t.Payee_agent;

        /* ORIGSQL: log('33 update payee agent : '||SQL%ROWCOUNT) */
        CALL Log('33 update payee agent : '||::ROWCOUNT);  

        /* ORIGSQL: Commit; */
        COMMIT;

        --UPDATE PIAOR STATUS

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: Merge into PIAOR_DETAIL pd using (SELECT d.name, p.genericattribute1 FROM cs_position d, cs_participant p where d.removedate = Cdt_Endoftime and d.effectivestartdate <= V_Compdate and d.effectiveendda(...) */
        MERGE INTO PIAOR_DETAIL AS pd 
            /* RESOLVE: Identifier not found: Table/view 'EXT.CS_PARTICIPANT' not found */
            USING
            (
                SELECT   /* ORIGSQL: (select d.name, p.genericattribute1 from cs_position d, cs_participant p where d.removedate = Cdt_Endoftime and d.effectivestartdate <= V_Compdate and d.effectiveenddate > V_Compdate and p.payeeseq = (...) */
                    d.name,
                    p.genericattribute1
                FROM
                    cs_position d,
                    cs_participant p
                WHERE
                    d.removedate = :cdt_EndOfTime
                    AND d.effectivestartdate <= :v_compDate
                    AND d.effectiveenddate > :v_compDate
                    AND p.payeeseq = d.payeeseq
                    AND p.removedate = :cdt_EndOfTime
                    AND p.effectivestartdate <= :v_compDate
                    AND p.effectiveenddate > :v_compDate
            ) AS t
            ON ('SGT'||IFNULL(pd.Payee_Agent,'') = t.name
            AND pd.piaor_year = :v_piaor_year)
        WHEN MATCHED THEN
            UPDATE SET pd.status = t.genericattribute1;

        /* ORIGSQL: log('34 update year end AOR status : '||SQL%ROWCOUNT) */
        CALL Log('34 update year end AOR status : '||::ROWCOUNT);  

        /* ORIGSQL: Commit; */
        COMMIT;

        --update all year PI data

        /* RESOLVE: MERGE statement: Additional manual conversion of MERGE INTO may be required */
        /* ORIGSQL: Merge into PIAOR_DETAIL pd using (SELECT d.name, g.genericboolean2, g.genericboolean3, d.genericattribute2, p.genericattribute1, d.genericattribute11 FROM cs_position d, cs_gaposition g, cs_participan(...) */
        MERGE INTO PIAOR_DETAIL AS pd 
            /* RESOLVE: Identifier not found: Table/view 'EXT.CS_GAPOSITION' not found */
            USING
            (
                SELECT   /* ORIGSQL: (select d.name, g.genericboolean2, g.genericboolean3, d.genericattribute2, p.genericattribute1, d.genericattribute11 from cs_position d, cs_gaposition g, cs_participant p where d.removedate = Cdt_Endo(...) */
                    d.name,
                    g.genericboolean2,
                    g.genericboolean3,
                    d.genericattribute2,
                    p.genericattribute1,
                    d.genericattribute11
                FROM
                    cs_position d,
                    cs_gaposition g,
                    cs_participant p
                WHERE
                    d.removedate = :cdt_EndOfTime
                    AND d.effectivestartdate <= :v_compDate
                    AND d.effectiveenddate > :v_compDate
                    AND d.ruleelementownerseq = g.ruleelementownerseq
                    AND g.removedate = :cdt_EndOfTime
                    AND g.effectivestartdate <= :v_compDate
                    AND g.effectiveenddate > :v_compDate
                    AND p.payeeseq = d.payeeseq
                    AND p.removedate = :cdt_EndOfTime
                    AND p.effectivestartdate <= :v_compDate
                    AND p.effectiveenddate > :v_compDate
            ) AS t
            ON ('SGT'||IFNULL(pd.Payee_Agent,'') = t.name
                AND pd.piaor_year = :v_piaor_year
            AND pd.component LIKE '%PI%')
        WHEN MATCHED THEN
            UPDATE SET pd.RI =
                CASE
                    WHEN t.genericboolean3 = 1
                    AND pd.rule = 'PI - Direct Team'
                    THEN 1
                    ELSE 0
                END,
                pd.CB =
                CASE
                    WHEN t.genericboolean2 = 1
                    AND t.genericattribute2 = pd.Wri_Agent
                    THEN 1
                    ELSE 0
                END,
                pd.FSAD_Exclude_Indirect =
                CASE
                    WHEN t.genericattribute11 = 'FSAD'
                    AND pd.rule = 'PI - Indirect Team'
                    THEN 1
                    ELSE 0
                END;

        /* ORIGSQL: log('35 update year end PI and AOR data : '||SQL%ROWCOUNT) */
        CALL Log('35 update year end PI and AOR data : '||::ROWCOUNT);  

        /* ORIGSQL: Commit; */
        COMMIT;

        --update aor status

        /* ORIGSQL: log('SP_TXNTXA_YREND_PI: end') */
        CALL Log('SP_TXNTXA_YREND_PI: end');

        --     SequenceGenPkg.UpdateSeq('auditLogSeq');

        /* ORIGSQL: exception when NO_YRENDEVENTTYPE_FOUND then */
    END;

    --version 8 add year end procedure

    --********** Creating procedure 'EXT.PK_PIAOR_CALCULATION:SP_YEAR_END_CALCULATION' ********************
    /* ORIGSQL: PROCEDURE SP_YEAR_END_CALCULATION As v_Periodseq int; */
PUBLIC PROCEDURE SP_YEAR_END_CALCULATION
()
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE v_Periodseq BIGINT;  /* ORIGSQL: v_Periodseq int; */
    DECLARE v_periodStartDate TIMESTAMP;  /* ORIGSQL: v_periodStartDate date; */
    DECLARE v_Periodenddate TIMESTAMP;  /* ORIGSQL: v_Periodenddate date; */
    DECLARE v_piaor_year VARCHAR(100);  /* ORIGSQL: v_piaor_year varchar2(100); */
    DECLARE v_rtn BIGINT = 0;  /* ORIGSQL: v_rtn int := 0; */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: When Others Then */
        BEGIN
            /* ORIGSQL: COMDEBUGGER('SP_YEAR_END_CALCULATION error: ', sqlerrm) */
            CALL comDebugger('SP_YEAR_END_CALCULATION error: ', ::SQL_ERROR_MESSAGE 
            );  /* ORIGSQL: sqlerrm */

            /* sapdbmtk: Closing RETURN in exception handler commented out, not supported in HANA */
            --RETURN;
        END;

        /* initialize library variables, if not yet done */
        CALL init_session_global();

        /* ORIGSQL: log('SP_YEAR_END_CALCULATION: start') */
        CALL Log('SP_YEAR_END_CALCULATION: start');

        /* ORIGSQL: log('gv_CYCLE_DATE: '||gv_CYCLE_DATE) */
        CALL Log('gv_CYCLE_DATE: '||IFNULL(:gv_CYCLE_DATE,''));

        /* ORIGSQL: log('gv_calendarSeq: '||gv_calendarSeq) */
        CALL Log('gv_calendarSeq: '||IFNULL(TO_VARCHAR(:gv_calendarSeq),''));

        --get period startDate, endDate  

        SELECT
            cp.PERIODSEQ,
            cp.startDate,
            cp.endDate
        INTO
            v_Periodseq,
            v_periodStartDate,
            v_Periodenddate
        FROM
            CS_PERIOD cp,
            cs_periodtype pt
        WHERE
            cp.tenantid = 'AIAS'
            AND cp.REMOVEDATE = :cdt_EndOfTime
            AND cp.CALENDARSEQ = :gv_calendarSeq
            AND cp.startdate <= to_char(:gv_CYCLE_DATE,'yyyy-mm-dd')  /* ORIGSQL: to_date(gv_CYCLE_DATE,'yyyy-mm-dd') */
            AND cp.enddate > to_char(:gv_CYCLE_DATE,'yyyy-mm-dd')  /* ORIGSQL: to_date(gv_CYCLE_DATE,'yyyy-mm-dd') */
            AND pt.name = 'month'
            AND pt.periodtypeseq = cp.periodtypeseq;

        v_rtn = comGetYrLastMonth(:v_Periodseq);

        IF :v_rtn < 1
        THEN
            /* ORIGSQL: log('SP_YEAR_END_CALCULATION: not year end month') */
            CALL Log('SP_YEAR_END_CALCULATION: not year end month'); 

            RETURN;
        END IF;

        IF :v_periodStartDate >= TO_DATE('2016-12-01')   /* ORIGSQL: date '2016-12-01' */
        AND :v_Periodenddate <= TO_DATE('2018-01-01') --for 2017
        /* ORIGSQL: date '2018-01-01' */
        THEN   
            SELECT
                SUBSTRING(C.NAME,1,4)  /* ORIGSQL: SUBSTR(C.NAME,1,4) */
            INTO
                v_piaor_year
            FROM
                CS_PERIOD A,
                CS_PERIOD B,
                CS_PERIOD C
            WHERE
                A.tenantid = 'AIAS'
                AND B.tenantid = 'AIAS'
                AND C.tenantid = 'AIAS'
                AND A.removeDate = :cdt_EndOfTime
                AND B.removeDate = :cdt_EndOfTime
                AND C.removeDate = :cdt_EndOfTime
                AND A.PERIODSEQ = :v_Periodseq
                AND A.calendarSeq = B.calendarSeq
                AND A.PARENTSEQ = B.PERIODSEQ
                AND B.calendarSeq = C.calendarSeq
                AND B.PARENTSEQ = C.PERIODSEQ;
        ELSE  
            SELECT
                EXTRACT(YEAR FROM startdate)
            INTO
                v_piaor_year
            FROM
                cs_period
            WHERE
                tenantid = 'AIAS'
                AND removeDate = :cdt_EndOfTime
                AND PERIODSEQ = :v_Periodseq;
        END IF;

        /* ORIGSQL: log('v_periodSeq: ' ||v_periodSeq) */
        CALL Log('v_periodSeq: '||IFNULL(TO_VARCHAR(:v_Periodseq),''));

        /* ORIGSQL: log('v_periodStartDate: '||v_periodStartDate) */
        CALL Log('v_periodStartDate: '||IFNULL(TO_VARCHAR(:v_periodStartDate),''));

        /* ORIGSQL: log('v_periodEndDate: '||v_periodEndDate) */
        CALL Log('v_periodEndDate: '||IFNULL(TO_VARCHAR(:v_Periodenddate),''));

        /* ORIGSQL: log('v_piaor_year: '||v_piaor_year) */
        CALL Log('v_piaor_year: '||IFNULL(:v_piaor_year,''));

        /* ORIGSQL: commit; */
        COMMIT;

        --init table

        /* ORIGSQL: delete from PIAOR_Payment where Year =v_piaor_year; */
        DELETE
        FROM
            PIAOR_Payment
        WHERE
            Year = :v_piaor_year;

        /* ORIGSQL: log('init table: '||sql%rowcount) */
        CALL Log('init table: '||::ROWCOUNT);  

        /* ORIGSQL: commit; */
        COMMIT;

        --AOR 

        /* ORIGSQL: insert into PIAOR_Payment(Year, District, Agency, Payee_Name, Payee_Code, Title, Inforce, Annual_PIB, Annual_RYC, type, PAYMENT_YEAR) WITH tmp_pos AS (SELECT * FROM cs_position where removeDate=cdt_En(...) */
        INSERT INTO PIAOR_Payment
            (
                Year,
                District,
                Agency,
                Payee_Name,
                Payee_Code,
                Title,
                Inforce,
                Annual_PIB,
                Annual_RYC,
                type,
                PAYMENT_YEAR
            )
            WITH 
            tmp_pos AS (
                SELECT   /* ORIGSQL: (select * from cs_position where removeDate=cdt_EndofTime and effectiveStartDate<=v_periodEndDate -1 and effectiveEndDate > v_periodEndDate-1) */
                    *
                FROM
                    cs_position
                WHERE
                    removeDate = :cdt_EndOfTime
                    AND effectiveStartDate <= TO_DATE(ADD_SECONDS(:v_Periodenddate,(86400*-1)))   /* ORIGSQL: v_periodEndDate -1 */
            AND effectiveEndDate > TO_DATE(ADD_SECONDS(:v_Periodenddate,(86400*-1)))    /* ORIGSQL: v_periodEndDate-1 */
            )
            SELECT   /* ORIGSQL: select pd.PIAOR_YEAR as Year, tp.genericattribute3 as District, pd.PAYEE_AGENCY as Agency, tp.genericattribute7 as Payee_Name, pd.PAYEE_AGENT as Payee_Code, tp.genericattribute11 as Title, CASE WHEN p(...) */
                pd.PIAOR_YEAR AS Year,
                tp.genericattribute3 AS District,
                pd.PAYEE_AGENCY AS Agency,
                tp.genericattribute7 AS Payee_Name,
                pd.PAYEE_AGENT AS Payee_Code,
                tp.genericattribute11 AS Title,
                CASE
                    WHEN pd.status IN ('00','0')
                    THEN 'Y'
                    ELSE 'N'
                END
                AS Inforce,
                /* --version 9 */
                /* --sum(pd.PIB) as Annual_PIB, */
                /* --sum(pd.RYC) as Annual_RYC, */
                SUM(IFNULL(pd.PIB,0)) AS Annual_PIB,  /* ORIGSQL: nvl(pd.PIB,0) */
                SUM(IFNULL(pd.RYC,0)) AS Annual_RYC,  /* ORIGSQL: nvl(pd.RYC,0) */
                pd.component AS type,
                TO_VARCHAR(CAST(pd.PIAOR_YEAR AS BIGINT) +1) AS PAYMENT_YEAR  /* ORIGSQL: to_char(cast(pd.PIAOR_YEAR AS integer)+1) */
            FROM
                piaor_detail pd,
                tmp_pos tp
            WHERE
                pd.PIAOR_YEAR = :v_piaor_year
                AND 'SGT'||IFNULL(pd.payee_agent,'') = tp.name
                AND pd.component = 'AOR'
            GROUP BY
                pd.PIAOR_YEAR,
                tp.genericattribute3,
                pd.PAYEE_AGENCY,
                tp.genericattribute7,
                pd.PAYEE_AGENT,
                tp.genericattribute11,
                pd.status,
                pd.component,
                CAST(pd.PIAOR_YEAR AS BIGINT) +1;  /* ORIGSQL: cast(pd.PIAOR_YEAR AS integer) */

        /* ORIGSQL: log('sum AOR detail: '||sql%rowcount) */
        CALL Log('sum AOR detail: '||::ROWCOUNT);  

        /* ORIGSQL: commit; */
        COMMIT;

        --PI 

        /* ORIGSQL: insert into PIAOR_Payment(Year, District, Agency, Payee_Name, Payee_Code, Title, Inforce, RI, CB, Annual_RYC, Persistency, type, PAYMENT_YEAR) WITH tmp_pos AS (SELECT * FROM cs_position where removeDa(...) */
        INSERT INTO PIAOR_Payment
            (
                Year,
                District,
                Agency,
                Payee_Name,
                Payee_Code,
                Title,
                Inforce,
                RI,
                CB,
                Annual_RYC,
                Persistency,
                type,
                PAYMENT_YEAR
            )
            WITH 
            tmp_pos AS (
                SELECT   /* ORIGSQL: (select * from cs_position where removeDate=cdt_EndofTime and effectiveStartDate<=v_periodEndDate -1 and effectiveEndDate > v_periodEndDate-1) */
                    *
                FROM
                    cs_position
                WHERE
                    removeDate = :cdt_EndOfTime
                    AND effectiveStartDate <= TO_DATE(ADD_SECONDS(:v_Periodenddate,(86400*-1)))   /* ORIGSQL: v_periodEndDate -1 */
            AND effectiveEndDate > TO_DATE(ADD_SECONDS(:v_Periodenddate,(86400*-1)))    /* ORIGSQL: v_periodEndDate-1 */
            )
            SELECT   /* ORIGSQL: select pd.PIAOR_YEAR as Year, tp.genericattribute3 as District, pd.PAYEE_AGENCY as Agency, tp.genericattribute7 as Payee_Name, pd.PAYEE_AGENT as Payee_Code, tp.genericattribute11 as Title, CASE WHEN p(...) */
                pd.PIAOR_YEAR AS Year,
                tp.genericattribute3 AS District,
                pd.PAYEE_AGENCY AS Agency,
                tp.genericattribute7 AS Payee_Name,
                pd.PAYEE_AGENT AS Payee_Code,
                tp.genericattribute11 AS Title,
                CASE
                    WHEN pd.status IN ('00','0')
                    THEN 'Y'
                    ELSE 'N'
                END
                AS Inforce,
                pd.RI AS RI,
                pd.CB AS CB,
                /* --version 9 */
                /* --sum(pd.RYC) as Annual_RYC, */
                SUM(IFNULL(pd.RYC,0)) AS Annual_RYC,  /* ORIGSQL: nvl(pd.RYC,0) */
                CASE
                    WHEN tp.genericattribute11 = 'FSD'
                    THEN tl1.PER_CC_P12
                    ELSE tl2.PER_CC_P12
                END
                AS Persistency,
                'PI' AS type,
                TO_VARCHAR(CAST(pd.PIAOR_YEAR AS BIGINT) +1) AS PAYMENT_YEAR  /* ORIGSQL: to_char(cast(pd.PIAOR_YEAR AS integer)+1) */
            FROM
                /* RESOLVE: Oracle Outer Join query (+): Oracle outer join syntax converted to ANSI join syntax, verify correct conversion */
                EXT.piaor_detail AS pd
            LEFT OUTER JOIN
                (
                    SELECT   /* ORIGSQL: (select agy,agt,per_cc_p12,agent_type from per_limra where cycle_mth = v_periodEndDate-1 and LIMRA_TYPE = 'LIMPI' and CMCD = 'SG' and agent_type='03' and agy not like 'A%') */
                        agy,
                        agt,
                        per_cc_p12,
                        agent_type
                    FROM
                        per_limra
                    WHERE
                        cycle_mth = TO_DATE(ADD_SECONDS(:v_Periodenddate,(86400*-1)))   /* ORIGSQL: v_periodEndDate-1 */
                        AND LIMRA_TYPE = 'LIMPI'
                        AND CMCD = 'SG'
                        AND agent_type = '03'
                        AND agy NOT LIKE 'A%' --version 11
                ) AS tl1
                ON pd.payee_agent = tl1.agt  /* ORIGSQL: pd.payee_agent=tl1.agt(+) */
            LEFT OUTER JOIN
                (
                    SELECT   /* ORIGSQL: (select agy,agt,per_cc_p12,agent_type from per_limra where cycle_mth = v_periodEndDate-1 and LIMRA_TYPE = 'LIMPI' and CMCD = 'SG' and agent_type='02' and agy not like 'A%') */
                        agy,
                        agt,
                        per_cc_p12,
                        agent_type
                    FROM
                        per_limra
                    WHERE
                        cycle_mth = TO_DATE(ADD_SECONDS(:v_Periodenddate,(86400*-1)))   /* ORIGSQL: v_periodEndDate-1 */
                        AND LIMRA_TYPE = 'LIMPI'
                        AND CMCD = 'SG'
                        AND agent_type = '02'
                        AND agy NOT LIKE 'A%' --version 11
                ) AS tl2
                ON pd.payee_agent = tl2.agt  /* ORIGSQL: pd.payee_agent=tl2.agt(+) */
            INNER JOIN
                EXT.tmp_pos AS tp
                ON 'SGT'||IFNULL(pd.payee_agent,'') = tp.name
            WHERE
                pd.PIAOR_YEAR = :v_piaor_year
                AND pd.CB <> 1
                AND pd.FSAD_Exclude_Indirect <> 1
                AND pd.RI <> 1
                AND pd.component IN ('PI','PI REASSIGN')
            GROUP BY
                pd.PIAOR_YEAR,
                tp.genericattribute3,
                pd.PAYEE_AGENCY,
                tp.genericattribute7,
                pd.PAYEE_AGENT,
                tp.genericattribute11,
                pd.status,
                pd.RI,
                pd.CB,
                CASE
                    WHEN tp.genericattribute11 = 'FSD'
                    THEN tl1.PER_CC_P12
                    ELSE tl2.PER_CC_P12
                END,
                CAST(pd.PIAOR_YEAR AS BIGINT) +1;  /* ORIGSQL: cast(pd.PIAOR_YEAR AS integer) */

        /* ORIGSQL: log('sum PI detail: '||sql%rowcount) */
        CALL Log('sum PI detail: '||::ROWCOUNT);  

        /* ORIGSQL: commit; */
        COMMIT;

        --update PI_rate 

        /* ORIGSQL: update PIAOR_Payment pp SET PI_rate = CASE WHEN pp.Persistency >= 0.9 THEN 0.3 WHEN pp.Persistency >= 0.85 THEN 0.25 WHEN pp.Persistency >= 0.8 THEN 0.15 ELSE 0 END where pp.Year=v_piaor_year and pp.t(...) */
        UPDATE PIAOR_Payment pp
            SET
            /* ORIGSQL: PI_rate = */
            PI_rate =
            CASE 
                WHEN pp.Persistency >= 0.9
                THEN 0.3
                WHEN pp.Persistency >= 0.85
                THEN 0.25
                WHEN pp.Persistency >= 0.8
                THEN 0.15
                ELSE 0
            END
        WHERE
            pp.Year = :v_piaor_year
            AND pp.type = 'PI'
            AND pp.Inforce = 'Y';

        /* ORIGSQL: log('update pi rate') */
        CALL Log('update pi rate');

        /* ORIGSQL: commit; */
        COMMIT;

        --update payment 

        /* ORIGSQL: update PIAOR_Payment pp SET pp.Payment= CASE WHEN pp.type ='PI' THEN (Annual_RYC*PI_rate)/12 ELSE Annual_PIB*AOR_PIB_Rate+Annual_RYC*AOR_RYC_Rate END where pp.year = v_piaor_year and pp.Inforce='Y' ; */
        UPDATE PIAOR_Payment pp
            SET
            /* ORIGSQL: pp.Payment = */
            Payment =
            CASE 
                WHEN pp.type = 'PI'
                THEN (Annual_RYC*PI_rate)/12
                ELSE Annual_PIB*:AOR_PIB_Rate+Annual_RYC*:AOR_RYC_Rate
            END
        WHERE
            pp.year = :v_piaor_year
            AND pp.Inforce = 'Y';

        /* ORIGSQL: log('update AOR and PI payment') */
        CALL Log('update AOR and PI payment');

        /* ORIGSQL: commit; */
        COMMIT;

        /* ORIGSQL: log('SP_YEAR_END_CALCULATION: end') */
        CALL Log('SP_YEAR_END_CALCULATION: end');

        /* ORIGSQL: Exception When Others Then */
    END;

    --********** Creating procedure 'EXT.PK_PIAOR_CALCULATION:SP_PIAOR_CALCULATION_ALL' ********************
    /* ORIGSQL: PROCEDURE SP_PIAOR_CALCULATION_ALL IS BEGIN */
PUBLIC PROCEDURE SP_PIAOR_CALCULATION_ALL
()
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* ORIGSQL: Log('SP_PIAOR_CALCULATION_ALL Started') */
    CALL Log('SP_PIAOR_CALCULATION_ALL Started');

    /* ORIGSQL: INIT */
    CALL init();

    /* ORIGSQL: SP_TXA_PIAOR */
    CALL SP_TXA_PIAOR();

    /* ORIGSQL: SP_MONTHLY_AGGREGATE */
    CALL SP_MONTHLY_AGGREGATE();

    /* ORIGSQL: SP_TXNTXA_YREND_PI */
    CALL SP_TXNTXA_YREND_PI();

    /* ORIGSQL: SP_YEAR_END_CALCULATION */
    CALL SP_YEAR_END_CALCULATION();

    /* ORIGSQL: Log('SP_PIAOR_CALCULATION_ALL Ended') */
    CALL Log('SP_PIAOR_CALCULATION_ALL Ended');

    /* ORIGSQL: COMMIT; */
    COMMIT;
END;

--********** Creating function 'EXT.PK_PIAOR_CALCULATION:comGetCrossoverAgy' ********************
/* ORIGSQL: function comGetCrossoverAgy (i_comp in varchar2,I_wAgyLdr in varchar2, i_policyIssueDate in date) return string is v_oldDM varchar2(30); */
PUBLIC FUNCTION comGetCrossoverAgy
(
    IN i_comp VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                              /* ORIGSQL: i_comp IN varchar2 */
    IN I_wAgyLdr VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                 /* ORIGSQL: I_wAgyLdr IN varchar2 */
    IN i_policyIssueDate TIMESTAMP     /* ORIGSQL: i_policyIssueDate IN date */
)
RETURNS dbmtk_function_result VARCHAR(255)   /* ORIGSQL: return string */
--add by nelson
/* RESOLVE: Manual edits required: VARCHAR2 function value(no length): user-configured length=255; adjust as needed */
SQL SECURITY DEFINER
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE v_oldDM VARCHAR(30);  /* ORIGSQL: v_oldDM varchar2(30); */
    DECLARE v_countsetup BIGINT;  /* ORIGSQL: v_countsetup number(10); */
    DECLARE v_odm VARCHAR(30);  /* ORIGSQL: v_odm varchar2(30); */
    DECLARE v_effdate TIMESTAMP;  /* ORIGSQL: v_effdate date; */

    DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
        /* ORIGSQL: when no_data_found then */
        BEGIN
            --Log('70'); 

            dbmtk_function_result = NULL;
            /* sapdbmtk: Closing RETURN in exception handler commented out, not supported in HANA */
            --RETURN;
        END;

        /* initialize library variables, if not yet done */
        CALL init_session_global();

        --Log('70');

        --add by nelson start

        IF :i_comp = 'PI' 
        THEN
            ---add version 5 (If Payee only include one setup , it will assign to old DM)
            --version 8 comment
            -- select count(1), max(TXTOLDDMCODE), max(Dteeffectivedate)
            --into v_countsetup, v_odm, v_effdate
            --   from (select max(ST.TXTOLDDMCODE) TXTOLDDMCODE,
                SELECT
                    COUNT(1),
                    MAX(Txtolddistrict),
                    MAX(Dteeffectivedate)
                INTO
                    v_countsetup,
                    v_odm,
                    v_effdate 
                FROM
                    (
                        SELECT   /* ORIGSQL: (select MAX(ST.Txtolddistrict) Txtolddistrict, ST.Dteeffectivedate, ST.Txtagt from In_Pi_Aor_Setup ST, Cs_Period PT where 'SGT' || to_number(ST.Txtagt) = I_wAgyLdr and I_Policyissuedate <= ST.Dteeffec(...) */
                            MAX(ST.Txtolddistrict) AS Txtolddistrict,
                            ST.Dteeffectivedate,
                            ST.Txtagt
                        FROM
                            In_Pi_Aor_Setup ST,
                            Cs_Period PT
                        WHERE
                            'SGT' || IFNULL(TO_VARCHAR(TO_DECIMAL(ST.Txtagt,38,18)),'') = :I_wAgyLdr  /* ORIGSQL: to_number(ST.Txtagt) */
                            AND :i_policyIssueDate <= ST.Dteeffectivedate
                            AND ST.Dtecycle = TO_DATE(ADD_SECONDS(PT.Enddate,(86400*-1)))   /* ORIGSQL: PT.Enddate - 1 */
                            AND PT.Periodseq = :Gv_Periodseq
                            AND ST.Txttype IN ('C')
                            AND ST.Decstatus = 0
                        GROUP BY
                            ST.Dteeffectivedate, ST.Txtagt
                    ) AS dbmtk_corrname_54256
                GROUP BY
                    Txtagt;

            IF :v_countsetup = 1
            THEN
                v_oldDM = :v_odm;

                gv_CrossoverEffectiveDate = :v_effdate;
            ELSE  
                SELECT
                    ST.Txtolddistrict,
                    ST.Dteeffectivedate
                INTO
                    v_oldDM,
                    gv_CrossoverEffectiveDate
                FROM
                    In_Pi_Aor_Setup ST,
                    Cs_Period PT
                WHERE
                    'SGT'||IFNULL(TO_VARCHAR(TO_DECIMAL(ST.Txtagt,38,18)),'') = :I_wAgyLdr  /* ORIGSQL: to_number(ST.Txtagt) */
                    AND ST.Dteeffectivedate  
                    =
                    (
                        SELECT   /* ORIGSQL: (Select MIN(S.Dteeffectivedate) From In_Pi_Aor_Setup S,Cs_Period P Where 'SGT'||to_number(S.Txtagt) =I_wAgyLdr And I_Policyissuedate <= S.Dteeffectivedate And S.Dtecycle = P.Enddate - 1 And P.Periodse(...) */
                            MIN(S.Dteeffectivedate) 
                        FROM
                            In_Pi_Aor_Setup S,
                            Cs_Period P
                        WHERE
                            'SGT'||IFNULL(TO_VARCHAR(TO_DECIMAL(S.Txtagt,38,18)),'') = :I_wAgyLdr  /* ORIGSQL: to_number(S.Txtagt) */
                            AND :i_policyIssueDate <= S.Dteeffectivedate
                            AND S.Dtecycle = TO_DATE(ADD_SECONDS(P.Enddate,(86400*-1)))   /* ORIGSQL: P.Enddate - 1 */
                            AND P.Periodseq = :Gv_Periodseq
                            AND S.Txttype IN ('C')
                            AND S.Decstatus = 0
                    )
                    AND ST.Dtecycle = TO_DATE(ADD_SECONDS(PT.Enddate,(86400*-1)))   /* ORIGSQL: PT.Enddate -1 */
                    AND PT.Periodseq = :Gv_Periodseq
                    AND :i_policyIssueDate >=
                    (
                        SELECT   /* ORIGSQL: (select MAX(g.effectiveenddate) from sh_agent_role g where g.agentcode = to_number(ST.Txtagt) and g.effectiveenddate < ST.dteeffectivedate) */
                            MAX(g.effectiveenddate)
                        FROM
                            sh_agent_role g
                        WHERE
                            g.agentcode = TO_DECIMAL(ST.Txtagt,38,18)  /* ORIGSQL: to_number(ST.Txtagt) */
                            AND g.effectiveenddate < ST.dteeffectivedate
                    );
            END IF;
            ELSEIF :i_comp = 'AOR'   /* ORIGSQL: elsif i_comp = 'AOR' then */
            THEN
                ---add version 5 (If Payee only include one setup , it will assign to old DM)
                SELECT
                    COUNT(1),
                    MAX(Txtolddistrict),
                    MAX(Dteeffectivedate)
                INTO
                    v_countsetup,
                    v_odm,
                    v_effdate 
                FROM
                    (
                        SELECT   /* ORIGSQL: (select MAX(ST.Txtolddistrict) Txtolddistrict, ST.Dteeffectivedate, ST.Txtagt from In_Pi_Aor_Setup ST, Cs_Period PT where 'SGT'|| to_number(ST.Txtagt) = I_wAgyLdr and I_Policyissuedate <= ST.Dteeffect(...) */
                            MAX(ST.Txtolddistrict) AS Txtolddistrict,
                            ST.Dteeffectivedate,
                            ST.Txtagt
                        FROM
                            In_Pi_Aor_Setup ST,
                            Cs_Period PT
                        WHERE
                            'SGT'|| IFNULL(TO_VARCHAR(TO_DECIMAL(ST.Txtagt,38,18)),'') = :I_wAgyLdr  /* ORIGSQL: to_number(ST.Txtagt) */
                            AND :i_policyIssueDate <= ST.Dteeffectivedate
                            AND ST.Dtecycle = TO_DATE(ADD_SECONDS(PT.Enddate,(86400*-1)))   /* ORIGSQL: PT.Enddate - 1 */
                            AND PT.Periodseq = :Gv_Periodseq
                            AND ST.Txttype IN ('C','D')
                            AND ST.Decstatus = 0
                        GROUP BY
                            ST.Dteeffectivedate, ST.Txtagt
                    ) AS dbmtk_corrname_54262
                GROUP BY
                    Txtagt;

                IF :v_countsetup = 1
                THEN
                    v_oldDM = :v_odm;

                    gv_CrossoverEffectiveDate = :v_effdate;
                ELSE  
                    SELECT
                        ST.Txtolddistrict,
                        ST.Dteeffectivedate
                    INTO
                        v_oldDM,
                        gv_CrossoverEffectiveDate
                    FROM
                        In_Pi_Aor_Setup ST,
                        Cs_Period PT
                    WHERE
                        PT.tenantid = 'AIAS'
                        AND 'SGT'||IFNULL(TO_VARCHAR(TO_DECIMAL(ST.Txtagt,38,18)),'') = :I_wAgyLdr  /* ORIGSQL: to_number(ST.Txtagt) */
                        AND ST.Dteeffectivedate  
                        =
                        (
                            SELECT   /* ORIGSQL: (Select MIN(S.Dteeffectivedate) From In_Pi_Aor_Setup S,Cs_Period P Where 'SGT'||to_number(S.Txtagt) =I_wAgyLdr And I_Policyissuedate <= S.Dteeffectivedate And S.Dtecycle = p.enddate-1 And P.Periodseq=(...) */
                                MIN(S.Dteeffectivedate) 
                            FROM
                                In_Pi_Aor_Setup S,
                                Cs_Period P
                            WHERE
                                'SGT'||IFNULL(TO_VARCHAR(TO_DECIMAL(S.Txtagt,38,18)),'') = :I_wAgyLdr  /* ORIGSQL: to_number(S.Txtagt) */
                                AND :i_policyIssueDate <= S.Dteeffectivedate
                                AND S.Dtecycle = TO_DATE(ADD_SECONDS(p.enddate,(86400*-1)))   /* ORIGSQL: p.enddate-1 */
                                AND P.Periodseq = :Gv_Periodseq
                                AND S.Txttype IN ('C','D')
                                AND S.Decstatus = 0
                        )
                        AND ST.Dtecycle = TO_DATE(ADD_SECONDS(PT.Enddate,(86400*-1)))   /* ORIGSQL: PT.Enddate - 1 */
                        AND PT.Periodseq = :Gv_Periodseq
                        AND :i_policyIssueDate >=
                        (
                            SELECT   /* ORIGSQL: (select MAX(g.effectiveenddate) from sh_agent_role g where g.agentcode = to_number(ST.Txtagt) and g.effectiveenddate < ST.dteeffectivedate) */
                                MAX(g.effectiveenddate)
                            FROM
                                sh_agent_role g
                            WHERE
                                g.agentcode = TO_DECIMAL(ST.Txtagt,38,18)  /* ORIGSQL: to_number(ST.Txtagt) */
                                AND g.effectiveenddate < ST.dteeffectivedate
                        );
                END IF;
            END IF;
            --Log('70'); 

            dbmtk_function_result = :v_oldDM;
            RETURN;

            --add by nelson end

            /* ORIGSQL: exception when no_data_found then */
        END;


    --********** Creating function 'EXT.PK_PIAOR_CALCULATION:Comgeteventtypeseq' ********************
    /* ORIGSQL: Function Comgeteventtypeseq(I_Eventtypeid In Varchar2) Return Int as v_eventtypeseq int; */
PUBLIC FUNCTION Comgeteventtypeseq
(
    IN I_Eventtypeid VARCHAR(255)   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                    /* ORIGSQL: I_Eventtypeid IN Varchar2 */
)
RETURNS dbmtk_function_result BIGINT   /* ORIGSQL: Return Int */
SQL SECURITY DEFINER
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE v_eventtypeseq BIGINT;  /* ORIGSQL: v_eventtypeseq int; */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: when others then */
        BEGIN 
            dbmtk_function_result = 0;
            /* sapdbmtk: Closing RETURN in exception handler commented out, not supported in HANA */
            --RETURN;
        END;

        SELECT
            datatypeseq
        INTO
            v_eventtypeseq
        FROM
            Cs_Eventtype
        WHERE
            Eventtypeid = :I_Eventtypeid
            AND removedate = :cdt_EndOfTime;

        dbmtk_function_result = :v_eventtypeseq;
        RETURN;

        /* ORIGSQL: exception when others then */
    END;

    --********** Creating procedure 'EXT.PK_PIAOR_CALCULATION:ComInitialpartition' ********************
    /* ORIGSQL: Procedure ComInitialpartition(I_Component In Varchar2, i_componentValue in varchar2, I_Periodseq In Int) As V_str Varchar2(1000); */
PUBLIC PROCEDURE ComInitialpartition
(
    IN I_Component VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                   /* ORIGSQL: I_Component IN Varchar2 */
    IN i_componentValue VARCHAR(255),   /* RESOLVE: Manual edits required: VARCHAR2 parameter(no length): user-configured length=255; adjust as needed */
                                        /* ORIGSQL: i_componentValue IN varchar2 */
    IN I_Periodseq BIGINT     /* ORIGSQL: I_Periodseq IN Int */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE V_str VARCHAR(1000);  /* ORIGSQL: V_str Varchar2(1000); */
    DECLARE V_Cnt BIGINT;  /* ORIGSQL: V_Cnt Int; */
    DECLARE v_partitionname VARCHAR(100) = 'SH_INITIAL_' || IFNULL(:I_Component,'');  /* ORIGSQL: v_partitionname varchar2(100) := 'SH_INITIAL_' || I_Component; */
    DECLARE V_subPartitionname VARCHAR(100) = UPPER('Sh_' || IFNULL(:I_Component,'') || '_' ||
        IFNULL(TO_VARCHAR(:I_Periodseq),''));  /* ORIGSQL: V_subPartitionname Varchar2(100) := UPPER('Sh_' || I_Component || '_' || I_Periodseq) ; */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: When Others Then */
        BEGIN
            --dbms_output.put_line(sqlerrm);

            /* ORIGSQL: COMDEBUGGER('CLEAN UP PARTTION ERROR', sqlerrm) */
            CALL comDebugger('CLEAN UP PARTTION ERROR', ::SQL_ERROR_MESSAGE 
            );  /* ORIGSQL: sqlerrm */

            /* sapdbmtk: Closing RETURN in exception handler commented out, not supported in HANA */
            --RETURN;
        END;

        /* ORIGSQL: DBMS_OUTPUT.PUT_LINE('CLEAN UP PARTTION' || V_subPartitionname || '---' || V_Cnt); */
        CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('CLEAN UP PARTTION' || IFNULL(:V_subPartitionname,'') ||
            '---' || IFNULL(TO_VARCHAR(:V_Cnt),''));

        SELECT
            COUNT(*) 
        INTO
            V_Cnt
        FROM
            SYS.TABLE_PARTITIONS  /* RESOLVE: Catalog reference(partly converted): Oracle catalog 'USER_TAB_SUBPARTITIONS': verify conversion */
                                  /* ORIGSQL: User_Tab_SUBPartitions (Oracle catalog) */
        WHERE
            LEVEL_2_PARTITION>0 and SCHEMA_NAME = CURRENT_USER
            AND UPPER(TABLE_NAME) = 'SH_QUERY_RESULT'   /* ORIGSQL: Table_Name (column in Oracle catalog 'USER_TAB_SUBPARTITIONS') */
            AND UPPER(subpartition_name) = :V_subPartitionname;

        --Comdebugger('CLEAN UP PARTTION',V_subPartitionname||'---'||V_Cnt);

        IF :V_Cnt = 0
        THEN
            V_str = 'alter table SH_QUERY_RESULT modify partition ' ||
            IFNULL(:v_partitionname,'') || ' add subpartition ' ||
            IFNULL(:V_subPartitionname,'') || ' values (' || IFNULL(TO_VARCHAR(:I_Periodseq),'') || ')';

            --dbms_output.put_line(v_Str);

            -- Comdebugger('CLEAN UP PARTTION',V_Str);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: execute immediate v_str; */
            EXECUTE IMMEDIATE :V_str;
        ELSE 
            V_str = 'ALTER TABLE SH_QUERY_RESULT truncate subpartition ' ||
            IFNULL(:V_subPartitionname,'');

            --Comdebugger('CLEAN UP PARTTION',V_Str);

            /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
            /* ORIGSQL: execute immediate v_str; */
            EXECUTE IMMEDIATE :V_str;
        END IF; 

        RETURN;

        /* ORIGSQL: Exception When Others Then */
    END;

    --version 8 add piaor_assignment initial procedure

    --********** Creating procedure 'EXT.PK_PIAOR_CALCULATION:AssignmentInitialpartition' ********************
    /* ORIGSQL: Procedure AssignmentInitialpartition(I_Periodseq In Int) As V_str Varchar2(1000); */
PUBLIC PROCEDURE AssignmentInitialpartition
(
    IN I_Periodseq BIGINT     /* ORIGSQL: I_Periodseq IN Int */
)
SQL SECURITY DEFINER
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE V_str VARCHAR(1000);  /* ORIGSQL: V_str Varchar2(1000); */
    DECLARE V_Cnt BIGINT;  /* ORIGSQL: V_Cnt Int; */
    DECLARE v_partitionname VARCHAR(100) = 'P_AIAS_';  /* ORIGSQL: v_partitionname varchar2(100) := 'P_AIAS_'; */
    DECLARE v_Periodenddate VARCHAR(100);  /* ORIGSQL: v_Periodenddate varchar2(100); */

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        /* ORIGSQL: When Others Then */
        BEGIN
            /* ORIGSQL: COMDEBUGGER('CLEAN UP PARTTION ERROR', sqlerrm) */
            CALL comDebugger('CLEAN UP PARTTION ERROR', ::SQL_ERROR_MESSAGE 
            );  /* ORIGSQL: sqlerrm */

            /* sapdbmtk: Closing RETURN in exception handler commented out, not supported in HANA */
            --RETURN;
        END;

        /* initialize library variables, if not yet done */
        CALL init_session_global();

        /* ORIGSQL: log('init start---') */
        CALL Log('init start---');

        --get Period startdate enddate rate 
        SELECT
            TO_VARCHAR(cs.enddate,'yyyymmdd')  /* ORIGSQL: to_char(cs.enddate,'yyyymmdd') */
        INTO
            v_Periodenddate
        FROM
            cs_period cs
        WHERE
            cs.tenantid = 'AIAS'
            AND cs.periodSeq = :I_Periodseq
            AND cs.Removedate = :cdt_EndOfTime
            AND cs.CALENDARSEQ = :gv_calendarSeq;

        /* ORIGSQL: log('v_Periodenddate: ' ||v_Periodenddate) */
        CALL Log('v_Periodenddate: '||IFNULL(:v_Periodenddate,''));

        v_partitionname = IFNULL(:v_partitionname,'')||IFNULL(:v_Periodenddate,'');

        SELECT
            COUNT(*) 
        INTO
            V_Cnt
        FROM
            SYS.TABLE_PARTITIONS  /* RESOLVE: Catalog reference(partly converted): Oracle catalog 'USER_TAB_PARTITIONS': verify conversion */
                                  /* ORIGSQL: User_Tab_Partitions (Oracle catalog) */
        WHERE
            SCHEMA_NAME = CURRENT_USER
            AND UPPER(TABLE_NAME) = 'PIAOR_ASSIGNMENT'   /* ORIGSQL: Table_Name (column in Oracle catalog 'USER_TAB_PARTITIONS') */
            AND UPPER(TO_NVARCHAR(PART_ID)) = :v_partitionname;  /* ORIGSQL: PARTITION_NAME (column in Oracle catalog 'USER_TAB_PARTITIONS') */

        /* ORIGSQL: log('v_partitionname: ' ||v_partitionname) */
        CALL Log('v_partitionname: '||IFNULL(:v_partitionname,''));

        /* ORIGSQL: log('V_Cnt: ' ||V_Cnt) */
        CALL Log('V_Cnt: '||IFNULL(TO_VARCHAR(:V_Cnt),''));

        /* ORIGSQL: DBMS_OUTPUT.PUT_LINE('CLEAN UP PARTTION' || v_partitionname || '---' || V_Cnt); */
        CALL sapdbmtk.sp_dbmtk_buffered_output_writeln('CLEAN UP PARTTION' || IFNULL(:v_partitionname,'') ||
            '---' || IFNULL(TO_VARCHAR(:V_Cnt),''));

        IF :V_Cnt = 0
        THEN
            V_str = 'alter table PIAOR_ASSIGNMENT add partition ' ||
            IFNULL(:v_partitionname,'') || ' values less than (''AIAS'',TO_DATE(''' || IFNULL(:v_Periodenddate,'')
            || ' 00:00:00'', ''YYYYMMDD HH24:MI:SS''))';
        ELSE 
            V_str = 'ALTER TABLE PIAOR_ASSIGNMENT truncate partition ' ||
            IFNULL(:v_partitionname,'');
        END IF;

        /* ORIGSQL: log('V_str: ' ||V_str) */
        CALL Log('V_str: '||IFNULL(:V_str,''));

        /* RESOLVE: Dynamically generated SQL: Dynamically generated SQL, as executed by Execute-Immediate, convert manually */
        /* ORIGSQL: execute immediate v_str; */
        EXECUTE IMMEDIATE :V_str;

        RETURN;

        /* ORIGSQL: Exception When Others Then */
    END;

    --********** Creating function 'EXT.PK_PIAOR_CALCULATION:comGetYrLastMonth' ********************
    /* ORIGSQL: function comGetYrLastMonth(i_periodSeq in int) return int is DECLARE NOT_YEAR_END CONDITION; */
PUBLIC FUNCTION comGetYrLastMonth
(
    IN i_periodSeq BIGINT     /* ORIGSQL: i_periodSeq IN int */
)
RETURNS dbmtk_function_result BIGINT   /* ORIGSQL: return int */
SQL SECURITY DEFINER
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE NOT_YEAR_END CONDITION;  /* ORIGSQL: NOT_YEAR_END EXCEPTION; */
    DECLARE v_periodTypeSeq BIGINT = 0;  /* ORIGSQL: v_periodTypeSeq int := 0; */
    DECLARE v_periodStartDate TIMESTAMP;  /* ORIGSQL: v_periodStartDate date; */

    DECLARE EXIT HANDLER FOR NOT_YEAR_END
        /* ORIGSQL: WHEN NOT_YEAR_END then */
        BEGIN
            gv_error = 'Info [PIAOR_Calculation]: The PIAOR Calculation will be skip in current period.'; 

            dbmtk_function_result = - 1;
            /* sapdbmtk: Closing RETURN in exception handler commented out, not supported in HANA */
            --RETURN;
        END;

        /* initialize library variables, if not yet done */
        CALL init_session_global();

         

    SELECT
        startdate
    INTO
        v_periodStartDate
    FROM
        cs_period
    WHERE
        tenantid = 'AIAS'
        AND periodSeq = :i_periodSeq
        AND Removedate = :cdt_EndOfTime
        AND CALENDARSEQ = :gv_calendarSeq;
    BEGIN 
        DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
            /* ORIGSQL: when no_data_found then */
            BEGIN 
                dbmtk_function_result = - 1;
                /* RESOLVE: Syntax not supported in target DBMS: RETURN in exception handler not supported in HANA, rewrite manually */
                RETURN;
            END;



        IF :v_periodStartDate <= TO_DATE('2016-12-01')  /* ORIGSQL: date '2016-12-01' */
        THEN 
            SELECT
                periodTypeSeq
            INTO
                v_periodTypeSeq
            FROM
                cs_period
            WHERE
                tenantid = 'AIAS'
                AND periodSeq = :i_periodSeq
                AND shortName = 'Nov'
                AND Removedate = :cdt_EndOfTime
                AND CALENDARSEQ = :gv_calendarSeq;
        ELSE  
            SELECT
                periodTypeSeq
            INTO
                v_periodTypeSeq
            FROM
                cs_period
            WHERE
                tenantid = 'AIAS'
                AND periodSeq = :i_periodSeq
                AND shortName = 'Dec'
                AND Removedate = :cdt_EndOfTime
                AND CALENDARSEQ = :gv_calendarSeq;
        END IF;

        /* ORIGSQL: exception when no_data_found then */
    END;

    IF :v_periodTypeSeq = NULL
    THEN
        /* ORIGSQL: raise NOT_YEAR_END; */
        SIGNAL NOT_YEAR_END;
    END IF; 

    dbmtk_function_result = 1;
    RETURN;

    /* ORIGSQL: exception WHEN NOT_YEAR_END then */
END;

/* ORIGSQL: end PK_PIAOR_CALCULATION; */
END;/* end of library */