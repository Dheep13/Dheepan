CREATE PROCEDURE SP_UPDATE_GST(in I_PERIODSEQ bigINT) 
AS
   begin
	   
    DECLARE gv_error VARCHAR(1000); /* package/session variable */
    DECLARE cdt_EndOfTime date= EndOfTime();
    DECLARE Gv_Processingunitseq BIGINT; /* package/session variable */

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE V_PERIODSEQ BIGINT = :I_PERIODSEQ;  /* ORIGSQL: V_PERIODSEQ INT := I_PERIODSEQ; */
    DECLARE V_PERIODSTARTDATE TIMESTAMP;  /* ORIGSQL: V_PERIODSTARTDATE DATE; */
    DECLARE V_PERIODENDDATE TIMESTAMP;  /* ORIGSQL: V_PERIODENDDATE DATE; */
    DECLARE V_CALENDARSEQ BIGINT;  /* ORIGSQL: V_CALENDARSEQ INT; */
    DECLARE V_PERIODTYPESEQ BIGINT;  /* ORIGSQL: V_PERIODTYPESEQ INT; */
    DECLARE V_COMPONENTVALUE VARCHAR(30) = 'GST';  /* ORIGSQL: V_COMPONENTVALUE VARCHAR2(30) := 'GST'; */
    DECLARE V_NORMALRATE BIGINT;  /* ORIGSQL: V_NORMALRATE INT; */
    DECLARE V_DEFAULTRATE BIGINT;  /* ORIGSQL: V_DEFAULTRATE INT; */
    DECLARE V_STRDATEFORMAT VARCHAR(30) = 'YYYY-MM-DD';  /* ORIGSQL: V_STRDATEFORMAT VARCHAR2(30) := 'YYYY-MM-DD'; */
    DECLARE V_GST_RATE DECIMAL(10,2);  /* ORIGSQL: V_GST_RATE NUMBER(10,2); */
    DECLARE V_ET1 DECIMAL(38,10);  /* ORIGSQL: V_ET1 NUMBER; */
    DECLARE V_ET2 DECIMAL(38,10);  /* ORIGSQL: V_ET2 NUMBER; */
    DECLARE V_ET3 DECIMAL(38,10);  /* ORIGSQL: V_ET3 NUMBER; */
    DECLARE V_ET4 DECIMAL(38,10);  /* ORIGSQL: V_ET4 NUMBER; */
    DECLARE V_ET5 DECIMAL(38,10);  /* ORIGSQL: V_ET5 NUMBER; */
    DECLARE V_ET6 DECIMAL(38,10);  /* ORIGSQL: V_ET6 NUMBER; */
    DECLARE V_ET7 DECIMAL(38,10);  /* ORIGSQL: V_ET7 NUMBER; */
    DECLARE V_ET8 DECIMAL(38,10);  /* ORIGSQL: V_ET8 NUMBER; */
    DECLARE V_ET9 DECIMAL(38,10);  /* ORIGSQL: V_ET9 NUMBER; */
    DECLARE V_ET10 DECIMAL(38,10);  /* ORIGSQL: V_ET10 NUMBER; */
    DECLARE V_ET11 DECIMAL(38,10);  /* ORIGSQL: V_ET11 NUMBER; */
    DECLARE V_ET12 DECIMAL(38,10);  /* ORIGSQL: V_ET12 NUMBER; */
    DECLARE V_ET13 DECIMAL(38,10);  /* ORIGSQL: V_ET13 NUMBER; */
    DECLARE V_UNITTYPENAME VARCHAR(30) = 'percent';  /* ORIGSQL: V_UNITTYPENAME VARCHAR2(30) := 'percent'; */
    DECLARE V_UNITTYPESEQ DECIMAL(38,10);  /* ORIGSQL: V_UNITTYPESEQ NUMBER; */
    DECLARE V_PU VARCHAR(30);  /* ORIGSQL: V_PU VARCHAR2(30); */
    declare v_tenantid varchar(20) = gettenantid();

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            gv_error = 'Error [SP_UPDATE_GST]: ' ||::SQL_ERROR_MESSAGE ;--|| ' - ' ||  /* ORIGSQL: sqlerrm */
            /*dbms_utility.format_error_backtrace;*/
            SET SESSION 'GV_ERROR' = :gv_error;
            Log(:gv_error);
            ROLLBACK;
        END;

        /* retrieve the package/session variables referenced in this procedure */
        SELECT SESSION_CONTEXT('GV_ERROR') INTO gv_error FROM SYS.DUMMY ;
        SELECT CAST(SESSION_CONTEXT('GV_PROCESSINGUNITSEQ') AS BIGINT) INTO Gv_Processingunitseq FROM SYS.DUMMY ;
        /* end of package/session variables */ 
	   
/*   comInitialPartition('GST', v_componentValue, i_periodSeq);*/

   --Gv_Processingunitseq := 38280596832649218;

   Log('[GST] Periodseq is: ' || i_periodSeq);

   select  startDate, endDate, periodTypeSeq, calendarSeq into v_periodStartDate, v_periodEndDate, v_periodTypeSeq, v_calendarSeq from cs_period where tenantid = v_tenantid and periodseq = v_periodseq and removeDate = cdt_EndOfTime;

   Log('[GST] v_periodStartDate is: ' || v_periodStartDate || ' , v_periodEndDate is: ' || v_periodEndDate || ' , v_periodTypeSeq is: ' || v_periodTypeSeq || ' , v_calendarSeq is: ' || v_calendarSeq );

   SELECT NAME INTO V_PU FROM CS_PROCESSINGUNIT WHERE PROCESSINGUNITSEQ = Gv_Processingunitseq;

    Log('[GST] V_PU is: ' || V_PU);

   SELECT UNITTYPESEQ INTO V_UNITTYPESEQ FROM CS_UNITTYPE WHERE NAME = V_UNITTYPENAME AND REMOVEDATE = cdt_EndOfTime;    

   v_ET1 := Comgeteventtypeseq('APF');
   v_ET2 := Comgeteventtypeseq('APF Payable');
   v_ET3 := Comgeteventtypeseq('API');
   v_ET4 := Comgeteventtypeseq('FYC');
   v_ET5 := Comgeteventtypeseq('SSCP');
   v_ET6 := Comgeteventtypeseq('RYC');
   v_ET7 := Comgeteventtypeseq('FYC_TP');
   v_ET8 := Comgeteventtypeseq('RYC_TP');
   v_ET9 := Comgeteventtypeseq('TFR');
   v_ET10 := Comgeteventtypeseq('TFS');
   v_ET11 := Comgeteventtypeseq('FYC_INTRODUCER');
   -- version 22 For MAS Section86 project
   v_ET12 := Comgeteventtypeseq('FYC_TPGI');
   v_ET13 := Comgeteventtypeseq('RYC_TPGI');

   execute immediate 'truncate table SH_GST_TMP_DATAPARTICIPANT';
   Log('[GST] truncate table SH_GST_TMP_DATAPARTICIPANT');

   insert into SH_GST_TMP_DATAPARTICIPANT
     select SUBSTR(pa.USERID, 4), pa.payeeseq
       from cs_participant pa
      where pa.tenantid = v_tenantid
        and pa.removeDate = cdt_EndOfTime
        and v_periodEndDate >= pa.effectiveStartDate  
        and v_periodEndDate < pa.effectiveEndDate
    and GENERICBOOLEAN3 = 1;
   Log('[GST] insert into SH_GST_TMP_DATAPARTICIPANT '||::ROWCOUNT);
   commit;


    delete from Sh_Query_Result where Component = v_componentValue and periodseq = v_periodseq and genericSequence2 = Gv_Processingunitseq;
  	commit;

   IF V_PU = 'AGY_PU'
   THEN
   Insert Into Sh_Query_Result (Component, periodseq, value, 
      genericsequence1, -- salestransactionseq
      genericsequence2, -- processingunitseq
      genericattribute1, -- commission agent code
      genericattribute2, -- policy no
      genericattribute3, -- component code
      genericdate1, -- coverge issue date
      genericdate2, -- compensation date
      genericboolean1 -- api eligible indicator
      )
     SELECT distinct v_componentValue,
                     v_periodseq,
                     txn.value,
                     txn.salestransactionseq,
           			 txn.processingunitseq ,
                     txn.genericattribute12,
                     txn.ponumber,
                     txn.PRODUCTID,
                     txn.genericdate2,
           txn.COMPENSATIONDATE,
           txn.GENERICBOOLEAN4
       FROM Cs_Salestransaction Txn
      INNER JOIN SH_GST_TMP_DATAPARTICIPANT tpa
         ON txn.genericattribute12 = tpa.AGENTCODE
      WHERE txn.tenantid = v_tenantid
        AND txn.processingunitseq = Gv_Processingunitseq
        AND txn.compensationDate >= v_periodStartDate
        AND txn.compensationDate < v_periodEndDate
        AND txn.eventtypeseq IN (v_ET1,v_ET2,v_ET3,v_ET4,v_ET5,v_ET6,v_ET7,v_ET8,v_ET9,v_ET10,v_ET11,v_ET12,v_ET13); -- version 22 For MAS Section86 project  


   Log('[GST] insert into Sh_Query_Result '||::ROWCOUNT);
   commit;

   merge into Sh_Query_Result qr
   using (select cg.salestransactionseq,
                 cg.GENERICNUMBER3
      from cs_gaSalestransaction cg
      INNER JOIN Sh_Query_Result t1 ON t1.genericSequence1 = cg.salestransactionseq
      WHERE cg.tenantid = v_tenantid
      AND cg.processingunitseq = Gv_Processingunitseq
      AND cg.compensationDate >= v_periodStartDate
      AND cg.compensationDate < v_periodEndDate
      AND cg.PAGENUMBER = 0
      AND t1.Component = v_componentValue
      AND t1.GENERICBOOLEAN1 = 1
      AND t1.periodseq = v_periodseq
      ) r
   on (qr.genericSequence1 = r.salestransactionseq and qr.GENERICBOOLEAN1 = 1 AND qr.Component = v_componentValue AND qr.periodseq = v_periodseq)
   when matched then
     update set qr.value = r.GENERICNUMBER3;

   Log('[GST] update GENERICNUMBER3 for Sh_Query_Result '||::ROWCOUNT);

   ELSEIF  V_PU = 'PD_PU' THEN

   Insert Into Sh_Query_Result (Component, periodseq, value, 
      genericSequence1, -- SalesTransactionSeq
      genericSequence2, -- processingunitseq
      GENERICATTRIBUTE1, -- Commission agent code
      GENERICATTRIBUTE2, -- Policy no
      GENERICATTRIBUTE3, -- Component code
      GENERICDATE1, -- Coverge issue date
      GENERICDATE2, -- compensation date
      GENERICBOOLEAN1 -- API Eligible Indicator
      )
     SELECT distinct v_componentValue,
                     v_periodseq,
                     txn.value,
                     txn.salestransactionseq,
                     txn.processingunitseq ,
                     txn.genericattribute13,
                     txn.ponumber,
                     txn.PRODUCTID,
                     txn.genericdate2,
         txn.COMPENSATIONDATE,
         txn.GENERICBOOLEAN4
       FROM Cs_Salestransaction Txn
      INNER JOIN SH_GST_TMP_DATAPARTICIPANT tpa
         ON txn.genericattribute13 = tpa.AGENTCODE
      WHERE txn.tenantid = v_tenantid
        AND txn.processingunitseq = Gv_Processingunitseq
        AND txn.compensationDate >= v_periodStartDate
        AND txn.compensationDate < v_periodEndDate
    AND txn.eventtypeseq IN (v_ET1,v_ET2,v_ET3,v_ET4,v_ET5,v_ET6,v_ET7,v_ET8,v_ET9,v_ET10,v_ET11);  


      Log('[GST] insert into Sh_Query_Result '||::ROWCOUNT);

   commit;
   ELSE
      Log('[GST] PU is not correct');
    return;
   END IF;

   select value into V_GST_RATE from vw_lt_gst_rate where v_periodStartDate >= effectivestartdate and v_periodStartDate < effectiveenddate;

   execute immediate 'truncate table GST_RATE_TMP'; 
   INSERT INTO GST_RATE_TMP
     (component,
      periodSeq,
      GENERICATTRIBUTE1,
      GENERICATTRIBUTE2,
      GENERICATTRIBUTE3,
    GENERICDATE1,
      gst_rate)
     SELECT distinct component,
            periodSeq,
            GENERICATTRIBUTE1, -- Commission agent code
            GENERICATTRIBUTE2, -- Policy no
            GENERICATTRIBUTE3, -- Component code
      MAX(GENERICDATE1), -- Coverge issue date
            case
              when sum(value) >= 0 then
               V_GST_RATE
              else
               (select value
                  from vw_lt_gst_rate
                 where GENERICDATE1 >=
                       effectivestartdate
                   and GENERICDATE1 <
                       effectiveenddate)
            end AS gst_rate
       from sh_query_result
      where component = v_componentValue
        and periodSeq = v_periodseq
    and genericSequence2 = Gv_Processingunitseq
      group by component,
             periodSeq,
               GENERICATTRIBUTE1,
               GENERICATTRIBUTE2,
               GENERICATTRIBUTE3,
         GENERICDATE1;
   Log('[GST] insert into GST_RATE_TMP '||::ROWCOUNT);
   commit;

   merge into Sh_Query_Result qr
   using (select component,
                 periodSeq,
                 GENERICATTRIBUTE1,
                 GENERICATTRIBUTE2,
                 GENERICATTRIBUTE3,
         GENERICDATE1,
                 gst_rate
            from GST_RATE_TMP) r
   on (qr.component = r.component and qr.periodSeq = r.periodSeq and qr.GENERICATTRIBUTE1 = r.GENERICATTRIBUTE1 and qr.GENERICATTRIBUTE2 = r.GENERICATTRIBUTE2 and qr.GENERICATTRIBUTE3 = r.GENERICATTRIBUTE3 and qr.GENERICDATE1 = r.GENERICDATE1)
   when matched then
     update set qr.GENERICNUMBER1 = r.gst_rate;

   Log('[GST] update GST rate for Sh_Query_Result '||::ROWCOUNT);
   commit;

       update cs_gaSalestransaction ga
       set ga.GENERICNUMBER9 =
           (select max(r.GENERICNUMBER1)
              from sh_query_result r
             where r.Component = v_componentValue
               and r.periodSeq = v_periodseq
               and ga.salestransactionseq = r.genericsequence1
         and ga.PROCESSINGUNITSEQ = Gv_Processingunitseq
               /*and rownum = 1*/),
      ga.UNITTYPEFORGENERICNUMBER9 = V_UNITTYPESEQ
            where ga.PAGENUMBER = 0 and ga.tenantid = v_tenantid and exists (select 1
              from sh_query_result sh
             where sh.genericSequence1 = ga.salestransactionSeq
               and sh.Component = v_componentValue
               and sh.periodSeq = v_periodseq
         and ga.PAGENUMBER = 0
         and ga.tenantid = v_tenantid 
         and ga.PROCESSINGUNITSEQ = Gv_Processingunitseq
         and ga.COMPENSATIONDATE = sh.GENERICDATE2);
   Log('[GST] update GST rate for cs_gaSalestransaction '||::ROWCOUNT);
   commit;
 END --SP_UPDATE_GST;