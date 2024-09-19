CREATE procedure Comtransferpiaor_debug(in I_R_Agydisttrxn_row  R_Agydisttrxn)
as
begin

	DECLARE cdt_EndOfTime date = to_date('2200-01-01','yyyy-mm-dd');
    DECLARE Gv_Periodseq BIGINT; /* package/session variable */
    DECLARE Gv_Setnumberpi BIGINT; /* package/session variable */
    DECLARE gv_setnumberaor BIGINT; /* package/session variable */

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
    DECLARE v_CurDistrict VARCHAR(30);  /* ORIGSQL: v_CurDistrict Varchar2(30); */    --add by nelson
    DECLARE v_LdrCurRole VARCHAR(30);  /* ORIGSQL: v_LdrCurRole Varchar2(30); */    --add by nelson
    DECLARE v_wAgyLdrCde VARCHAR(30);  /* ORIGSQL: v_wAgyLdrCde Varchar2(30); */    --add by nelson
    DECLARE v_setup VARCHAR(30);  /* ORIGSQL: v_setup Varchar2(30); */    --add by nelson
    DECLARE v_wAgyLdrCurClass VARCHAR(30);  /* ORIGSQL: v_wAgyLdrCurClass varchar2(30); */
    DECLARE V_Crossoverflag BIGINT = 0;  /* ORIGSQL: V_Crossoverflag Int:=0; */
    DECLARE v_ConstantCrossoverDate date = to_date('1/1/2005','mm/dd/yyyy');  /* ORIGSQL: v_ConstantCrossoverDate date:=to_date('1/1/2005','mm/dd/yyyy') ; */
    DECLARE V_Manageragy VARCHAR(30);  /* ORIGSQL: V_Manageragy Varchar2(30); */
    DECLARE v_RunningType VARCHAR(255);  /* ORIGSQL: v_RunningType varchar2(255); */
    DECLARE v_OrphanPolicy VARCHAR(30);  /* ORIGSQL: v_OrphanPolicy varchar2(30); */
    DECLARE INVALID_MANAGER condition;  /* ORIGSQL: Invalid_Manager exception; */
    DECLARE I_R_Agydisttrxn  row like ext.R_Agydisttrxn;

    DECLARE EXIT HANDLER FOR INVALID_MANAGER
        BEGIN
            /* ORIGSQL: Comdebugger('ComTransferPIAOR','Stagehook is not able get any spin off manager'||I_R_Agydisttrxn.salestransactionSeq) */
            comDebugger('ComTransferPIAOR', 'Stagehook is not able get any spin off manager'||IFNULL(TO_VARCHAR(:I_R_Agydisttrxn.Salestransactionseq),''));
        END;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            /* ORIGSQL: comDebugger('ComTransferPIAOR','Error'||sqlerrm) */
            comDebugger('ComTransferPIAOR', 'Error'||::SQL_ERROR_MESSAGE );  /* ORIGSQL: sqlerrm */
        END;

        /* retrieve the package/session variables referenced in this procedure */
        SELECT CAST(SESSION_CONTEXT('GV_PERIODSEQ') AS BIGINT) INTO Gv_Periodseq from sys.dummy;
        SELECT CAST(SESSION_CONTEXT('GV_SETNUMBERPI') AS BIGINT) INTO Gv_Setnumberpi from sys.dummy;
        SELECT CAST(SESSION_CONTEXT('GV_SETNUMBERAOR') AS BIGINT) INTO gv_setnumberaor from sys.dummy;
        /* end of package/session variables */

	select top 1 * into I_R_Agydisttrxn from :I_R_Agydisttrxn_row;
       
       
  /*
  begin
      select nvl(max(setNumber),0)
      into v_maxSetNumber
      from cs_transactionAssignment
      where I_R_Agydisttrxn.salestransactionSeq=salestransactionSeq
      And Genericattribute4 Is Null;

    exception when no_data_found then
      v_maxSetNumber:=0;
    end;

    */



    --DBMS_OUTPUT.put_line(c_txn.salestransactionSeq||'   start c_txn  issue date  '||to_char(v_policyIssueDate,'mm/dd/yyyy'));
    --DBMS_OUTPUT.put_line(v_policyIssueDate||' ---- '||v_cutoverdate);

      v_wAgency:=:I_R_Agydisttrxn.wAgency;
      v_wAgencyLeader:=:I_R_Agydisttrxn.wAgencyLeader;
      v_wAgyLdrTitle:=:I_R_Agydisttrxn.wAgyLdrTitle;
      v_wAgyLdrDistrict:=:I_R_Agydisttrxn.wAgyLdrDistrict;
      v_Wagtclass:=:I_R_Agydisttrxn.Wagtclass;
      V_Policyissuedate:=:I_R_Agydisttrxn.Policyissuedate;
      V_Commissionagy:=:I_R_Agydisttrxn.Commissionagy;
      V_Runningtype:=:I_R_Agydisttrxn.Runningtype;
      v_OrphanPolicy:=:I_R_Agydisttrxn.Orphanpolicy;
      v_CurDistrict:=:I_R_Agydisttrxn.CurDistrict; --add by nelson
      v_LdrCurRole:=:I_R_Agydisttrxn.LdrCurRole; --add by nelson
      v_wAgyLdrCde:=:I_R_Agydisttrxn.wAgyLdrCde; --add by nelson
      v_setup:=:I_R_Agydisttrxn.setup; --add by nelson


      --comDebugger('PIAOR DEBUGGER','WAGENCY['||v_wAgency||']||');

 Log('Comtransferpiaor wAgency '|| :I_R_Agydisttrxn.wAgency);
 Log('Comtransferpiaor Eventtypeid '|| :I_R_Agydisttrxn.Eventtypeid);
 Log('Comtransferpiaor OrphanPolicy '|| :I_R_Agydisttrxn.OrphanPolicy);
 Log('Comtransferpiaor productname '|| :I_R_Agydisttrxn.productname);
 Log('Comtransferpiaor txnCode '|| :I_R_Agydisttrxn.txnCode);

 Log('Comtransferpiaor CurDistrict '|| :I_R_Agydisttrxn.CurDistrict);
 Log('Comtransferpiaor LdrCurRole '|| :I_R_Agydisttrxn.LdrCurRole);
 Log('Comtransferpiaor wAgyLdrCde '|| :I_R_Agydisttrxn.wAgyLdrCde);

 Log('Comtransferpiaor wAgencyLeader '|| :I_R_Agydisttrxn.wAgencyLeader);
 Log('Comtransferpiaor wAgyLdrTitle '|| :I_R_Agydisttrxn.wAgyLdrTitle);
 Log('Comtransferpiaor wAgyLdrDistrict '|| :I_R_Agydisttrxn.wAgyLdrDistrict);

 Log('Comtransferpiaor Spinstartdate '|| :I_R_Agydisttrxn.Spinstartdate);
 Log('Comtransferpiaor SpinEnddate '|| :I_R_Agydisttrxn.SpinEnddate);
 Log('Comtransferpiaor Txnclasscode '|| :I_R_Agydisttrxn.Txnclasscode);


 Log('Comtransferpiaor Salestransactionseq '|| :I_R_Agydisttrxn.Salestransactionseq);
 Log('Comtransferpiaor Wagency '|| :I_R_Agydisttrxn.Wagency);
 Log('Comtransferpiaor Commissionagy '|| :I_R_Agydisttrxn.Commissionagy);

 if :I_R_Agydisttrxn.wAgency is not null then

    --comDebugger('PIAOR DEBUGGER','vNewWritingAgy: '||vNewWritingAgy ||' -- '||v_wAgencyLeader);
Log('C1');
      -- pi
      If (:I_R_Agydisttrxn.Eventtypeid = 'RYC' Or (:I_R_Agydisttrxn.Eventtypeid = 'ORYC'
      AND substr(:I_R_Agydisttrxn.OrphanPolicy,1,1) ='X'))
      and :I_R_Agydisttrxn.productname in ('LF','HS')
      and :I_R_Agydisttrxn.txnCode in ('PAY2','PAY3','PAY4','PAY5','PAY6') then
        if v_wAgyLdrTitle = 'FSD' then

           if v_wAgyLdrDistrict=v_commissionAgy then
                v_rule:='PI - Direct Team';
                v_writingAgyLdr:=v_wAgyLdrDistrict;
                Log('C2');
          else
            v_rule:='PI - Indirect Team';
            v_writingAgyLdr:=v_wAgyLdrDistrict;
            Log('C3');
          end if;

        ELSEIF  V_Wagyldrtitle = 'FSAD' Then

           -- If :I_R_Agydisttrxn.Agyspinoffindicator='Y' And :I_R_Agydisttrxn.Agyspinoffflag=1 Then
           --above checking was disabled
            if :I_R_Agydisttrxn.Spinstartdate is not null then
            --spin off case
              V_Writingagyldr:=V_Wagencyleader;
              V_Rule:='PI - Direct Team';
              v_RunningType:=v_RunningType||'_SpinOff';
              Log('C4');
            Else
            --non spin off case
                if :I_R_Agydisttrxn.Wagtclass <>'12' then -- FSAD no need compare ga13 with district, as long as class=10, writingAgy always get PI

                  select p.genericattribute4
                  into v_wAgyLdrCurClass
                  from cs_position p,cs_period t
                  where p.name = v_wAgyLdrCde
                  and   p.removeDate=Cdt_Endoftime
                  and   t.periodseq = Gv_Periodseq
                  and   p.effectiveStartDate<= add_days(t.enddate, -1)
                  and   p.effectiveEndDate > add_days(t.enddate,-1) ;

Log('C5');
                  if v_wAgyLdrCurClass <> '12' Then
                    v_rule:='PI - Direct Team';
                    v_writingAgyLdr:=v_wAgencyLeader  ;
                    Log('C6');
                  Else
                    v_rule:='PI - Indirect Team';
                    v_writingAgyLdr:=v_wAgyLdrDistrict;
                    Log('C7');
                  end if;
                Else
                  v_rule:='PI - Indirect Team';
                  v_writingAgyLdr:=v_wAgyLdrDistrict;
                  Log('C8');
                End If;
            end if;--spin off chking

        ELSEIF v_wAgyLdrTitle in ('FSM','AM') then
          --
          Log('C9');
          If :I_R_Agydisttrxn.Agyspinoffindicator='N' And :I_R_Agydisttrxn.Agyspinoffflag=1 Then
            --spin off case
              begin
	              declare exit handler for sqlexception 
	              begin 
                   signal Invalid_Manager;
                  end;

	              Select Name
                Into v_managerAgy
                From Cs_Position
               Where Ruleelementownerseq=:I_R_Agydisttrxn.Managerseq
                 And Removedate=Cdt_Endoftime
                 And Effectivestartdate<=:I_R_Agydisttrxn.Versioningdate
                 and effectiveEndDate>:I_R_Agydisttrxn.versioningdate;
              
              End;
Log('C10');
              V_Rule:='PI - Indirect Team';
              V_Writingagyldr:=V_Manageragy;
              v_RunningType:=v_RunningType||'_SpinOff';

            Else
              --non spin off
            if v_wAgyLdrDistrict=v_commissionAgy then
              V_Rule:='PI - Indirect Team';
              V_Writingagyldr:=V_Wagyldrdistrict;
              Log('C11');
            else
              V_Rule:='PI - Indirect Team';
              V_Writingagyldr:=V_Wagyldrdistrict;
              Log('C12');
            end if;


           End If; --spin off chking for um
       end if;
Log('C13');
       --check crossover set up
       if v_setup <> 'X' and v_wAgyLdrTitle in ('FSD','FSAD','FSM','AM') then
            vNewWritingAgy:=comGetCrossoverAgy('PI',v_wAgyLdrCde,v_policyIssueDate); --add by nelson
Log('C14');
            IF vNewWritingAgy IS NOT NULL THEN -- add by nelson
               V_Writingagyldr:='SGY'||Vnewwritingagy;  -- add by nelson
               V_Rule:='PI - Direct Team'; -- add by nelson
               Log('C15');
            END IF; -- add by nelson
       end if;
      end if; --eventtype check FOR PI

        --comDebugger('PIAOR DEBUGGER','v_rule1: '||v_rule);
Log('C16');
          --aor
      if :I_R_Agydisttrxn.eventtypeid  in ('API','IFYC','FYC', 'SSCP')
        or (:I_R_Agydisttrxn.eventtypeid = 'RYC' and :I_R_Agydisttrxn.productname in ('LF','HS','PA','CS'))
        then
Log('C17');
          --add by nelson start
          if v_wAgyLdrTitle = 'FSD' and v_wAgyLdrDistrict=v_commissionAgy and v_wAgyLdrDistrict=v_CurDistrict and v_LdrCurRole ='FSD' then
                      v_AorRule:='AOR - Direct Team';
                      v_AorWAgyLdr:=v_CurDistrict;
                      Log('C18');
          elseif v_wAgyLdrTitle = 'FSAD' and v_wAgyLdrDistrict<>v_CurDistrict and v_LdrCurRole ='FSD'  then
                      v_AorRule:='AOR - Indirect Team';
                      v_AorWAgyLdr:=v_wAgyLdrDistrict;
                      Log('C19');
          elseif v_wAgyLdrTitle = 'FSAD' and (:I_R_Agydisttrxn.Spinstartdate Is Not Null  or :I_R_Agydisttrxn.SpinEndDate is not null) then
                      --spin off case
                      V_Aorrule:='AOR - Direct Team';
                      V_Aorwagyldr:=V_Wagyldrdistrict;
                      Log('C20');
                      If :I_R_Agydisttrxn.Spindaterange>8 Or ( :I_R_Agydisttrxn.Actualorphanpolicy<>'O'
                      AND :I_R_Agydisttrxn.compensationDate>:I_R_Agydisttrxn.spinEndDate )  Then
                          v_OrphanPolicy:='XO'; --- set the flag as 'XO', the transaction will not get PI or AOR, but stamp 1 to EB4 of trxn
                      Log('C21');
                      end if;
          else
                      v_AorRule:='AOR - Indirect Team';
                      v_AorWAgyLdr:=v_CurDistrict;
                      Log('C22');
          end if;
          --add by nelson end

          if v_setup <> 'X' then
               vAorNewWritingAgy:=comGetCrossoverAgy('AOR',v_wAgyLdrCde,v_policyIssueDate);
Log('C23');
                IF vAorNewWritingAgy IS NOT NULL THEN
                 --Vnewwritingagy:='SGY'||Vnewwritingagy; commented by nelson
                 V_Aorwagyldr:='SGY'||vAorNewWritingAgy;  -- add by nelson
                 V_Aorrule:='AOR - Direct Team';
                 Log('C24');
                END IF;
          end if;

        END IF; --EVENTTYPE CHECK FOR AOR


     --comDebugger('PIAOR DEBUGGER','v_rule2'||v_rule);
Log('C25 '|| V_Rule);

     If V_Rule Is Not Null And :I_R_Agydisttrxn.Eventtypeid IN   ('RYC','ORYC') Then
     -- v_maxSetNumber:=v_maxSetNumber+1;
     Log('C26');
      insert  into sh_query_result (component,periodseq,
          genericSequence1, --txnseq
          genericSequence2,  --orderSeq
          genericAttribute1, --wAgyLdr
          genericAttribute2,  --wAgyLdrTitle
          genericAttribute3,   --wAgy
          genericAttribute4,   --wAgyDistrict
          genericAttribute5,  --rule
          genericDate1,    --policyIssueDate
          genericDate2,     --compensationDate
          genericNumber1,  --setNumber
          genericAttribute6,  --BUMap
          GENERICATTRIBUTE7,
          Genericattribute8,
          Genericattribute9,
          Genericattribute10, --ga10 is the rule before redirect
          Genericattribute11,
          genericAttribute12
          ) Values (
          'PI',gv_periodseq,
          :I_R_Agydisttrxn.salestransactionSeq,
          :I_R_Agydisttrxn.salesOrderSeq,
          v_writingAgyLdr,
          v_wAgyLdrTitle,
          v_wAgency,
          v_wAgyLdrDistrict,
          v_rule,
          v_policyIssueDate,
          :I_R_Agydisttrxn.Compensationdate,
          gV_Setnumberpi,
          :I_R_Agydisttrxn.businessUnitMap,
          :I_R_Agydisttrxn.eventtypeid,
          :I_R_Agydisttrxn.Productname,
          'PI '||v_RunningType,
          V_Rule,
          :I_R_Agydisttrxn.Orphanpolicy,  --REAL GA17
          :I_R_Agydisttrxn.TxnclassCode
      );
     end if;


      if v_AorRule is not null
      And :I_R_Agydisttrxn.Eventtypeid  In ('RYC','API','IFYC','FYC', 'SSCP') Then
      --v_maxSetNumber:=v_maxSetNumber+1;
      Log('C27');
      insert into sh_query_result (component,periodseq,
          genericSequence1, --txnseq
          genericSequence2,  --orderSeq
          genericAttribute1, --wAgyLdr
          genericAttribute2,  --wAgyLdrTitle
          genericAttribute3,   --wAgy
          genericAttribute4,   --wAgyDistrict
          genericAttribute5,  --rule
          genericDate1,    --policyIssueDate
          genericDate2,     --compensationDate
          genericNumber1,  --setNumber
          genericAttribute6,  --BUMap
          GENERICATTRIBUTE7, --eventtype
          GENERICATTRIBUTE8, --productname
          Genericattribute9, --running type
          Genericattribute10, --rule before redicrect, because, after the SP, stagehook will update GA5
          Genericattribute11,  --REAL GA17
          Genericdate3, --spin off start date
          Genericnumber2, --spin off range
          genericAttribute12 --txn classcode
          ) Values (
          'AOR',gv_periodSeq,
          :I_R_Agydisttrxn.salestransactionSeq,
          :I_R_Agydisttrxn.salesOrderSeq,
          v_AorWAgyLdr,
          v_wAgyLdrTitle,
          v_wAgency,
          v_wAgyLdrDistrict,
          v_AorRule,
          v_policyIssueDate,
          :I_R_Agydisttrxn.Compensationdate,
          gV_Setnumberaor,
          :I_R_Agydisttrxn.businessUnitMap,
          :I_R_Agydisttrxn.eventtypeid,
          :I_R_Agydisttrxn.productname,
          'AOR '||v_RunningType,
          V_Aorrule,
          v_OrphanPolicy,
          :I_R_Agydisttrxn.Spinstartdate,
          :I_R_Agydisttrxn.Spindaterange,
          :I_R_Agydisttrxn.TxnClassCode

      );

      end if;

      end if; -- wAgency is not null

      V_Rule:=Null;
      v_AorRule:=null;
      Log('C29');

end --Comtransferpiaor_debug;