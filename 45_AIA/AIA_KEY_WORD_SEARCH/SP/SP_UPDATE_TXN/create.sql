CREATE  procedure Sp_Update_Txn(in In_Periodseq  bigInt) 
sql security definer 
as
begin 
using sqlscript_print as dbms_output;    
declare dec_txnadjseq    like  CS_TransactionAdjustment.transactionadjustmentseq;
declare dec_txnseq        like CS_SalesTransaction.salestransactionseq;
declare dec_messagelogseq like CS_Message.messageLogSeq;
declare dec_messageseq    like CS_Message.messageSeq;
declare v_transferDate date;
declare v_componentValue varchar2(30) := 'TXNUPD';
declare vstartdate date;
declare venddate date;
declare vparname varchar2(255);
declare v_ET1 decimal;
declare v_ET2 decimal;
declare v_ET3 decimal;
declare v_ET4 decimal;
declare v_ET5 decimal;
declare v_ET6 decimal;
declare v_ET7 decimal;
declare v_ET8 decimal;
declare v_ET9 decimal;
declare vSQL varchar2(4000);
declare vDecodeSQL varchar2(1000);
declare cdt_EndOfTime date := to_date('2200-01-01','yyyy-mmm-dd');
declare Gv_Processingunitseq bigint;



/* Mark -20150820 - start */

/*
TYPE txnseq_t IS TABLE OF cs_salestransaction.salestransactionseq%TYPE;
TYPE transferdt_t IS TABLE OF cs_gasalestransaction.genericdate2%TYPE;
TYPE contractdt_t IS TABLE OF cs_gasalestransaction.genericdate1%TYPE;
TYPE welcomepk_t IS TABLE OF cs_gasalestransaction.genericboolean1%TYPE;
TYPE assigndt_t IS TABLE OF cs_gasalestransaction.genericdate3%TYPE;
TYPE ldrsocdt_t IS TABLE OF cs_gasalestransaction.genericdate5%TYPE;
TYPE starttxndt_t IS TABLE OF cs_gasalestransaction.genericdate7%TYPE;
*/

declare l_txnseq txnseq_t;
declare l_transferdt transferdt_t;
declare l_contractdt contractdt_t;
declare l_welcomepk welcomepk_t;
declare l_assigndt assigndt_t;
declare l_ldrsocdt ldrsocdt_t;
declare l_starttxndt starttxndt_t;

declare exit handler for sqlexception
			begin
			rollback;
			resignal;
			end;	


/* Mark -20150820 - end */
  
    /*TBD comInitialPartition(v_componentValue, v_componentValue, in_periodSeq);*/
    Log('Start 1');
   
   SELECT CAST(SESSION_CONTEXT('GV_PROCESSINGUNITSEQ') AS BIGINT) INTO Gv_Processingunitseq FROM SYS.DUMMY ;
   

    --Mark 20150820
    --Maintenance.Enablepdml;

    select Startdate, enddate
      into vstartdate, venddate
      from cs_period per
     where per.periodSeq = in_periodSeq
       AND per.removedate = cdt_EndOfTime;

    Log('Looking for partition  ' || gv_ProcessingUnitSeq || ' date ' || to_varchar(venddate, 'YYYYMMDD') || ' periodseq ' || in_periodSeq);
 
   vParName := 'P_AIAS_00002_20161201';

/*TBD 
    begin
      SELECT
         (SELECT subobject_name
            FROM all_objects
           WHERE data_object_id = dbms_rowid.rowid_object(CS_SALESTRANSACTION.ROWID)) into vParName
        FROM CS_SALESTRANSACTION
       where tenantid='AIAS'
         and processingUnitseq=gv_ProcessingUnitSeq
         and compensationdate >= vstartdate
         and compensationdate < venddate
         and rownum = 1;
    exception
    when others
    then
       RAISE_APPLICATION_ERROR(-20000,'No TX partition found, goodbye');
    end;
*/

    Log('Found partition name  ' || ifnull(vParName, 'NULL'));


    v_ET1 := Comgeteventtypeseq('APF');
    v_ET2 := Comgeteventtypeseq('APF Payable');
    v_ET3 := Comgeteventtypeseq('API');
    v_ET4 := Comgeteventtypeseq('FYC');
    v_ET5 := Comgeteventtypeseq('OFYC');
    v_ET6 := Comgeteventtypeseq('ORYC');
    v_ET7 := Comgeteventtypeseq('OSSCP');
    v_ET8 := Comgeteventtypeseq('SSCP');
    v_ET9 := Comgeteventtypeseq('RYC');

    --Maintenance.Enablepdml;
    vDecodeSQL := 'case when TXn.EVENTTYPESEQ = ' ||v_ET1 || ' then ''APF''  
                   		when TXn.EVENTTYPESEQ = ' ||v_ET2 || ' then ''APF Payable'' 
                   		when TXn.EVENTTYPESEQ = ' ||v_ET3 || ' then ''API''
                  		when TXn.EVENTTYPESEQ = ' ||v_ET4 || ' then ''FYC'' 
                  		when TXn.EVENTTYPESEQ = ' ||v_ET5 || ' then ''OFYC''
                  		when TXn.EVENTTYPESEQ = ' ||v_ET6 || ' then ''ORYC'' 
                  		when TXn.EVENTTYPESEQ = ' ||v_ET7 || ' then ''OSSCP''
                  		when TXn.EVENTTYPESEQ = ' ||v_ET8 || ' then ''SSCP'' 
                  		when TXn.EVENTTYPESEQ = ' ||v_ET9 || ' then ''RYC'' else null end ';

    Log('Found decode string  ' || vDecodeSQL);

    vSQL := 'Insert Into Sh_Query_Result ';
    vSQL := vSQL ||'(Component,periodseq,Genericsequence1, Genericnumber1, Genericnumber2, Genericattribute1, genericattribute2, Genericattribute3, Genericattribute4, Genericattribute5,   genericboolean1,  Genericboolean2, Genericdate1,  genericdate2,  Genericdate3,  genericsequence2,  Genericattribute6,  Genericattribute7,  Genericattribute8,  Genericdate4,  genericDate5  ) ';
    vSQL := vSQL ||'SELECT ' || '''' ||v_componentValue || ''',' || in_periodSeq ||', txn.salestransactionseq, ';
    vSQL := vSQL || '   txn.linenumber, ';
    vSQL := vSQL || '   txn.sublinenumber, ';
    vSQL := vSQL || ' ' || vDecodeSQL || ' as eventtypeid, ';
    vSQL := vSQL || '   txn.salesorderseq, ';
    vSQL := vSQL || '   txn.businessunitmap, ';
    vSQL := vSQL || '   txn.genericattribute14 as classCode, ';
    vSQL := vSQL || '   Pos.Genericattribute4, ';
    vSQL := vSQL || '   case when ifnull(pos.genericattribute4,''#'')=ifnull(txn.genericattribute14,''#'') then 1 else 0 end, ';
    vSQL := vSQL || '   pos.genericBoolean4 as welcomePackage, ';
    vSQL := vSQL || '   pa.hireDate as contractDate, ';
    vSQL := vSQL || '   pos.genericDate4 as assignmentDate, ';
    vSQL := vSQL || '   Gpa.Genericdate6 As Leadersocdate,  ';
    vSQL := vSQL || '   pos.ruleElementOwnerSeq as AgtPosSeq, ';
    vSQL := vSQL || '   txn.genericAttribute12 as agentCode, ';
    vSQL := vSQL || '   Txn.Genericattribute13 As Agencycode, ';
    vSQL := vSQL || '   Txn.Businessunitmap, ';
    vSQL := vSQL || '   Gapos12.Genericdate19 As Starttransactiondate, ';
    vSQL := vSQL || '   txn.compensationdate as compdate ';
    vSQL := vSQL || ' FROM Cs_Salestransaction txn '; --PARTITION(' || vParname ||') Txn ';
    vSQL := vSQL || ' INNER JOIN cs_transactionassignment asg '; --PARTITION(' || vParname ||') asg ';
    vSQL := vSQL || '   ON txn.salestransactionseq=asg.salestransactionseq and asg.tenantid=''AIAS'' ';
    vSQL := vSQL || '   AND asg.positionname like ''%T%'' ';
    vSQL := vSQL || '   and ifnull(txn.genericattribute12,''#'') = substr(asg.positionname,4,8) ';
    vSQL := vSQL || ' INNER JOIN cs_position pos ';
    vSQL := vSQL || '   ON pos.removedate = '''||:cdt_EndOfTime ||'''';
    vSQL := vSQL || '   AND pos.name = asg.positionname ';
    vSQL := vSQL || '   And Pos.Effectivestartdate< to_date(''' ||to_char(venddate, 'yyyymmdd') || ''',''yyyymmdd'') ';
    vSQL := vSQL || '   /*and pos.tenantid=''AIAS''*/ and pos.effectiveEnddate>= to_date(''' ||to_char(venddate, 'yyyymmdd') || ''',''yyyymmdd'') ';
    vSQL := vSQL || ' left join cs_participant pa ';
    vSQL := vSQL || '   on pos.payeeSeq=pa.payeeSeq ';
    vSQL := vSQL || '    /*and pa.tenantid=''AIAS''*/ and pa.effectiveStartDate < to_date(''' ||to_char(venddate, 'yyyymmdd') || ''',''yyyymmdd'') ';
    vSQL := vSQL || '   and pa.effectiveEndDate >= to_date(''' ||to_char(venddate, 'yyyymmdd') || ''',''yyyymmdd'') ';
    vSQL := vSQL || '   and pa.removeDate='''||:cdt_EndOfTime ||'''';
    vSQL := vSQL || ' left join cs_gaparticipant gpa ';
    vSQL := vSQL || '   on pa.payeeSeq=gpa.payeeSeq ';
    vSQL := vSQL || '    /*and gpa.tenantid=''AIAS''*/ and gpa.effectiveStartDate < to_date(''' ||to_char(venddate, 'yyyymmdd') || ''',''yyyymmdd'') ';
    vSQL := vSQL || '   and gpa.effectiveEndDate >= to_date(''' ||to_char(venddate, 'yyyymmdd') || ''',''yyyymmdd'') ';
    vSQL := vSQL || '   And Gpa.Removedate='''||:cdt_EndOfTime ||'''';
    vSQL := vSQL || '   And Gpa.Pagenumber=0 ';
    vSQL := vSQL || ' Inner Join Cs_Position Posga12 ';
    vSQL := vSQL || '   on ''SGT''||txn.Genericattribute12=Posga12.Name ';
    vSQL := vSQL || '    /*and Posga12.tenantid=''AIAS''*/ And Posga12.Removedate='''||:cdt_EndOfTime ||'''';
    vSQL := vSQL || '   And Posga12.Effectivestartdate < to_date(''' ||to_char(venddate, 'yyyymmdd') || ''',''yyyymmdd'') ';
    vSQL := vSQL || '  /*and Posga12.tenantid=''AIAS''*/  And Posga12.Effectiveenddate  >= to_date(''' ||to_char(venddate, 'yyyymmdd') || ''',''yyyymmdd'') ';
    vSQL := vSQL || ' Inner Join Cs_Gaposition Gapos12 ';
    vSQL := vSQL || '   on Gapos12.Pagenumber=0 /*and Gapos12.tenantid=''AIAS''*/ ';
    vSQL := vSQL || '   And posga12.ruleElementOwnerSeq=Gapos12.ruleElementOwnerSeq ';
    vSQL := vSQL || '   And Gapos12.Removedate='''||:cdt_EndOfTime ||'''';
    vSQL := vSQL || '   And Gapos12.Effectivestartdate < to_date(''' ||to_char(venddate, 'yyyymmdd') || ''',''yyyymmdd'') ';
    vSQL := vSQL || '   and Gapos12.effectiveEndDate >= to_date(''' ||to_char(venddate, 'yyyymmdd') || ''',''yyyymmdd'') ';
    vSQL := vSQL || ' WHERE 1=1 /*and txn.tenantid=''AIAS''*/    and TXN.EVENTTYPESEQ IN (' || v_ET1 || ',' || v_ET2 || ',' ||v_ET3 || ',' || v_ET4 || ',' || v_ET5 || ',' || v_ET6 || ',' ||v_ET7 || ',' || v_ET8 || ',' || v_ET9 || ')';

    dbms_output:print_line('SQL is ' || vSQL);
    Log('SQL is ' || vSQL);
    execute immediate vSQL using Cdt_Endoftime, Cdt_Endoftime, Cdt_Endoftime, Cdt_Endoftime, Cdt_Endoftime;
    Log('End 1');

commit;

    commit;

    Log('Start 2');

    --Mark 20150820
    --  Merge  Into Cs_Salestransaction Txn
    Merge
    Into Cs_Salestransaction Txn
    Using (Select R.genericsequence1 as salestransactionseq,
                  --R.Genericattribute4 As Classcode,
                  R.Genericattribute5 As Classcode, --update by pos.classcode
                  R.Genericboolean1   As Classcodeflag
             From Sh_Query_Result R
            Where Component = v_componentValue
              and periodseq = in_periodSeq
              and r.genericboolean1 = 0 --only the ps.classcode is differet from txn.classcode, then will be update
           ) T
    on (t.salestransactionseq = txn.salestransactionseq
        /*and txn.tenantid='AIAS'*/)
    When Matched Then
      Update Set Txn.Genericattribute14 = T.Classcode;

    commit;
    Log('End  2');


commit;
 Log('Start  3');


    /* Mark -20150820 - start */
--    with transferset as
--     (select  p.ruleelementownerseq,
--             r.genericSequence1 as salestransactionseq,
--             0 as pagenumber,
--             r.genericdate1 as Contractdate,
--             gatxn.genericdate1 as TxnContractdate,
--             r.genericboolean2 as Welcomepackage,
--             gatxn.genericboolean1 as TxnWelcomepackage,
--             case
--               when (p.genericattribute1 = r.genericattribute7 and
--                    p.Transferdate is not null) then
--                p.Transferdate
--               else
--                m.oldTransferDate
--             end as TransferDate,
--             gatxn.genericdate2 as TxnTransferdate,
--             r.genericdate2 as AssignmentDate,
--             gatxn.genericdate3 as TxnAssignmentDate,
--             r.genericdate3 as Leadersocdate,
--             gatxn.Genericdate5 as TxnLeadersocdate,
--             r.genericdate4 as StartTransactionDate,
--             gatxn.genericdate7 as TxnStartTransactionDate
--        from Sh_Query_Result r
--        join (Select Ruleelementownerseq,
--                    Genericattribute1,
--                    effectivestartdate,
--                    effectiveenddate,
--                    Max(Genericdate3) Transferdate
--               From Cs_Position Pos
--              Where 1=1/*tenantid='AIAS'*/ and Removedate = to_date('01/01/2200', 'dd/mm/yyyy')
--              Group By Ruleelementownerseq,
--                       Genericattribute1,
--                       effectivestartdate,
--                       effectiveenddate) p
--          on p.ruleelementownerseq = r.genericSequence2
--         and p.Effectivestartdate <= r.Genericdate5
--         and p.effectiveenddate > r.genericDate5
--        join Cs_gaSalestransaction gatxn
--          on gatxn.salestransactionseq = r.genericSequence1
--         and gatxn.pagenumber = 0
--        left outer join (
--                        Select Agent_code,
--                          New_Agency_code,
--                          Max(Effective_Date) Oldtransferdate
--                          from dm_tbl_agent_move
--                         where Move_Type In ('31', '30')
--                            Or (Move_Type = '20' And
--                               Agency_Code != New_Agency_Code)
--                         group by agent_code, new_agency_code) m
--          on m.agent_code = r.genericAttribute6
--         and m.new_agency_code = r.genericAttribute7
--       where r.periodseq = in_periodSeq --2533274790398913
--         and component = v_componentValue --v_component
--        /*and gatxn.tenantid='AIAS'*/
--      )
--    select salestransactionseq,
--           transferdate,
--           contractdate,
--           welcomepackage,
--           assignmentdate,
--           leadersocdate,
--           starttransactiondate bulk collect
--      into l_txnseq,
--           l_transferdt,
--           l_contractdt,
--           l_welcomepk,
--           l_assigndt,
--           l_ldrsocdt,
--           l_starttxndt
--      from transferset t
--     where (nvl(TxnContractdate, to_date('01/01/2200', 'dd/mm/yyyy')) != nvl(Contractdate, to_date('01/01/2200', 'dd/mm/yyyy')) or
--           nvl(TxnWelcomepackage, 0) != nvl(Welcomepackage, 0) or 
--           nvl(TxnTransferdate, to_date('01/01/2200', 'dd/mm/yyyy')) != nvl(TransferDate, to_date('01/01/2200', 'dd/mm/yyyy')) or
--           nvl(TxnAssignmentDate, to_date('01/01/2200', 'dd/mm/yyyy')) != nvl(AssignmentDate, to_date('01/01/2200', 'dd/mm/yyyy')) or
--           nvl(TxnLeadersocdate, to_date('01/01/2200', 'dd/mm/yyyy')) != nvl(Leadersocdate, to_date('01/01/2200', 'dd/mm/yyyy')) or
--           nvl(TxnStartTransactionDate,to_date('01/01/2200', 'dd/mm/yyyy')) != nvl(StartTransactionDate, to_date('01/01/2200', 'dd/mm/yyyy')));
--
--    FORALL indx IN 1 .. l_txnseq.COUNT
--      update cs_gasalestransaction
--         set genericdate2    = l_transferdt(indx), --to_date('01/01/2200','dd/mm/yyyy'),
--             genericdate1    = l_contractdt(indx),
--             genericboolean1 = l_welcomepk(indx),
--             genericdate3    = l_assigndt(indx),
--             genericdate5    = l_ldrsocdt(indx),
--             genericdate7    = l_starttxndt(indx)
--       where salestransactionseq = l_txnseq(indx)
--         and pagenumber = 0
--         /*and tenantid='AIAS'*/;
        
        /*overwritten the above sql*/
        

    /* Mark -20150820 - end */

update cs_gasalestransaction tgt 
set tgt.genericdate2 = src.transferdate, 
tgt.genericdate1     = src.contractdate,
tgt.genericboolean1  = src.welcomepackage,
tgt.genericdate3     = src.assignmentdate,
tgt.genericdate5     = src.leadersocdate,
tgt.genericdate7     = src.starttransactiondate
from  cs_gasalestransaction tgt, 
    (select salestransactionseq,
           transferdate,
           contractdate,
           welcomepackage,
           assignmentdate,
           leadersocdate,
           starttransactiondate 
      from  (select  p.ruleelementownerseq,
             r.genericSequence1 as salestransactionseq,
             0 as pagenumber,
             r.genericdate1 as Contractdate,
             gatxn.genericdate1 as TxnContractdate,
             r.genericboolean2 as Welcomepackage,
             gatxn.genericboolean1 as TxnWelcomepackage,
             case
               when (p.genericattribute1 = r.genericattribute7 and
                    p.Transferdate is not null) then
                p.Transferdate
               else
                m.oldTransferDate
             end as TransferDate,
             gatxn.genericdate2 as TxnTransferdate,
             r.genericdate2 as AssignmentDate,
             gatxn.genericdate3 as TxnAssignmentDate,
             r.genericdate3 as Leadersocdate,
             gatxn.Genericdate5 as TxnLeadersocdate,
             r.genericdate4 as StartTransactionDate,
             gatxn.genericdate7 as TxnStartTransactionDate
        from Sh_Query_Result r
        join (Select Ruleelementownerseq,
                    Genericattribute1,
                    effectivestartdate,
                    effectiveenddate,
                    Max(Genericdate3) Transferdate
               From Cs_Position Pos
              Where 1=1 /*tenantid='AIAS'*/ and Removedate = to_date('01/01/2200', 'dd/mm/yyyy')
              Group By Ruleelementownerseq,
                       Genericattribute1,
                       effectivestartdate,
                       effectiveenddate) p
          on p.ruleelementownerseq = r.genericSequence2
         and p.Effectivestartdate <= r.Genericdate5
         and p.effectiveenddate > r.genericDate5
        join Cs_gaSalestransaction gatxn
          on gatxn.salestransactionseq = r.genericSequence1
         and gatxn.pagenumber = 0
        left outer join (Select Agent_code,
                          New_Agency_code,
                          Max(Effective_Date) Oldtransferdate
                          from dm_tbl_agent_move
                         where Move_Type In ('31', '30')
                            Or (Move_Type = '20' And
                               Agency_Code != New_Agency_Code)
                         group by agent_code, new_agency_code) m
          on m.agent_code = r.genericAttribute6
         and m.new_agency_code = r.genericAttribute7
       where r.periodseq = in_periodSeq --2533274790398913
         and component = v_componentValue --v_component
        /*and gatxn.tenantid='AIAS'*/ ) t
     where (ifnull(TxnContractdate, to_date('01/01/2200', 'dd/mm/yyyy')) != ifnull(Contractdate, to_date('01/01/2200', 'dd/mm/yyyy')) or
           ifnull(TxnWelcomepackage, 0) != ifnull(Welcomepackage, 0) or 
           ifnull(TxnTransferdate, to_date('01/01/2200', 'dd/mm/yyyy')) != ifnull(TransferDate, to_date('01/01/2200', 'dd/mm/yyyy')) or
           ifnull(TxnAssignmentDate, to_date('01/01/2200', 'dd/mm/yyyy')) != ifnull(AssignmentDate, to_date('01/01/2200', 'dd/mm/yyyy')) or
           ifnull(TxnLeadersocdate, to_date('01/01/2200', 'dd/mm/yyyy')) != ifnull(Leadersocdate, to_date('01/01/2200', 'dd/mm/yyyy')) or
           ifnull(TxnStartTransactionDate,to_date('01/01/2200', 'dd/mm/yyyy')) != ifnull(StartTransactionDate, to_date('01/01/2200', 'dd/mm/yyyy')))) src 
where 
tgt.salestransactionseq=src.salestransactionseq 
and tgt.pagenumber=0;
        
    commit;
    Log('End  3');


commit;
 
  END; --Sp_Update_Txn