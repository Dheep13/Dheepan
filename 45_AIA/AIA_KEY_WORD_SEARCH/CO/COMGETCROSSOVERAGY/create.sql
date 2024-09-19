CREATE function comGetCrossoverAgy ( IN i_comp VARCHAR(255),  
    IN I_wAgyLdr VARCHAR(255),   
    IN i_policyIssueDate TIMESTAMP) 
   RETURNS dbmtk_function_result VARCHAR(255)   /* ORIGSQL: return string */
 --add by nelson
as 
begin 
	DECLARE Gv_Periodseq BIGINT; /* package/session variable */
    DECLARE gv_CrossoverEffectiveDate TIMESTAMP; /* package/session variable */

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */

    DECLARE v_oldDM VARCHAR(30);  /* ORIGSQL: v_oldDM varchar2(30); */
    DECLARE v_countsetup BIGINT;  /* ORIGSQL: v_countsetup number(10); */
    DECLARE v_odm VARCHAR(30);  /* ORIGSQL: v_odm varchar2(30); */
    DECLARE v_effdate TIMESTAMP;  /* ORIGSQL: v_effdate date; */

    DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
        BEGIN
                dbmtk_function_result = NULL;
        END;

        SELECT CAST(SESSION_CONTEXT('GV_PERIODSEQ') AS BIGINT) INTO Gv_Periodseq FROM SYS.DUMMY ;
        SELECT TO_TIMESTAMP(SESSION_CONTEXT('GV_CROSSOVEREFFECTIVEDATE'), 'yyyy Mon dd hh24:mi:ss:ff3') INTO gv_CrossoverEffectiveDate FROM SYS.DUMMY ;
        
    --Log('70');


--add by nelson start

    if i_comp = 'PI' then

      ---add version 5 (If Payee only include one setup , it will assign to old DM)
      select count(1), max(TXTOLDDMCODE), max(Dteeffectivedate)
        into v_countsetup, v_odm, v_effdate
        from (select max(ST.TXTOLDDMCODE) TXTOLDDMCODE,
                     ST.Dteeffectivedate,
                     ST.Txtagt
                from In_Pi_Aor_Setup ST, Cs_Period PT
               where 'SGT' || to_number(ST.Txtagt) = I_wAgyLdr
                 and I_Policyissuedate <= ST.Dteeffectivedate
                 and ST.Dtecycle = add_days(PT.Enddate ,- 1)
                 and PT.Periodseq = Gv_Periodseq
                 And ST.Txttype in ('C')
                 and ST.Decstatus = 0
               group by ST.Dteeffectivedate, ST.Txtagt)
       group by Txtagt;

      if v_countsetup = 1 then
        V_oldDM := v_odm;
        Gv_Crossovereffectivedate := v_effdate;
      else

      select ST.Txtolddistrict, ST.Dteeffectivedate
      Into v_oldDM, Gv_Crossovereffectivedate
      from  In_Pi_Aor_Setup ST,Cs_Period PT
      where 'SGT'||to_number(ST.Txtagt)=I_wAgyLdr
      and   ST.Dteeffectivedate = (
         Select MIN(S.Dteeffectivedate)
          From In_Pi_Aor_Setup S,Cs_Period P
          Where 'SGT'||to_number(S.Txtagt)=I_wAgyLdr
          And I_Policyissuedate <= S.Dteeffectivedate
          And S.Dtecycle = add_days(p.Enddate ,- 1)
          And P.Periodseq=Gv_Periodseq
          And S.Txttype in ('C')
          and S.Decstatus = 0)
      and ST.Dtecycle = add_days(PT.Enddate ,- 1)
      and PT.Periodseq=Gv_Periodseq
      and I_Policyissuedate >= (select max(g.effectiveenddate) from sh_agent_role g
                                   where g.agentcode = to_number(ST.Txtagt)
                                   and   g.effectiveenddate < ST.dteeffectivedate);

    end if;
    else if   i_comp = 'AOR' then

      ---add version 5 (If Payee only include one setup , it will assign to old DM)
      select count(1), max(Txtolddistrict), max(Dteeffectivedate)
        into v_countsetup, v_odm, v_effdate
        from (select max(ST.Txtolddistrict) Txtolddistrict,
                     ST.Dteeffectivedate,
                     ST.Txtagt
                from In_Pi_Aor_Setup ST, Cs_Period PT
               where 'SGT' || to_number(ST.Txtagt) = I_wAgyLdr
                 and I_Policyissuedate <= ST.Dteeffectivedate
                 and ST.Dtecycle =add_days(PT.Enddate ,- 1)
                 and PT.Periodseq = Gv_Periodseq
                 And ST.Txttype in ('C','D')
                 and ST.Decstatus = 0
               group by ST.Dteeffectivedate, ST.Txtagt)
       group by Txtagt;

       if v_countsetup = 1 then
        V_oldDM := v_odm;
        Gv_Crossovereffectivedate := v_effdate;

      else


      select ST.Txtolddistrict, ST.Dteeffectivedate
      Into v_oldDM, Gv_Crossovereffectivedate
      from  In_Pi_Aor_Setup ST,Cs_Period PT
      where PT.tenantid='AIAS' and 'SGT'||to_number(ST.Txtagt)=I_wAgyLdr
      and   ST.Dteeffectivedate = (
         Select MIN(S.Dteeffectivedate)
          From In_Pi_Aor_Setup S,Cs_Period P
          Where 'SGT'||to_number(S.Txtagt)=I_wAgyLdr
          And I_Policyissuedate <= S.Dteeffectivedate
          And S.Dtecycle = add_days(p.Enddate ,- 1)
          And P.Periodseq=Gv_Periodseq
          And S.Txttype in ('C','D')
          and S.Decstatus = 0)
      and ST.Dtecycle = add_days(PT.Enddate ,- 1)
      and PT.Periodseq=Gv_Periodseq
      and I_Policyissuedate >= (select max(g.effectiveenddate) from sh_agent_role g
                                   where g.agentcode = to_number(ST.Txtagt)
                                   and   g.effectiveenddate < ST.dteeffectivedate);

    end if;

    end if;
   
   end if;
    --Log('70');

            dbmtk_function_result = :v_oldDM;
--add by nelson end

end --comGetCrossoverAgy;


