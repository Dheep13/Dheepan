CREATE procedure comConvertAgentRole(in i_periodSeq bigint) 
as
  begin
    DECLARE gv_error VARCHAR(1000); /* package/session variable */

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_perviousAgent VARCHAR(30) = '';  /* ORIGSQL: v_perviousAgent varchar2(30) := ''; */
    DECLARE v_perviousRown BIGINT = 1;  /* ORIGSQL: v_perviousRown int := 1; */
    DECLARE v_perAgentRole VARCHAR(30);  /* ORIGSQL: v_perAgentRole varchar2(30); */
    DECLARE v_perAgencyLeader VARCHAR(30);  /* ORIGSQL: v_perAgencyLeader varchar2(30); */
    DECLARE v_perAgencyCode VARCHAR(30);  /* ORIGSQL: v_perAgencyCode varchar2(30); */
    DECLARE v_perDistrict VARCHAR(30);  /* ORIGSQL: v_perDistrict varchar2(30); */
    DECLARE v_perClass VARCHAR(10);  /* ORIGSQL: v_perClass varchar2(10); */
    DECLARE v_perDteEffective TIMESTAMP;  /* ORIGSQL: v_perDteEffective date; */
    DECLARE v_cutoffdate TIMESTAMP;  /* ORIGSQL: v_cutoffdate date; */
	 DECLARE CURSOR dbmtk_cursor_2023
    FOR (select agt.*,
                         case
                           when agt.old_role IN ('DM', 'FSD') then
                            'FSD'
                           when agt.old_role in ('ADM', 'FSAD') then
                            'FSAD'
                           when agt.old_role in ('M1', 'FSM') then
                            'FSM'
                           when agt.old_role = 'AM' then
                            'AM'
                           when agt.old_role in ('AAL', 'FSC') then
                            'FSC'
                         end oldTitle,
                         case
                           when agt.new_role IN ('DM', 'FSD') then
                            'FSD'
                           when agt.new_role in ('ADM', 'FSAD') then
                            'FSAD'
                           when agt.new_role in ('M1', 'FSM') then
                            'FSM'
                           when agt.new_role = 'AM' then
                            'AM'
                           when agt.new_role in ('AAL', 'FSC') then
                            'FSC'
                         end newTitle,
                         row_number() over(partition by agt.txtagt order by agt.dteEffective) as rown,
                         max(agt.dteEffective) over(partition by agt.txtagt) as maxEffectiveDate,
                         Count(1) Over(Partition By Agt.Txtagt) As Cnt
                    From Dm_Tbl_Agent_Role_Move Agt
                   Where 1 = 1
                     AND agt.dteeffective <= v_cutoffdate
                   order by txtagt, dteEffective);
                  
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            gv_error = 'Error [comConvertAgentRole]: ' || ::SQL_ERROR_MESSAGE  /* ORIGSQL: sqlerrm */

            /* RESOLVE: Standard Package call(not converted): 'dbms_utility.format_error_backtrace' not supported, manual conversion required */
           /* dbms_utility.format_error_backtrace*/
           ;
            /* Saving modified package/session variable 'gv_error': */ 
            SET SESSION 'GV_ERROR' = :gv_error;
            RESIGNAL;
            ROLLBACK;
        END;                  
                  
SELECT SESSION_CONTEXT('GV_ERROR') INTO gv_error FROM SYS.DUMMY ;

    select refdatevalue
      into v_cutoffdate
      from sh_reference
     where refid = 'CUTOVERDATE';

    Log('60');
    --delete from sh_agent_role;
    execute immediate 'truncate table sh_agent_role';
    Log('60');

    Log('61');

    for c_agt as dbmtk_cursor_2023
     do

      if c_agt.dteEffective = v_perDteEffective and v_perviousAgent = c_agt.txtAgt and C_Agt.Rown != C_Agt.Cnt then -- modified by nelson
        /*GOTO end_loop*/
      end if;

      if c_agt.rown = 1 and not (c_agt.dteEffective = v_perDteEffective and v_perviousAgent = c_agt.txtAgt and C_Agt.Rown != C_Agt.Cnt) then
        --first row
        insert /*+ append*/
        into sh_agent_role
        values
          (c_agt.txtAgt, --agentcode
           c_agt.oldTitle, --agentRole
           c_agt.txtOldAgyLeader, --agencyLeader
           c_agt.txtOldAgy, --agencyCode
           c_agt.txtOldDistrict, --disctrict
           c_agt.old_class, --classcode
           To_Date('1/1/1900', 'mm/dd/yyyy'), --efftiveStartDate
           C_Agt.Dteeffective,
           0);

      end if;

      If C_Agt.Rown = C_Agt.Cnt and not(c_agt.dteEffective = v_perDteEffective and v_perviousAgent = c_agt.txtAgt and C_Agt.Rown != C_Agt.Cnt) then

        if c_agt.rown > 1 and V_Perviousagent = C_Agt.Txtagt then
          insert into sh_agent_role
          values
            (c_agt.txtAgt, --agentcode
             v_perAgentRole, --agentRole
             v_perAgencyLeader, --agencyLeader
             v_perAgencyCode, --agencyCode
             v_perDistrict, --disctrict
             v_perClass, --class
             V_Perdteeffective, --efftiveStartDate
             C_Agt.Dteeffective, --effectiveEndDate
             0);
        end if;

        if (c_agt.rown > 1 and V_Perviousagent = C_Agt.Txtagt) or
           c_agt.rown = 1 then
          --last row
          insert into sh_agent_role
          values
            (c_agt.txtAgt, --agentcode
             c_agt.newTitle, --agentRole
             c_agt.txtNewAgyLeader, --agencyLeader
             c_agt.txtNewAgy, --agencyCode
             c_agt.txtNewDistrict, --disctrict
             c_agt.new_class, --classcode
             C_Agt.Dteeffective, --efftiveStartDate
             add_days(V_Cutoffdate , 1), --follow option1
             0);

        end if;

      end if;

      if c_agt.rown > 1 and c_agt.rown < c_agt.cnt and not(c_agt.dteEffective = v_perDteEffective and v_perviousAgent = c_agt.txtAgt and C_Agt.Rown != C_Agt.Cnt) then
        --current
        insert into sh_agent_role
        values
          (c_agt.txtAgt, --agentcode
           v_perAgentRole, --agentRole
           v_perAgencyLeader, --agencyLeader
           v_perAgencyCode, --agencyCode
           v_perDistrict, --disctrict
           v_perClass, --class
           V_Perdteeffective, --efftiveStartDate
           C_Agt.Dteeffective, --effectiveEndDate
           0);

      end if;

      /*
      if c_agt.cnt=2 and c_agt.rown=2  then

        insert into sh_agent_role
        values (
        c_agt.txtAgt, --agentcode
        v_perAgentRole,--agentRole
        v_perAgencyLeader, --agencyLeader
        v_perAgencyCode, --agencyCode
        v_perDistrict, --disctrict
        v_perClass, --class
        v_perDteEffective, --efftiveStartDate
        c_agt.dteEffective --effectiveEndDate
        );

      end if;*/

--end_loop:  /* ORIGSQL: <<end_loop>> */

      v_perviousRown    := c_agt.rown;
      v_perviousAgent   := c_agt.txtAgt;
      v_perAgentRole    := c_agt.newTitle;
      v_perAgencyLeader := c_agt.txtNewAgyLeader;
      v_perAgencyCode   := c_agt.txtNewAgy;
      v_perDistrict     := c_agt.txtNewDistrict;
      v_perClass        := c_agt.old_class;
      v_perDteEffective := c_agt.dteEffective;

      /*null;*/

    end for;

    commit;
    Log('61');

    delete from sh_agent_role d where d.effectivestartdate = d.effectiveenddate; -- added by nelson

        commit;

  end --comConvertAgentRole;
  
 