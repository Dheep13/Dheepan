CREATE PROCEDURE EXT.CTAS_SP_SPMHIERARCHY_OUT(OUT FILENAME varchar(120), IN plRunSeq varchar(255) default '', IN pPeriodName varchar(255) default '')
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER DEFAULT SCHEMA EXT AS 
BEGIN
/**************************************************************************************************
	This stored procedure is for Creating SPM Hierarchy Data Outbound Feed

	REVISIONS:
	Ver        Date          Author           Description
	---------  -----------   ---------------  -----------------------------------------------------
	1.0       18-OCT-2023		Anand		     Initial creation
	2.0       19-OCT-2023		Deepan		     Added outbound logic, added exception handling,
												 replaced ruleelementownerseq with positionname
    
***************************************************************************************************/
/* DSC : Start- Added exception handling*/
DECLARE v_proc_name varchar2(100);
DECLARE v_TenantID varchar2(50);
DECLARE v_yyyymmdd varchar2(10);
DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 
    BEGIN 
    rollback;
    ext.ctas_event_log (v_proc_name,::SQL_ERROR_CODE || ' . ' ||::SQL_ERROR_MESSAGE,0);
    commit;
resignal;

END;
DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    BEGIN 
    rollback;
    ext.ctas_event_log (v_proc_name,::SQL_ERROR_CODE || ' . ' ||::SQL_ERROR_MESSAGE,0);
    commit;
resignal;
END;
/* DSC : End- Added exception handling*/

v_proc_name :=::CURRENT_OBJECT_NAME;
ext.ctas_event_log (:v_proc_name,'Begin '|| now(),0);

select tenantid into v_TenantID from cs_tenant;

select case when length(month(current_date))= 1 then year(current_date)||'-'||'0'||month(current_date)||'-'||'27' else 
year(current_date)||'-'||month(current_date)||'-'||'27' end 
into v_yyyymmdd 
from sys.dummy;

/*delete historical data*/
-- delete from EXT.CTAS_HIERARCHY;
/* DSC : Start - Truncate instead of delete */
truncate table EXT.CTAS_HIERARCHY;
/* DSC : End - Truncate instead of delete */

-- commit; -- commit not required after truncate

/*Load SPM Hierarchy data in hierarchy table */
INSERT INTO EXT.CTAS_HIERARCHY 
			(
			        HIERARCHY_RANK,
					HIERARCHY_TREE_SIZE,
					HIERARCHY_PARENT_RANK,
					HIERARCHY_ROOT_RANK,
					HIERARCHY_LEVEL,
					HIERARCHY_IS_CYCLE,
					HIERARCHY_IS_ORPHAN,
					NODE_SEQ,
					PARENT_SEQ,
					PARENT_ID,
					PARENT_NAME,
					CHILD_ID,
					CHILD_NAME,
					EFFECTIVESTARTDATE,
					EFFECTIVEENDDATE,
					PPOSSEQ,
					PARENT_SPMTITLE,
					PARENT_CNTTITLE,
					CPOSSEQ,
					CHILD_TITLE,
					CHILD_CNTTITLE
			)
(
SELECT  HIERARCHY_RANK,
		HIERARCHY_TREE_SIZE,
		HIERARCHY_PARENT_RANK,
		HIERARCHY_ROOT_RANK,
		HIERARCHY_LEVEL,
		HIERARCHY_IS_CYCLE,
		HIERARCHY_IS_ORPHAN,
		H.NODE_ID NODE_SEQ,
		H.PARENT_ID PARENT_SEQ,
		P.PARENT_ID,
		PARENT_NAME,
		CHILD_ID,
		CHILD_NAME,
		C.EFFECTIVESTARTDATE,
		C.EFFECTIVEENDDATE,
		PPOSSEQ,
		PARENT_SPMTITLE,
		PARENT_CNTTITLE,
		CPOSSEQ,
		CHILD_TITLE,
		CHILD_CNTTITLE
 from HIERARCHY (
			 		Source (
			 				select DISTINCT ruleelementownerseq node_id ,
			 					   managerseq parent_id
			 					   --EFFECTIVESTARTDATE,
			 					   --EFFECTIVEENDDATE
							from cs_position p
							where removedate > now() --and ruleelementownerseq = 4785074604151165
			 				)
					-- start where ruleelementownerseq = 4785074604152399 -- DSC : ruleelementownerseq will not be the same for all environments  
					start where name ='1029177_Non_Payable' -- DSC : replace ruleelementownerseq with position name
				) H,
	  (
			SELECT  DISTINCT PPY.PAYEEID Parent_Id,
					PPA.FIRSTNAME || ', ' || PPA.LASTNAME Parent_Name,
			ppo.ruleelementownerseq pposseq, 		
			PTI.NAME PARENT_SPMTITLE,							-- Added for Testing
			PPO.GENERICATTRIBUTE1 PARENT_CNTTITLE,						-- Added for Testing
			to_char(PPO.EFFECTIVESTARTDATE,'mm/dd/yyyy') EFFECTIVESTARTDATE,
			to_char(PPO.EFFECTIVEENDDATE,'mm/dd/yyyy') EFFECTIVEENDDATE			
			FROM	CS_POSITION PPO,
					CS_PARTICIPANT PPA,
					CS_PAYEE PPY,
					CS_TITLE PTI
			WHERE	PPO.REMOVEDATE > NOW() AND
					PPA.REMOVEDATE > NOW() AND 
					PPO.PAYEESEQ = PPA.PAYEESEQ AND
					PPA.PAYEESEQ = PPY.PAYEESEQ
					AND PPO.TITLESEQ = PTI.RULEELEMENTOWNERSEQ AND PTI.REMOVEDATE > NOW() 
	  ) P,
	  (
			SELECT  DISTINCT CPY.PAYEEID CHILD_Id,				
					CPA.FIRSTNAME || ', ' || CPA.LASTNAME Child_Name,
			cpo.ruleelementownerseq cposseq, 			
			CTI.NAME CHILD_TITLE,								-- Added for Testing
			CPO.GENERICATTRIBUTE1 CHILD_CNTTITLE,						-- Added for Testing
					CPO.EFFECTIVESTARTDATE,
					CPO.EFFECTIVEENDDATE
			FROM	CS_POSITION CPO,
					CS_PARTICIPANT CPA,
					CS_PAYEE CPY
					, CS_TITLE CTI
			WHERE	CPO.REMOVEDATE > NOW() AND
					CPA.REMOVEDATE > NOW() AND 
					CPO.PAYEESEQ = CPA.PAYEESEQ AND
					CPA.PAYEESEQ = CPY.PAYEESEQ
			AND CPO.TITLESEQ = CTI.RULEELEMENTOWNERSEQ AND  CTI.REMOVEDATE > NOW()
	  ) C
where h.node_id = c.cposseq and h.parent_id = p.pposseq	 
--AND P.EFFECTIVESTARTDATE = C.EFFECTIVESTARTDATE 
order by hierarchy_parent_rank, hierarchy_rank
);
-- ext.ctas_event_log (:v_proc_name,'Count of records loaded into stagesalestransactionassign for : ',::ROWCOUNT);--DSC : probably a copy paste issue

ext.ctas_event_log (:v_proc_name,'Count of hierarchy records loaded into CTAS_HIERARCHY : ',::ROWCOUNT);--DSC : replaced with correct comments

/*DSC : Start - Adding the outbound logic also in the same proc*/

truncate table ext.CTAS_SPM_HIERARCHY_OUT;

insert into ext.CTAS_SPM_HIERARCHY_OUT
(SELECT PARENT_ID, 
            PARENT_NAME,
            CHILD_ID,
            CHILD_NAME , 
            EFFECTIVESTARTDATE, 
            EFFECTIVEENDDATE 
FROM    EXT.CTAS_HIERARCHY
);

commit;

ext.ctas_event_log (::CURRENT_OBJECT_NAME,'SPM Hierarchy extract generated',::ROWCOUNT);
FILENAME := 'SPMHIERARCHY_'||v_TenantID||'_'||:v_yyyymmdd||'.txt';
/*DSC : End - Adding the outbound logic also in the same proc*/

ext.ctas_event_log (:v_proc_name,'End ',0);

commit;
 
END