CREATE OR REPLACE PROCEDURE EXT.TRYG_SH_Stagehook ( 
IN in_Stage NVARCHAR(127), 
IN in_Mode NVARCHAR(127) DEFAULT NULL , 
IN in_Period NVARCHAR(127) DEFAULT NULL, 
IN in_PeriodSeq BIGINT DEFAULT NULL, 
IN in_Group NVARCHAR(127) DEFAULT NULL, 
IN in_Tracing NVARCHAR(127) DEFAULT NULL, 
IN in_Connection NVARCHAR(127) DEFAULT NULL, 
IN in_userName NVARCHAR(127) DEFAULT NULL, 
IN in_Calendar NVARCHAR(127) DEFAULT NULL, 
IN in_calendarSeq BIGINT DEFAULT NULL,
IN in_ProcessingUnitSeq BIGINT DEFAULT NULL
) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER 
DEFAULT SCHEMA EXT AS 
/*---------------------------------------------------------------------
    | Author: Sharath K
    | Project Title: Consultant
    | Company: SAP Callidus
    | Initial Version Date: 19-Apr-2022
    |----------------------------------------------------------------------
    | Procedure Purpose: 
    | Version: 1	19-Apr-2022	Intial Version
    -----------------------------------------------------------------------
    */
BEGIN
	---------------------------------------------------------------------------
	-------- Variable Declarations --------------------------------------------
	DECLARE v_removeDate DATE;
	DECLARE v_procedureName VARCHAR(50);
	DECLARE v_slqerrm VARCHAR(4000);
	/* AMR 20230523 v_batchName increased from 50 to 100 */
	DECLARE v_batchName VARCHAR(100);

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN v_slqerrm := ::SQL_ERROR_MESSAGE;
		CALL EXT.TRYG_LOG(v_procedureName,'ERROR = '||IFNULL(:v_slqerrm,'') ,NULL);
	END;


	
--------------------------------------------------------------------------- 
	v_procedureName = 'TRYG_SH_STAGEHOOK';
	v_removeDate = TO_DATE('01/01/2200','mm/dd/yyyy');

	EXT.TRYG_LOG(v_procedureName,'####   BEGIN   #### STAGE = '||in_Stage,NULL);

	EXT.TRYG_LOG(v_procedureName,'#### in_Stage = '||in_Stage,NULL); 
--	EXT.TRYG_LOG(v_procedureName,'#### in_Mode  = '||in_Mode,NULL); 
	EXT.TRYG_LOG(v_procedureName,'#### in_Period  = '||in_Period,NULL); 
	EXT.TRYG_LOG(v_procedureName,'#### in_PeriodSeq  = '||in_PeriodSeq,NULL); 
--	EXT.TRYG_LOG(v_procedureName,'#### in_Group  = '||in_Group,NULL); 
--	EXT.TRYG_LOG(v_procedureName,'#### in_Tracing  = '||in_Tracing,NULL); 
--	EXT.TRYG_LOG(v_procedureName,'#### in_Connection  = '||in_Connection,NULL); 
--	EXT.TRYG_LOG(v_procedureName,'#### in_userName  = '||in_userName,NULL); 
--	EXT.TRYG_LOG(v_procedureName,'#### in_Calendar  = '||in_Calendar,NULL); 
--	EXT.TRYG_LOG(v_procedureName,'#### in_calendarSeq  = '||in_calendarSeq,NULL); 
	EXT.TRYG_LOG(v_procedureName,'#### in_ProcessingUnitSeq  = '||in_ProcessingUnitSeq,NULL); 

--get bacthname for summary transaction stagehook
	SELECT pr.batchname INTO v_batchName default Null
	FROM CS_PIPELINERUN pr
	JOIN CS_STAGESUMMARY SS ON ss.PIPELINERUNSEQ =pr.PIPELINERUNSEQ 
	JOIN CS_STAGETYPE st ON st.STAGETYPESEQ =ss.STAGETYPESEQ 
	WHERE ss.stoptime IS NULL
	and ss.starttime IS NOT NULL
	AND (st.name LIKE '%Validate%' OR st.name LIKE '%Transfer%')
	AND ss.STATUS ='Running';

	IF in_Stage = '__Validate'THEN
		CALL EXT.TRYG_SH_SUMMARY(v_batchName);
	END IF;

	IF in_Stage = '__ResetFromClassify'THEN
--		CALL EXT.TRYG_SH_CLAWBACK_ASSIGN(in_PeriodSeq,in_ProcessingUnitSeq);
		CALL EXT.TRYG_SH_CLAWBACK(in_PeriodSeq,in_ProcessingUnitSeq);
		CALL EXT.TRYG_SH_POLICYPAY(in_PeriodSeq,in_ProcessingUnitSeq);
	END IF;
	IF in_Stage = '__ResetFromAllocate'THEN
--		CALL EXT.TRYG_SH_CLAWBACK_ASSIGN(in_PeriodSeq,in_ProcessingUnitSeq);
		CALL EXT.TRYG_SH_CLAWBACK(in_PeriodSeq,in_ProcessingUnitSeq);
	END IF;
	IF in_Stage = '__Allocate'THEN
		CALL EXT.TRYG_SH_POLICYPAY(in_PeriodSeq,in_ProcessingUnitSeq);
		--AMR Temporary removeal for testing 20230823
		CALL EXT.TRYG_SH_PORTFOLIO(in_PeriodSeq,in_ProcessingUnitSeq);
	END IF;
	IF in_Stage = 'Allocate__'THEN
		CALL EXT.TRYG_LOG(v_procedureName,'NO PROCS IN STAGE = '||in_Stage,NULL);
	END IF;
	IF in_Stage = 'Reward__'THEN
		-- CALL EXT.TRYG_LOG(v_procedureName,'NO PROCS IN STAGE = '||in_Stage,NULL);
		CALL EXT.TRYG_SH_REDEMPTION_REPORT(in_PeriodSeq,in_ProcessingUnitSeq);
		
	END IF;


	COMMIT;
	EXT.TRYG_LOG(v_procedureName,'####   END   #### STAGE = '||in_Stage,NULL);
END