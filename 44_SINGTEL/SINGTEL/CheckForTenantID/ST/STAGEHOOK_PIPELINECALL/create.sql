CREATE procedure ext.Stagehook_PipelineCALL ( in_stage nVARCHAR(50),
	 in_periodseq bigint,
	 in_processingunitseq bigint ) 
	 LANGUAGE SQLSCRIPT 
	 SQL SECURITY INVOKER
	 AS 
begin 
	
if in_stage = '__ResetFromClassify'
then 
sh_resetfromclassify_pre(in_periodseq,in_processingunitseq);
end if;

if in_stage = '__Allocate'
then 
sh_allocate_pre(in_periodseq,in_processingunitseq);
end if;
	
end