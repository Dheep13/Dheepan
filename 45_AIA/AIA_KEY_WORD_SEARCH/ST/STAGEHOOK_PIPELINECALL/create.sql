CREATE procedure ext.Stagehook_PipelineCALL ( in 	i_stage             varchar(255),
in 	i_mode              varchar(255),
in 	i_period            varchar(255),
in 	i_periodSeq         bigint,
in 	i_userName          varchar(255),
in 	i_calendar          varchar(255),
in 	i_calendarSeq       bigint,
in 	i_processingUnitSeq bigint) 
	 LANGUAGE SQLSCRIPT 
	 SQL SECURITY INVOKER
	 AS 
begin 
using ext.SA_STAGEHOOK as STAGEHOOK;

	STAGEHOOK:run(i_stage,i_mode,i_period,i_periodSeq,i_userName,i_calendar,i_calendarSeq,i_processingUnitSeq);
end