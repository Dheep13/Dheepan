--liquibase formatted sql


--changeset jcadby:cs_preferences_delete1 splitStatements:false stripComments:false
--comment: preferences delete

delete from cs_preferences where name='pipeline.generateAssignment' and tenantid='1259';
--changeset jcadby:cs_preferences_Inserts1 splitStatements:false stripComments:false
--comment: preferences insert
insert into cs_preferences values ('1259','__^^CallidusSystemUser^^__',    'pipeline.generateAssignment',    'true');


--changeset jcadby:cs_preferences_delete1 splitStatements:false stripComments:false
--comment: preferences delete    
delete from cs_preferences where name='pipeline.masterRuleName' and tenantid='1259';    

--changeset jcadby:cs_preferences_Inserts2 splitStatements:false stripComments:false
--comment: preferences insert  
insert into cs_preferences values ('1259','__^^CallidusSystemUser^^__','pipeline.masterRuleName','TANDQ_ASSIGNMENT_GENERATION');

commit;
