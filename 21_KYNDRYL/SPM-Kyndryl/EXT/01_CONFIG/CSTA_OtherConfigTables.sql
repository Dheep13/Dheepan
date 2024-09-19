delete from CSTA_RUNPARAMETERS;
delete from CSTA_RULE;
delete from CSTA_MASTERRULE;
delete from CSTA_RUNLOG;
delete from CSTA_JOINCONDITION;
delete from CSTA_EXPRESSION;
delete from CSTA_TARGETFIELD;
delete from CSTA_EXECUTIONHINT;
delete from CSTA_COPYFIELD;
COMMIT;
 
insert into CSTA_MASTERRULE (tenantid,masterruleseq,masterrulename,effectivestartdate,effectiveenddate,
   removedate,createdate,createdby,modifiedby,islast,description)
values
('1259',50946970784628737,'TANDQ_ASSIGNMENT_GENERATION',to_date('19000101','YYYYMMDD')
                                                       ,to_date('22000101','YYYYMMDD')
                                                       ,to_date('22000101','YYYYMMDD'),current_date,'SA','SA',1,'TANDQ');
                                                       
insert into CSTA_MASTERRULE (tenantid,masterruleseq,masterrulename,effectivestartdate,effectiveenddate,
   removedate,createdate,createdby,modifiedby,islast,description)
values
('1259',50946970784628737,'HP_ASSIGNMENT_GENERATION',to_date('19000101','YYYYMMDD')
                                                       ,to_date('22000101','YYYYMMDD')
                                                       ,to_date('22000101','YYYYMMDD'),current_date,'SA','SA',1,'TANDQ');
 
 TENANTID MASTERRULESEQ MASTERRULENAME EFFECTIVESTARTDATE EFFECTIVEENDDATE REMOVEDATE CREATEDATE CREATEDBY MODIFIEDBY ISLAST DESCRIPTION
1259 5.0947E+16 HP_ASSIGNMENT_GENERATION 00:00.0 00:00.0 00:00.0 15:06.0 SA SA 1 
 
insert into CSTA_RULE (tenantid,masterruleseq,ruleseq,runorder,GENERiCcLASSIFIERTYPENAME,rulename,executerule,
  effectivestartdate,effectiveenddate,removedate,createdate,createdby,modifiedby,prerulesql,postrulesql,selectorid,
  generatedSQL,categorytreeseq,altersession1,altersession2,generateSQL,islast,description) values
('1259',50946970784628737,50665495807930581,1,NULL,'TANDQ RULE01','Y',to_date('19000101','YYYYMMDD')
                                                               ,to_date('22000101','YYYYMMDD')
                                                               ,to_date('22000101','YYYYMMDD'),current_date,
 'DR','DR',
  'CALL EXT.CSQ_SP_ACCOUNT_TO_POSITION_MAPPING ( :1,:2,:3,:4,:5,:6)',
 NULL,NULL,NULL,NULL,NULL,NULL,'Y',1,'Sigma Rule 1');
 
insert into CSTA_JOINCONDITION (tenantid,ruleseq,sourcetablename,sourcecolumnname,operator,targettablename,
   targetcolumnname,overrideexpression,effectivestartdate,effectiveenddate,removedate,createdate,
   islast) values
('1259',50665495807930581,'CS_SALESTRANSACTION','COMPENSATIONDATE','>=',
          'CSTA_GENERICCLASSIFIER','EFFECTIVESTARTDATE',NULL,to_date('19000101','YYYYMMDD')
                                                               ,to_date('22000101','YYYYMMDD')
                                                               ,to_date('22000101','YYYYMMDD'),current_date,1);
          
insert into CSTA_JOINCONDITION (tenantid,ruleseq,sourcetablename,sourcecolumnname,operator,targettablename,
   targetcolumnname,overrideexpression,effectivestartdate,effectiveenddate,removedate,createdate,
   islast) values
('1259',50665495807930581,'CS_SALESTRANSACTION','COMPENSATIONDATE','<',
          'CSTA_GENERICCLASSIFIER','EFFECTIVEENDDATE',NULL,to_date('19000101','YYYYMMDD')
                                                               ,to_date('22000101','YYYYMMDD')
                                                               ,to_date('22000101','YYYYMMDD'),current_date,1);
 
insert into CSTA_JOINCONDITION (tenantid,ruleseq,sourcetablename,sourcecolumnname,operator,targettablename,
   targetcolumnname,overrideexpression,effectivestartdate,effectiveenddate,removedate,createdate,
   islast) values
('1259',50665495807930581,'CS_SALESTRANSACTION','GenericAttribute32','=',
          'CSTA_GENERICCLASSIFIER','classifierid',NULL,to_date('19000101','YYYYMMDD')
                                                               ,to_date('22000101','YYYYMMDD')
                                                               ,to_date('22000101','YYYYMMDD'),current_date,1);          
--
insert into CSTA_EXPRESSION (tenantid,ruleseq,sourcetablename,overrideexpression,effectivestartdate,effectiveenddate,removedate,createdate)
values
('1259',50665495807930581,'CSTA_GENERICCLASSIFIER','CSTA_GENERICCLASSIFIER.REMOVEDATE=to_date(''22000101'',''YYYYMMDD'')'
,'19000101','22000101','22000101',current_date);
 
--
 select * from CSTA_TARGETFIELD
-- Position Name mapping into POSITIONNAME in TSTA –understand this has something to do with the assignment, not sure how this translates to populating assignment with the positionnames
insert into CSTA_TARGETFIELD (tenantid,ruleseq,sourcetablename,SOURCECOUMNNAME    ,recordtype,effectivestartdate,effectiveenddate,
removedate,createdate)
   values
('1259',50665495807930581,'CSTA_GENERICCLASSIFIER','GENERICATTRIBUTE1','POSITIONNAME',to_date('19000101','YYYYMMDD')
                                                               ,to_date('22000101','YYYYMMDD')
                                                               ,to_date('22000101','YYYYMMDD'),current_date);
 
-- Split mapping into GA1 in TSTA 
insert into CSTA_COPYFIELD (tenantid,ruleseq,sourcetablename,SOURCECOLUMNNAME,TARGETCOLUMNNAME,effectivestartdate,effectiveenddate,
removedate,createdate)
   values
('1259',50665495807930581,'CSTA_GENERICCLASSIFIER','GENERICATTRIBUTE2','GENERICATTRIBUTE1',to_date('19000101','YYYYMMDD')
                                                               ,to_date('22000101','YYYYMMDD')
                                                               ,to_date('22000101','YYYYMMDD'),current_date);
--
-- Alignment type mapping into GA2 in TSTA 
insert into CSTA_COPYFIELD (tenantid,ruleseq,sourcetablename,SOURCECOLUMNNAME,TARGETCOLUMNNAME,effectivestartdate,effectiveenddate,
removedate,createdate)
   values
('1259',50665495807930581,'CSTA_GENERICCLASSIFIER','GENERICATTRIBUTE3','GENERICATTRIBUTE2',to_date('19000101','YYYYMMDD')
                                                               ,to_date('22000101','YYYYMMDD')
                                                               ,to_date('22000101','YYYYMMDD'),current_date);


commit;