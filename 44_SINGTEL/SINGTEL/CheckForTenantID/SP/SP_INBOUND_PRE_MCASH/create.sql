CREATE PROCEDURE EXT.SP_INBOUND_PRE_MCASH
()
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
DEFAULT SCHEMA EXT
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE v_Sql VARCHAR(20000);  /* ORIGSQL: v_Sql VARCHAR2(20000); */
    DECLARE v_result varchar(1000);
    DECLARE v_field2 nvarchar(1000);
    DECLARE v_inbound_cfg_parameter ROW LIKE INBOUND_CFG_PARAMETER;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'DBMTK_USER_NAME.INBOUND_CFG_PARAMETER' not found (for %ROWTYPE declaration) */
    


    SELECT *
    INTO
        v_inbound_cfg_parameter
    FROM
        INBOUND_CFG_PARAMETER;

	 MERGE INTO ext.inbound_Data_staging ids
		USING (
		    SELECT 
		        sp.PAYEEID,
		        ids.field2,
		        ids.filename,
		        ids.filedate
		    FROM 
		        ext.inbound_Data_staging ids
		    INNER JOIN 
		        EXT.STEL_PARTICIPANT sp 
		    ON 
		        TRIM(sp.GENERICATTRIBUTE3) = TRIM(ids.field2)
		    AND current_timestamp BETWEEN sp.effectivestartdate AND sp.effectiveenddate
		    WHERE ids.filename = :v_inbound_cfg_parameter.file_name
		    AND ids.filedate = :v_inbound_cfg_parameter.file_Date
		    ) source
			ON (ids.filename = source.filename 
			AND ids.filedate = source.filedate 
			AND ids.field2 = source.field2)
	WHEN MATCHED THEN
	UPDATE SET
	    ids.field1 = TRIM(ids.field1),
	    ids.field2 = TRIM(ids.field2),
	    ids.field3 = TRIM(ids.field3),
	    ids.field4 = TRIM(ids.field4),
	    ids.field5 = TRIM(ids.field5),
	    ids.field6 = TRIM(ids.field6),
	    ids.field7 = TRIM(ids.field7),
	    ids.field8 = TRIM(ids.field8),
	    ids.field9 = TRIM(ids.field9),
	    ids.field10 = TRIM(ids.field10),
	    ids.field11 = TRIM(ids.field11),
	    ids.field12 = TRIM(ids.field12),
	    ids.field13 = TRIM(ids.field13),
	    ids.field14 = TRIM(ids.field14),
	    ids.field100 = source.PAYEEID;

COMMIT;

   /*Deepan : The idea behind the below approach was to reatain the use of the EXT.FN_VALUELOOKUP function, but considering
   that the performance might be an issue commenting it out*/
   
  /*   DECLARE CURSOR cur for
    SELECT field2
    FROM ext.inbound_Data_staging
    WHERE filename = :v_inbound_cfg_parameter.file_name
    AND filedate = :v_inbound_cfg_parameter.file_date;
    
  FOR cur_row AS cur
   DO
        -- Call the function to get the dynamic SQL query
        v_sql = EXT.FN_VALUELOOKUP(
            :cur_row.field2,
            'EXT.STEL_PARTICIPANT',
            'current_timestamp between effectivestartdate and effectiveenddate',
            'GENERICATTRIBUTE3',
            'PAYEEID'
        );
   
    
     EXECUTE IMMEDIATE :v_sql INTO v_result; 
    -- Deepan : Since FN_VALUELOOKUP is a procedure in HANA , made changes to accomodate proc instead of a function*
    UPDATE ext.inbound_Data_staging
        SET
      
        field1 = TRIM(field1),
     
        field2 = TRIM(field2),
     
        field3 = TRIM(field3),
      
        field4 = TRIM(field4),
     
        field5 = TRIM(field5),
        
        field6 = TRIM(field6),
      
        field7 = TRIM(field7),
      
        field8 = TRIM(field8),
 
        field9 = TRIM(field9),
    
        field10 = TRIM(field10),
 
        field11 = TRIM(field11),
      
        field12 = TRIM(field12),
     
        field13 = TRIM(field13),
    
        field14 = TRIM(field14),
   
        field100 = :v_result
    WHERE
        field2 = :cur_row.field2
        AND filename = :v_inbound_cfg_parameter.file_name
        AND filedate = :v_inbound_cfg_parameter.file_Date;
        
  END FOR;
 */
 
 
END