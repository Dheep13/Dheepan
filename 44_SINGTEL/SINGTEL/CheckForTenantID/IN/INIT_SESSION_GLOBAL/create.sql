CREATE PROCEDURE EXT.init_session_global()
/*READS SQL DATA*/ -- this procedure cannot be read-only
AS
BEGIN
    DECLARE init_timestamp VARCHAR(50) := TO_VARCHAR(CURRENT_TIMESTAMP);
    DECLARE vPeriodRow ROW LIKE CS_PERIOD ;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.CS_PERIOD' not found (for %ROWTYPE declaration) */
    DECLARE vCalendarRow ROW LIKE CS_CALENDAR;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.CS_CALENDAR' not found (for %ROWTYPE declaration) */
    DECLARE vProcessingUnitRow ROW LIKE CS_PROCESSINGUNIT;/* NOT CONVERTED! */  /* RESOLVE: Identifier not found: Table 'EXT.CS_PROCESSINGUNIT' not found (for %ROWTYPE declaration) */
    DECLARE cEndofTime CONSTANT TIMESTAMP = TO_DATE('01012200','mmddyyyy');   
    
    SET SESSION 'vProcName' = NULL;
    SET SESSION 'vSQLerrm' = NULL;
    SET SESSION 'vCurYrStartDate' = NULL;
    SET SESSION 'vCurYrEndDate' = NULL;
    SET SESSION 'vPeriodtype' = 'MONTHLY';
    SET SESSION 'vSTSRoadShowCategory' = 'Roadshow';
    SET SESSION 'veventtypeid_ccomobile' = 'Mobile Closed';
    SET SESSION 'veventtypeid_ccotv' = 'TV Closed';
    SET SESSION 'vcredittypeid_PayAdj' = 'Payment Adjustment';
    SET SESSION 'vcredittypeid_CEAdj' = 'Customer Experience';
    SET SESSION 'vcredittypeid_Mobile' = 'CCO Mobile VAS';
    SET SESSION 'vcredittypeid_TV' = 'CCO TV';
    SET SESSION 'vcredittypeid_HandFee' = 'TVReconHandlingFee - CCO';
    SET SESSION 'vOperational_Compliance' = 'Operational Compliance';
    SET SESSION 'vGSTRate' = 0.07;
    SET SESSION 'cEndofTime' = :cEndofTime;
    END