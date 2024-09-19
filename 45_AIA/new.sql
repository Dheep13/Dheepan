PUBLIC PROCEDURE Comtransferpiaor_debug
(
    IN I_R_Agydisttrxn_FIELD_Salestransactionseq BIGINT,     /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_SALESORDERSEQ BIGINT,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Wagency VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_wAgencyLeader VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Wagyldrtitle VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_LdrCurRole VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Wagyldrdistrict VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_CurDistrict VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Policyissuedate TIMESTAMP,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Compensationdate TIMESTAMP,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Wagtclass VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Commissionagy VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Runningtype VARCHAR(100),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Eventtypeid VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Productname VARCHAR(100),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Businessunitmap VARCHAR(100),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Orphanpolicy VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Managerseq BIGINT,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Agyspinoffindicator VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Agyspinoffflag BIGINT,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Versioningdate TIMESTAMP,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Periodseq BIGINT,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Spinstartdate TIMESTAMP,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Spindaterange DECIMAL(38,10),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Txnclasscode VARCHAR(10),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_Spinenddate TIMESTAMP,   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_actualOrphanPolicy VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_wAgyLdrCde VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_setup VARCHAR(30),   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
    IN I_R_Agydisttrxn_FIELD_txnCode VARCHAR(30)   /* ORIGSQL: I_R_Agydisttrxn In R_Agydisttrxn */
)


CALL Comtransferpiaor_debug(
                        :v_AgyDistTrxn_FIELD_Salestransactionseq,
                        :v_AgyDistTrxn_FIELD_SALESORDERSEQ,
                        :v_AgyDistTrxn_FIELD_Wagency,
                        :v_AgyDistTrxn_FIELD_wAgencyLeader,
                        :v_AgyDistTrxn_FIELD_Wagyldrtitle,
                        :v_AgyDistTrxn_FIELD_LdrCurRole,
                        :v_AgyDistTrxn_FIELD_Wagyldrdistrict,
                        :v_AgyDistTrxn_FIELD_CurDistrict,
                        :v_AgyDistTrxn_FIELD_Policyissuedate,
                        :v_AgyDistTrxn_FIELD_Compensationdate,
                        :v_AgyDistTrxn_FIELD_Wagtclass,
                        :v_AgyDistTrxn_FIELD_Commissionagy,
                        :v_AgyDistTrxn_FIELD_Runningtype,
                        :v_AgyDistTrxn_FIELD_Eventtypeid,
                        :v_AgyDistTrxn_FIELD_Productname,
                        :v_AgyDistTrxn_FIELD_Businessunitmap,
                        :v_AgyDistTrxn_FIELD_Orphanpolicy,
                        :v_AgyDistTrxn_FIELD_Managerseq,
                        :v_AgyDistTrxn_FIELD_Agyspinoffindicator,
                        :v_AgyDistTrxn_FIELD_Agyspinoffflag,
                        :v_AgyDistTrxn_FIELD_Versioningdate,
                        :v_AgyDistTrxn_FIELD_Periodseq,
                        :v_AgyDistTrxn_FIELD_Spinstartdate,
                        :v_AgyDistTrxn_FIELD_Spindaterange,
                        :v_AgyDistTrxn_FIELD_Txnclasscode,
                        :v_AgyDistTrxn_FIELD_Spinenddate,
                        :v_AgyDistTrxn_FIELD_actualOrphanPolicy,
                        :v_AgyDistTrxn_FIELD_wAgyLdrCde,
                        :v_AgyDistTrxn_FIELD_setup,
                        :v_AgyDistTrxn_FIELD_txnCode
                    );