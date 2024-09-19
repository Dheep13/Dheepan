CREATE function comGetQuarterMonth (IN i_periodSeq BIGINT)     
RETURNS dbmtk_function_result BIGINT   /* ORIGSQL: return int */
LANGUAGE SQLSCRIPT
SQL SECURITY DEFINER
AS
BEGIN
    DECLARE gv_error VARCHAR(1000); /* package/session variable */
    DECLARE cdt_EndOfTime date = to_date('2200-01-01','yyyy-mm-dd');
    DECLARE gv_calendarSeq BIGINT; /* package/session variable */
    DECLARE gv_prePeriodSeq1 BIGINT; /* package/session variable */
    DECLARE gv_prePeriodSeq2 BIGINT; /* package/session variable */

    /* sapdbmtk: one or more DECLARE statements below were moved upwards, before the first executable statement */
    DECLARE NOT_QUARTER_END CONDITION;  /* ORIGSQL: NOT_QUARTER_END EXCEPTION; */
    DECLARE v_periodTypeSeq BIGINT = 0;  /* ORIGSQL: v_periodTypeSeq int := 0; */

    DECLARE EXIT HANDLER FOR NOT_QUARTER_END
        /* ORIGSQL: WHEN NOT_QUARTER_END then */
        BEGIN
            gv_error = 'Info [SP_UPDATE_DO_QUARTERLY]: The stage hook will be skip in current period.';
            /* Saving modified package/session variable 'gv_error': */ 
            SET SESSION 'GV_ERROR' = :gv_error;   
            dbmtk_function_result = - 1;
            /* sapdbmtk: Closing RETURN in exception handler commented out, not supported in HANA */
            --RETURN;
        END;

    BEGIN 
        DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299 /*1299=ERR_SQLSCRIPT_NO_DATA_FOUND*/
            /* ORIGSQL: when no_data_found then */
            BEGIN 
                dbmtk_function_result = - 1;
                /* RESOLVE: Syntax not supported in target DBMS: RETURN in exception handler not supported in HANA, rewrite manually */
--                return ;
            END;

SELECT CAST(SESSION_CONTEXT('GV_CALENDARSEQ') AS BIGINT) INTO gv_calendarSeq FROM SYS.DUMMY ;
                     

        SELECT
            periodTypeSeq
        INTO
            v_periodTypeSeq
        FROM
            cs_period
        WHERE periodSeq = :i_periodSeq
            -- 20171213 COSIMO Project begin
            --and shortName in ('Feb', 'May', 'Aug', 'Nov')
            AND shortName IN ('Mar', 'Jun', 'Sep', 'Dec')
            -- 20171213 COSIMO Project end
            AND Removedate = :cdt_EndOfTime
            AND calendarseq = :gv_calendarSeq;

        /* ORIGSQL: exception when no_data_found then */
    END;

    IF :v_periodTypeSeq = NULL
    THEN
        /* ORIGSQL: raise NOT_QUARTER_END; */
        SIGNAL NOT_QUARTER_END;
    END IF;

    --version 7
    --select max(periodSeq)
    --  into gv_prePeriodSeq1
    --  from cs_period
    -- where tenantid='AIAS' and periodSeq < i_periodSeq
    -- and periodTypeSeq = v_periodTypeSeq
    -- And Removedate = Cdt_Endoftime
    -- AND CALENDARSEQ = GV_CALENDARSEQ;
    --
    --select max(periodSeq)
    --  into gv_prePeriodSeq2
    --  from cs_period
    -- where tenantid='AIAS' and periodSeq < gv_prePeriodSeq1
    -- and periodTypeSeq = v_periodTypeSeq
    -- and removeDate = cdt_EndOfTime
    -- AND CALENDARSEQ = GV_CALENDARSEQ; 

    SELECT
        MAX(a.periodseq) 
    INTO
        gv_prePeriodSeq1
    FROM
        cs_period a
        ,
        (
            SELECT   /* ORIGSQL: (select startdate,enddate from cs_period where periodseq = i_periodSeq and removedate = Cdt_Endoftime) */
                startdate,
                enddate
            FROM
                cs_period
            WHERE
                periodseq = :i_periodSeq
                AND removedate = :cdt_EndOfTime
        ) AS b
    WHERE a.periodTypeSeq = :v_periodTypeSeq
        AND a.Removedate = :cdt_EndOfTime
        AND a.CALENDARSEQ = :gv_calendarSeq
        AND a.startdate = ADD_MONTHS(b.startdate,-1)
        AND a.enddate = ADD_MONTHS(b.enddate,-1);

    SELECT
        MAX(a.periodseq) 
    INTO
        gv_prePeriodSeq2
    FROM
        cs_period a
        ,
        (
            SELECT   /* ORIGSQL: (select startdate,enddate from cs_period where periodseq = i_periodSeq and removedate = Cdt_Endoftime) */
                startdate,
                enddate
            FROM
                cs_period
            WHERE
                periodseq = :i_periodSeq
                AND removedate = :cdt_EndOfTime
        ) AS b
    WHERE a.periodTypeSeq = :v_periodTypeSeq
        AND a.Removedate = :cdt_EndOfTime
        AND a.CALENDARSEQ = :gv_calendarSeq
        AND a.startdate = ADD_MONTHS(b.startdate,-2)
        AND a.enddate = ADD_MONTHS(b.enddate,-2);

    dbmtk_function_result = 1;
   
 SET SESSION 'GV_PREPERIODSEQ1' = CAST(:gv_prePeriodSeq1 AS VARCHAR(512));
 SET SESSION 'GV_PREPERIODSEQ2' = CAST(:gv_prePeriodSeq2 AS VARCHAR(512));  
   
--    RETURN;

    
END;