
CREATE PROCEDURE ext.STEL_LOG (p_tenantid  VARCHAR(4),
                                       p_componentname  VARCHAR(255),
                                       p_message VARCHAR2(4000))
as
BEGIN
   INSERT INTO STEL_MESSAGELOG (tenantid,
                                 logdate,
                                 componentname,
                                 MESSAGE)
       VALUES (p_tenantid,
               current_date,
               p_componentname,
               p_message);

   COMMIT;
END;