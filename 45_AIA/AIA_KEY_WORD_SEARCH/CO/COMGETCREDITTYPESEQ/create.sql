CREATE Function Comgetcredittypeseq(in I_credittypeid Varchar2(500)) 
returns dbmtk_function_result bigint
sql security definer 
as
Begin

declare v_credittypeseq int;
declare cdt_EndOfTime date := to_date('2200-01-01','yyyy-mm-dd');
 declare exit handler for sqlexception
         begin 
            dbmtk_function_result = 0;
            /* sapdbmtk: closing return in exception handler commented out, not supported in hana */
            --return;
        end;

    Select datatypeseq
      Into V_Credittypeseq
      From Cs_Credittype
     Where credittypeid = I_credittypeid
       and removedate = cdt_EndofTime;

      
    dbmtk_function_result := v_credittypeseq;
  
  end; --Comgetcredittypeseq