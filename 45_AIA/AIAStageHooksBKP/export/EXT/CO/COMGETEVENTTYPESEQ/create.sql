CREATE Function Comgeteventtypeseq(in I_Eventtypeid Varchar(255))  
Returns v_eventtypeseq bigInt 
sql security definer 
 as 
 Begin
  declare v_eventtypeseq bigint;
  declare cdt_EndofTime date := to_date('2200-01-01','yyyy-mm-dd');
  declare exit handler for sqlexception 
  begin 
  v_eventtypeseq=0;	
  end;
 
    Select datatypeseq
      Into v_eventtypeseq
      From Cs_Eventtype
     Where Eventtypeid = I_Eventtypeid
       and removedate = cdt_EndofTime;

end