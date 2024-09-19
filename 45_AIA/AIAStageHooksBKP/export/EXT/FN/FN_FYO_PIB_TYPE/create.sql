CREATE FUNCTION EXT.FN_FYO_PIB_TYPE (
    GA13_Leader_Title NVARCHAR(255), 
    GA14_Contributor_Class_Code NVARCHAR(255),
    Credit_Output_Name NVARCHAR(255)
) RETURNS PIB_TYPE NVARCHAR(20)
AS
BEGIN
    DECLARE PIB_TYPE NVARCHAR(20);

    IF :GA14_Contributor_Class_Code = '12' THEN 
        PIB_TYPE := 'ASSIGNED'; -- ASSIGNED
    ELSEIF :GA14_Contributor_Class_Code = '10' AND :GA13_Leader_Title IN ('FSM', 'AM', 'FSAD') AND UPPER(:Credit_Output_Name) LIKE '%INDIRECT%' THEN   
        PIB_TYPE := 'INDIRECT';
    ELSEIF :GA14_Contributor_Class_Code = '10' AND :GA13_Leader_Title IN ('FSM', 'AM', 'FSAD') AND UPPER(:Credit_Output_Name) LIKE '%DIRECT%' THEN   
        PIB_TYPE := 'DIRECT';
    ELSEIF :GA14_Contributor_Class_Code = '10' AND :GA13_Leader_Title IN ('FSD') AND INSTR(UPPER(:Credit_Output_Name), '_DIRECT_') > 0 THEN   
        PIB_TYPE := 'DIRECT'; -- DIRECT
    ELSEIF :GA14_Contributor_Class_Code = '10' AND :GA13_Leader_Title IN ('FSD') AND INSTR(UPPER(:Credit_Output_Name), '_INDIRECT_') > 0 THEN   
        PIB_TYPE := 'INDIRECT';
    ELSEIF :GA14_Contributor_Class_Code <> '12' AND :GA14_Contributor_Class_Code <> '10' THEN   
        PIB_TYPE := 'PERSONAL'; -- PERSONAL
    END IF;

END