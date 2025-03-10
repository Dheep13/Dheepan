DROP TABLE EXT.TRYG_DEBUG_LOG ;
CREATE COLUMN TABLE EXT.TRYG_DEBUG_LOG (
    DATETIME LONGDATE CS_LONGDATE DEFAULT CURRENT_TIMESTAMP,
    PROCNAME VARCHAR(4000),
    COMMENTS VARCHAR(4000),
    VALUE DECIMAL(25, 10) CS_FIXED
) UNLOAD PRIORITY 5 AUTO MERGE