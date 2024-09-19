CREATE VIEW "EXT"."VW_LT_FYO_RATE" ( "VALUE", "NAME", "CONTRIBUTOR_LEADER_TITLE", "PIB_TYPE", "RECEIVER_TITLE" ) AS (SELECT   /* ORIGSQL: SELECT cell.VALUE, lookup_table.name, Cont_Leader_title_dimension.Contributor_Leader_title, PIB_TYPE_dimension.PIB_TYPE, Receiver_title_dimension.Receiver_title FROM CS_MDLTCell cell INNER JOIN CS_REL(...) */

        cell.VALUE,

        lookup_table.name,

        Cont_Leader_title_dimension.Contributor_Leader_title,

        PIB_TYPE_dimension.PIB_TYPE,

        Receiver_title_dimension.Receiver_title

    FROM

        CS_MDLTCell cell

    INNER JOIN

        CS_RELATIONALMDLT lookup_table

        ON cell.mdltseq = lookup_table.ruleelementseq

        /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_RELATIONALMDLT' not found */

        /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_MDLTDIMENSION' not found */

        /* RESOLVE: Identifier not found: Table/view 'AIASEXT.CS_MDLTINDEX' not found */

    INNER JOIN

        (

            SELECT   /* ORIGSQL: (SELECT lookup_table.name, dimension.name dimension_name, dimension.dimensionseq, dimension_index.ordinal, (CASE WHEN dimension.dimensiontype = 0 THEN TO_CHAR(dimension_index.minvalue) WHEN dimension.(...) */

            lookup_table.name,

            dimension.name AS dimension_name,

            dimension.dimensionseq,

            dimension_index.ordinal,

            (

                CASE

                    WHEN dimension.dimensiontype = 0

                    THEN TO_VARCHAR(dimension_index.minvalue)  /* ORIGSQL: TO_CHAR(dimension_index.minvalue) */

                    WHEN dimension.dimensiontype = 1

                    THEN TO_VARCHAR(dimension_index.minstring,NULL)  /* ORIGSQL: TO_CHAR(dimension_index.minstring) */

                    WHEN dimension.dimensiontype = 2

                    THEN TO_VARCHAR(dimension_index.mindate,'DD-MON-YY HH12:MI:SS.FF AM')  /* ORIGSQL: TO_CHAR(dimension_index.mindate) */

                END

            ) AS Contributor_Leader_title,

            lookup_table.ruleelementseq

        FROM

            CS_RELATIONALMDLT lookup_table

        INNER JOIN

            CS_MDLTDimension dimension

            ON lookup_table.ruleelementseq =

            dimension.ruleelementseq

        INNER JOIN

            CS_MDLTIndex dimension_index

            ON lookup_table.ruleelementseq =

            dimension_index.ruleelementseq

            AND dimension.dimensionseq =

            dimension_index.dimensionseq

        WHERE

            lookup_table.name = 'LT_FYO_RATE'

            AND lookup_table.removedate =

            TO_DATE('2200/01/01', 'YYYY/MM/DD')  /* ORIGSQL: TO_DATE('2200/01/01', 'YYYY/MM/DD') */

            AND dimension.removedate =

            TO_DATE('2200/01/01', 'YYYY/MM/DD')  /* ORIGSQL: TO_DATE('2200/01/01', 'YYYY/MM/DD') */

            AND dimension_index.removedate =

            TO_DATE('2200/01/01', 'YYYY/MM/DD')  /* ORIGSQL: TO_DATE('2200/01/01', 'YYYY/MM/DD') */

            AND UPPER(dimension.name) =

            UPPER('Contributor''s Leader title')

    ) AS Cont_Leader_title_dimension

    ON cell.mdltseq = Cont_Leader_title_dimension.ruleelementseq

    AND ((Cont_Leader_title_dimension.dimensionseq = 1

        AND Cont_Leader_title_dimension.ordinal = dim0index)

        OR (Cont_Leader_title_dimension.dimensionseq = 2

        AND Cont_Leader_title_dimension.ordinal = dim1index)

        OR (Cont_Leader_title_dimension.dimensionseq = 3

        AND Cont_Leader_title_dimension.ordinal = dim2index)

        OR (Cont_Leader_title_dimension.dimensionseq = 4

        AND Cont_Leader_title_dimension.ordinal = dim3index)

        OR (Cont_Leader_title_dimension.dimensionseq = 5

        AND Cont_Leader_title_dimension.ordinal = dim4index)

        OR (Cont_Leader_title_dimension.dimensionseq = 6

        AND Cont_Leader_title_dimension.ordinal = dim5index)

        OR (Cont_Leader_title_dimension.dimensionseq = 7

        AND Cont_Leader_title_dimension.ordinal = dim6index)

        OR (Cont_Leader_title_dimension.dimensionseq = 8

        AND Cont_Leader_title_dimension.ordinal = dim7index)

        OR (Cont_Leader_title_dimension.dimensionseq = 9

        AND Cont_Leader_title_dimension.ordinal = dim8index)

        OR (Cont_Leader_title_dimension.dimensionseq = 10

        AND Cont_Leader_title_dimension.ordinal = dim9index)

        OR (Cont_Leader_title_dimension.dimensionseq = 11

        AND Cont_Leader_title_dimension.ordinal = dim10index)

        OR (Cont_Leader_title_dimension.dimensionseq = 12

        AND Cont_Leader_title_dimension.ordinal = dim11index)

        OR (Cont_Leader_title_dimension.dimensionseq = 13

        AND Cont_Leader_title_dimension.ordinal = dim12index)

        OR (Cont_Leader_title_dimension.dimensionseq = 14

        AND Cont_Leader_title_dimension.ordinal = dim13index)

        OR (Cont_Leader_title_dimension.dimensionseq = 15

        AND Cont_Leader_title_dimension.ordinal = dim14index)

        OR (Cont_Leader_title_dimension.dimensionseq = 16

    AND Cont_Leader_title_dimension.ordinal = dim15index))

INNER JOIN

    (

        SELECT   /* ORIGSQL: (SELECT lookup_table.name, dimension.name dimension_name, dimension.dimensionseq, dimension_index.ordinal, (CASE WHEN dimension.dimensiontype = 0 THEN TO_CHAR(dimension_index.minvalue) WHEN dimension.(...) */

        lookup_table.name,

        dimension.name AS dimension_name,

        dimension.dimensionseq,

        dimension_index.ordinal,

        (

            CASE

                WHEN dimension.dimensiontype = 0

                THEN TO_VARCHAR(dimension_index.minvalue)  /* ORIGSQL: TO_CHAR(dimension_index.minvalue) */

                WHEN dimension.dimensiontype = 1

                THEN TO_VARCHAR(dimension_index.minstring,NULL)  /* ORIGSQL: TO_CHAR(dimension_index.minstring) */

                WHEN dimension.dimensiontype = 2

                THEN TO_VARCHAR(dimension_index.mindate,'DD-MON-YY HH12:MI:SS.FF AM')  /* ORIGSQL: TO_CHAR(dimension_index.mindate) */

            END

        ) AS PIB_TYPE,

        lookup_table.ruleelementseq

    FROM

        CS_RELATIONALMDLT lookup_table

    INNER JOIN

        CS_MDLTDimension dimension

        ON lookup_table.ruleelementseq =

        dimension.ruleelementseq

    INNER JOIN

        CS_MDLTIndex dimension_index

        ON lookup_table.ruleelementseq =

        dimension_index.ruleelementseq

        AND dimension.dimensionseq =

        dimension_index.dimensionseq

    WHERE

        lookup_table.name = 'LT_FYO_RATE'

        AND lookup_table.removedate =

        TO_DATE('2200/01/01', 'YYYY/MM/DD')  /* ORIGSQL: TO_DATE('2200/01/01', 'YYYY/MM/DD') */

        AND dimension.removedate =

        TO_DATE('2200/01/01', 'YYYY/MM/DD')  /* ORIGSQL: TO_DATE('2200/01/01', 'YYYY/MM/DD') */

        AND dimension_index.removedate =

        TO_DATE('2200/01/01', 'YYYY/MM/DD')  /* ORIGSQL: TO_DATE('2200/01/01', 'YYYY/MM/DD') */

        AND UPPER(dimension.name) = UPPER('PIB TYPE')

    ) AS PIB_TYPE_dimension

    ON cell.mdltseq = PIB_TYPE_dimension.ruleelementseq

    AND ((PIB_TYPE_dimension.dimensionseq = 1

        AND PIB_TYPE_dimension.ordinal = dim0index)

        OR (PIB_TYPE_dimension.dimensionseq = 2

        AND PIB_TYPE_dimension.ordinal = dim1index)

        OR (PIB_TYPE_dimension.dimensionseq = 3

        AND PIB_TYPE_dimension.ordinal = dim2index)

        OR (PIB_TYPE_dimension.dimensionseq = 4

        AND PIB_TYPE_dimension.ordinal = dim3index)

        OR (PIB_TYPE_dimension.dimensionseq = 5

        AND PIB_TYPE_dimension.ordinal = dim4index)

        OR (PIB_TYPE_dimension.dimensionseq = 6

        AND PIB_TYPE_dimension.ordinal = dim5index)

        OR (PIB_TYPE_dimension.dimensionseq = 7

        AND PIB_TYPE_dimension.ordinal = dim6index)

        OR (PIB_TYPE_dimension.dimensionseq = 8

        AND PIB_TYPE_dimension.ordinal = dim7index)

        OR (PIB_TYPE_dimension.dimensionseq = 9

        AND PIB_TYPE_dimension.ordinal = dim8index)

        OR (PIB_TYPE_dimension.dimensionseq = 10

        AND PIB_TYPE_dimension.ordinal = dim9index)

        OR (PIB_TYPE_dimension.dimensionseq = 11

        AND PIB_TYPE_dimension.ordinal = dim10index)

        OR (PIB_TYPE_dimension.dimensionseq = 12

        AND PIB_TYPE_dimension.ordinal = dim11index)

        OR (PIB_TYPE_dimension.dimensionseq = 13

        AND PIB_TYPE_dimension.ordinal = dim12index)

        OR (PIB_TYPE_dimension.dimensionseq = 14

        AND PIB_TYPE_dimension.ordinal = dim13index)

        OR (PIB_TYPE_dimension.dimensionseq = 15

        AND PIB_TYPE_dimension.ordinal = dim14index)

        OR (PIB_TYPE_dimension.dimensionseq = 16

    AND PIB_TYPE_dimension.ordinal = dim15index))

INNER JOIN

    (

        SELECT   /* ORIGSQL: (SELECT lookup_table.name, dimension.name dimension_name, dimension.dimensionseq, dimension_index.ordinal, (CASE WHEN dimension.dimensiontype = 0 THEN TO_CHAR(dimension_index.minvalue) WHEN dimension.(...) */

        lookup_table.name,

        dimension.name AS dimension_name,

        dimension.dimensionseq,

        dimension_index.ordinal,

        (

            CASE

                WHEN dimension.dimensiontype = 0

                THEN TO_VARCHAR(dimension_index.minvalue)  /* ORIGSQL: TO_CHAR(dimension_index.minvalue) */

                WHEN dimension.dimensiontype = 1

                THEN TO_VARCHAR(dimension_index.minstring,NULL)  /* ORIGSQL: TO_CHAR(dimension_index.minstring) */

                WHEN dimension.dimensiontype = 2

                THEN TO_VARCHAR(dimension_index.mindate,'DD-MON-YY HH12:MI:SS.FF AM')  /* ORIGSQL: TO_CHAR(dimension_index.mindate) */

            END

        ) AS Receiver_title,

        lookup_table.ruleelementseq

    FROM

        CS_RELATIONALMDLT lookup_table

    INNER JOIN

        CS_MDLTDimension dimension

        ON lookup_table.ruleelementseq =

        dimension.ruleelementseq

    INNER JOIN

        CS_MDLTIndex dimension_index

        ON lookup_table.ruleelementseq =

        dimension_index.ruleelementseq

        AND dimension.dimensionseq =

        dimension_index.dimensionseq

    WHERE

        lookup_table.name = 'LT_FYO_RATE'

        AND lookup_table.removedate =

        TO_DATE('2200/01/01', 'YYYY/MM/DD')  /* ORIGSQL: TO_DATE('2200/01/01', 'YYYY/MM/DD') */

        AND dimension.removedate =

        TO_DATE('2200/01/01', 'YYYY/MM/DD')  /* ORIGSQL: TO_DATE('2200/01/01', 'YYYY/MM/DD') */

        AND dimension_index.removedate =

        TO_DATE('2200/01/01', 'YYYY/MM/DD')  /* ORIGSQL: TO_DATE('2200/01/01', 'YYYY/MM/DD') */

        AND UPPER(dimension.name) =

        UPPER('Receiver title')

    ) AS Receiver_title_dimension

    ON cell.mdltseq = Receiver_title_dimension.ruleelementseq

    AND ((Receiver_title_dimension.dimensionseq = 1

        AND Receiver_title_dimension.ordinal = dim0index)

        OR (Receiver_title_dimension.dimensionseq = 2

        AND Receiver_title_dimension.ordinal = dim1index)

        OR (Receiver_title_dimension.dimensionseq = 3

        AND Receiver_title_dimension.ordinal = dim2index)

        OR (Receiver_title_dimension.dimensionseq = 4

        AND Receiver_title_dimension.ordinal = dim3index)

        OR (Receiver_title_dimension.dimensionseq = 5

        AND Receiver_title_dimension.ordinal = dim4index)

        OR (Receiver_title_dimension.dimensionseq = 6

        AND Receiver_title_dimension.ordinal = dim5index)

        OR (Receiver_title_dimension.dimensionseq = 7

        AND Receiver_title_dimension.ordinal = dim6index)

        OR (Receiver_title_dimension.dimensionseq = 8

        AND Receiver_title_dimension.ordinal = dim7index)

        OR (Receiver_title_dimension.dimensionseq = 9

        AND Receiver_title_dimension.ordinal = dim8index)

        OR (Receiver_title_dimension.dimensionseq = 10

        AND Receiver_title_dimension.ordinal = dim9index)

        OR (Receiver_title_dimension.dimensionseq = 11

        AND Receiver_title_dimension.ordinal = dim10index)

        OR (Receiver_title_dimension.dimensionseq = 12

        AND Receiver_title_dimension.ordinal = dim11index)

        OR (Receiver_title_dimension.dimensionseq = 13

        AND Receiver_title_dimension.ordinal = dim12index)

        OR (Receiver_title_dimension.dimensionseq = 14

        AND Receiver_title_dimension.ordinal = dim13index)

        OR (Receiver_title_dimension.dimensionseq = 15

        AND Receiver_title_dimension.ordinal = dim14index)

        OR (Receiver_title_dimension.dimensionseq = 16

    AND Receiver_title_dimension.ordinal = dim15index))

WHERE

    lookup_table.name = 'LT_FYO_RATE'

    AND lookup_table.removedate = TO_DATE('2200/01/01', 'YYYY/MM/DD')  /* ORIGSQL: TO_DATE('2200/01/01', 'YYYY/MM/DD') */

    AND cell.removedate = TO_DATE('2200/01/01', 'YYYY/MM/DD')) WITH READ ONLY