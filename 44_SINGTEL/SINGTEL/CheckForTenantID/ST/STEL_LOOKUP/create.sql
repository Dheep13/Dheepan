CREATE VIEW "EXT"."STEL_LOOKUP" ( "NAME", "MDLTCELLSEQ", "EFFECTIVESTARTDATE", "EFFECTIVEENDDATE", "VALUE", "STRINGVALUE", "DATEVALUE", "DIM0", "DIM1", "DIM2", "DIM3", "DIM4", "DIM5", "DIM6", "DIM7", "DIM8", "DIM9", "DIM10" ) AS (SELECT m.NAME,
          a.mdltcellseq,
          a.effectivestartdate,
          a.effectiveenddate,
          a.VALUE,
          a.stringvalue,
          a.datevalue,
          REPLACE (
             REPLACE (
                TRIM(   IFNULL (TO_CHAR (d0.minvalue), IFNULL (d0.minstring, ''))
                     || ' - '
                     || IFNULL (TO_CHAR (d0.maxvalue), IFNULL (d0.maxstring, ''))),
                ' - ',
                '-'),
             ' -',
             '')
             AS Dim0,
          REPLACE (
             REPLACE (
                TRIM(   IFNULL (TO_CHAR (d1.minvalue), IFNULL (d1.minstring, ''))
                     || ' - '
                     || IFNULL (TO_CHAR (d1.maxvalue), IFNULL (d1.maxstring, ''))),
                ' - ',
                '-'),
             ' -',
             '')
             AS Dim1,
          REPLACE (
             REPLACE (
                TRIM(   IFNULL (TO_CHAR (d2.minvalue), IFNULL (d2.minstring, ''))
                     || ' - '
                     || IFNULL (TO_CHAR (d2.maxvalue), IFNULL (d2.maxstring, ''))),
                ' - ',
                '-'),
             ' -',
             '')
             AS Dim2,
          REPLACE (
             REPLACE (
                TRIM(   IFNULL (TO_CHAR (d3.minvalue), IFNULL (d3.minstring, ''))
                     || ' - '
                     || IFNULL (TO_CHAR (d3.maxvalue), IFNULL (d3.maxstring, ''))),
                ' - ',
                '-'),
             ' -',
             '')
             AS Dim3,
          REPLACE (
             REPLACE (
                TRIM(   IFNULL (TO_CHAR (d4.minvalue), IFNULL (d4.minstring, ''))
                     || ' - '
                     || IFNULL (TO_CHAR (d4.maxvalue), IFNULL (d4.maxstring, ''))),
                ' - ',
                '-'),
             ' -',
             '')
             AS Dim4,
          REPLACE (
             REPLACE (
                TRIM(   IFNULL (TO_CHAR (d5.minvalue), IFNULL (d5.minstring, ''))
                     || ' - '
                     || IFNULL (TO_CHAR (d5.maxvalue), IFNULL (d5.maxstring, ''))),
                ' - ',
                '-'),
             ' -',
             '')
             AS Dim5,
          REPLACE (
             REPLACE (
                TRIM(   IFNULL (TO_CHAR (d6.minvalue), IFNULL (d6.minstring, ''))
                     || ' - '
                     || IFNULL (TO_CHAR (d6.maxvalue), IFNULL (d6.maxstring, ''))),
                ' - ',
                '-'),
             ' -',
             '')
             AS Dim6,
          REPLACE (
             REPLACE (
                TRIM(   IFNULL (TO_CHAR (d7.minvalue), IFNULL (d7.minstring, ''))
                     || ' - '
                     || IFNULL (TO_CHAR (d7.maxvalue), IFNULL (d7.maxstring, ''))),
                ' - ',
                '-'),
             ' -',
             '')
             AS Dim7,
          REPLACE (
             REPLACE (
                TRIM(   IFNULL (TO_CHAR (d8.minvalue), IFNULL (d8.minstring, ''))
                     || ' - '
                     || IFNULL (TO_CHAR (d8.maxvalue), IFNULL (d8.maxstring, ''))),
                ' - ',
                '-'),
             ' -',
             '')
             AS Dim8,
          REPLACE (
             REPLACE (
                TRIM(   IFNULL (TO_CHAR (d9.minvalue), IFNULL (d9.minstring, ''))
                     || ' - '
                     || IFNULL (TO_CHAR (d9.maxvalue), IFNULL (d9.maxstring, ''))),
                ' - ',
                '-'),
             ' -',
             '')
             AS Dim9,
          REPLACE (
             REPLACE (
                TRIM(   IFNULL (TO_CHAR (d10.minvalue), IFNULL (d10.minstring, ''))
                     || ' - '
                     || IFNULL (TO_CHAR (d10.maxvalue), IFNULL (d10.maxstring, ''))),
                ' - ',
                '-'),
             ' -',
             '')
             AS Dim10
     FROM cs_relationalmdlt m
          LEFT JOIN CS_MDLTcell a
             ON     m.ruleelementseq = a.mdltseq
                AND a.removedate > CURRENT_DATE
                AND a.modelseq = 0
          LEFT JOIN cs_mdltindex d0
             ON     a.MDLTSEQ = d0.ruleelementseq
                AND d0.dimensionseq = 1
                AND a.dim0index = d0.ordinal
                AND d0.removedate > CURRENT_DATE
                AND d0.modelseq = 0
          LEFT JOIN cs_mdltindex d1
             ON     a.MDLTSEQ = d1.ruleelementseq
                AND d1.dimensionseq = 2
                AND a.dim1index = d1.ordinal
                AND d1.removedate > CURRENT_DATE
                AND d1.modelseq = 0
          LEFT JOIN cs_mdltindex d2
             ON     a.MDLTSEQ = d2.ruleelementseq
                AND d2.dimensionseq = 3
                AND a.dim2index = d2.ordinal
                AND d2.removedate > CURRENT_DATE
                AND d2.modelseq = 0
          LEFT JOIN cs_mdltindex d3
             ON     a.MDLTSEQ = d3.ruleelementseq
                AND d3.dimensionseq = 4
                AND a.dim3index = d3.ordinal
                AND d3.removedate > CURRENT_DATE
                AND d3.modelseq = 0
          LEFT JOIN cs_mdltindex d4
             ON     a.MDLTSEQ = d4.ruleelementseq
                AND d4.dimensionseq = 5
                AND a.dim4index = d4.ordinal
                AND d4.removedate > CURRENT_DATE
                AND d4.modelseq = 0
          LEFT JOIN cs_mdltindex d5
             ON     a.MDLTSEQ = d5.ruleelementseq
                AND d5.dimensionseq = 6
                AND a.dim5index = d5.ordinal
                AND d5.removedate > CURRENT_DATE
                AND d5.modelseq = 0
          LEFT JOIN cs_mdltindex d6
             ON     a.MDLTSEQ = d6.ruleelementseq
                AND d6.dimensionseq = 7
                AND a.dim6index = d6.ordinal
                AND d6.removedate > CURRENT_DATE
                AND d6.modelseq = 0
          LEFT JOIN cs_mdltindex d7
             ON     a.MDLTSEQ = d7.ruleelementseq
                AND d7.dimensionseq = 8
                AND a.dim7index = d7.ordinal
                AND d7.removedate > CURRENT_DATE
                AND d7.modelseq = 0
          LEFT JOIN cs_mdltindex d8
             ON     a.MDLTSEQ = d8.ruleelementseq
                AND d8.dimensionseq = 9
                AND a.dim8index = d8.ordinal
                AND d8.removedate > CURRENT_DATE
                AND d8.modelseq = 0
          LEFT JOIN cs_mdltindex d9
             ON     a.MDLTSEQ = d9.ruleelementseq
                AND d9.dimensionseq = 10
                AND a.dim9index = d9.ordinal
                AND d9.removedate > CURRENT_DATE
                AND d9.modelseq = 0
          LEFT JOIN cs_mdltindex d10
             ON     a.MDLTSEQ = d10.ruleelementseq
                AND d10.dimensionseq = 11
                AND a.dim6index = d10.ordinal
                AND d10.removedate > CURRENT_DATE
                AND d10.modelseq = 0
    WHERE m.REMOVEDATE > CURRENT_DATE AND m.modelseq = 0) WITH READ ONLY