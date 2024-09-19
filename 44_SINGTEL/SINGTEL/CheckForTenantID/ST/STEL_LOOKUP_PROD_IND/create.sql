CREATE VIEW "EXT"."STEL_LOOKUP_PROD_IND" ( "NAME", "MDLTCELLSEQ", "EFFECTIVESTARTDATE", "EFFECTIVEENDDATE", "VALUE", "STRINGVALUE", "DATEVALUE", "DIM0", "DIM1", "DIM2", "DIM3", "DIM4", "DIM5", "DIM6", "DIM7", "DIM8", "DIM9", "DIM10" ) AS (SELECT distinct  m.NAME,
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
'-' as Dim4,    
'-' as Dim5,
'-' as Dim6,
'-' as Dim7,
'-' as Dim8,
'-' as Dim9, 
'-' as Dim10 FROM cs_relationalmdlt m
          LEFT JOIN CS_MDLTcell a
             ON     m.ruleelementseq = a.mdltseq
                AND a.REMOVEDATE < to_date('10-JAN-2019', 'DD-MON-YYYY') and a.CREATEDATE > to_date('17-DEC-2018', 'DD-MON-YYYY')
                AND a.modelseq = 0
          LEFT JOIN cs_mdltindex d0
             ON     a.MDLTSEQ = d0.ruleelementseq
                AND d0.dimensionseq = 1
                AND a.dim0index = d0.ordinal
                AND d0.REMOVEDATE < to_date('10-JAN-2019', 'DD-MON-YYYY') and d0.CREATEDATE > to_date('17-DEC-2018', 'DD-MON-YYYY')
                AND d0.modelseq = 0
          LEFT JOIN cs_mdltindex d1
             ON     a.MDLTSEQ = d1.ruleelementseq
                AND d1.dimensionseq = 2
                AND a.dim1index = d1.ordinal
                AND d1.REMOVEDATE < to_date('10-JAN-2019', 'DD-MON-YYYY') and d1.CREATEDATE > to_date('17-DEC-2018', 'DD-MON-YYYY')
                AND d1.modelseq = 0
          LEFT JOIN cs_mdltindex d2
             ON     a.MDLTSEQ = d2.ruleelementseq
                AND d2.dimensionseq = 3
                AND a.dim2index = d2.ordinal
                AND d2.REMOVEDATE < to_date('10-JAN-2019', 'DD-MON-YYYY') and d2.CREATEDATE > to_date('17-DEC-2018', 'DD-MON-YYYY')
                AND d2.modelseq = 0
          LEFT JOIN cs_mdltindex d3
             ON     a.MDLTSEQ = d3.ruleelementseq
                AND d3.dimensionseq = 4
                AND a.dim3index = d3.ordinal
                AND d3.REMOVEDATE < to_date('10-JAN-2019', 'DD-MON-YYYY') and d3.CREATEDATE > to_date('17-DEC-2018', 'DD-MON-YYYY')
                AND d3.modelseq = 0
    WHERE m.REMOVEDATE < to_date('10-JAN-2019', 'DD-MON-YYYY') and m.CREATEDATE > to_date('17-DEC-2018', 'DD-MON-YYYY') AND m.modelseq = 0
    and m.name='LT_CSI_Internal_Product_Target_Ind') WITH READ ONLY