CREATE VIEW "EXT"."STEL_NONWORKINGDAYS" ( "CHANNEL", "NONWORKDATE" ) AS (SELECT   /* ORIGSQL: select genericattribute1 channel, genericdate1 nonworkdate from stel_Classifier (...) */
        genericattribute1 AS channel,
        genericdate1 AS nonworkdate
    FROM
       ext. stel_Classifier
    WHERE
        categorytreename = 'Landing Pad Config'
        AND categoryname = 'Non Working Days')