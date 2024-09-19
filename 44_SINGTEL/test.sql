UPDATE
    EXT.INBOUND_DATA_STAGING
SET
    FIELD151 = (
        SELECT
            MAX(SCENARIO)
        FROM
            EXT.INBOUND_CFG_BCCSCNEARIOS
        WHERE
            ORDER_TYPE = IFNULL('FIELD3', '*')
            AND TRANSACTION_TYPE = IFNULL('FIELD4', '*')
            AND (
                SUBTRANSACTION_TYPE = '*'
                OR SUBTRANSACTION_TYPE = IFNULL('FIELD5', '*')
            )
            AND (
                RECON_FLAG = '*'
                OR RECON_FLAG = IFNULL('FIELD6', '*')
            )
            AND (
                SERVICE_TYPE = '*'
                OR SERVICE_TYPE = IFNULL('null', '*')
            )
            AND (
                COMPONENT_STATUS = '*'
                OR COMPONENT_STATUS = IFNULL('FIELD7', '*')
            )
            AND (
                ORDER_LINE_TYPE = '*'
                OR ORDER_LINE_TYPE = IFNULL('FIELD8', '*')
            )
            AND (
                GENERICFIELD1 = '*'
                OR GENERICFIELD1 = IFNULL('NULL', '*')
            )
            AND (
                GENERICFIELD2 = '*'
                OR GENERICFIELD2 = IFNULL('NULL', '*')
            )
            AND (
                GENERICFIELD3 = '*'
                OR GENERICFIELD3 = IFNULL('NULL', '*')
            )
            AND (
                GENERICFIELD4 = '*'
                OR GENERICFIELD4 = IFNULL('NULL', '*')
            )
            AND (
                GENERICFIELD5 = '*'
                OR GENERICFIELD5 = IFNULL('NULL', '*')
            )
            AND FILE_TYPE = 'BCCSCIISubmittedBroadBandOrders'
    )
WHERE
    (FILETYPE, FILENAME, FILEDATE) IN (
        SELECT
            'BCCSCIISubmittedBroadBandOrders',
            'BCCSCIISubmittedBroadBandOrders_20240228062910.txt',
            '2024-07-26 17:50:56.9160000'
        FROM
            DUMMY
    )