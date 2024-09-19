cannot
insert
    NULL
    or
update
    to NULL: "EXT"."INBOUND_TRIGGER": line 445 col 9 (at pos 18393): "EXT"."(DO statement)": line 1 col 10 (at pos 9): "EXT"."SP_RECON_VIRTUALPARTNERS_S2": line 460 col 9 (at pos 27654): "EXT"."SP_INBOUND_TXN_MAP": line 91 col 13 (at pos 6533): cannot
insert
    NULL
    or
update
    to NULL: "EXT"."SP_INBOUND_TXN_MAP": line 468 col 21 (at pos 30275): TrexColumnUpdate failed on table 'EXT:INBOUND_DATA_TXN' with error: constraint NOT NULL violation;

checkStringColumn(): found NULL on pos 0,
column 'ORDERID',
table 'EXT:INBOUND_DATA_TXN',
rc = 56