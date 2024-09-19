
--liquibase formatted sql

--changeset deepan s:EXT.TEST splitStatements:false stripComments:false
--comment: Create table

CREATE COLUMN TABLE EXT.TEST (
    CLAWBACKTYPE VARCHAR(4) NOT NULL);

