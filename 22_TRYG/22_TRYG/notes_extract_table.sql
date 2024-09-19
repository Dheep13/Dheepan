CREATE COLUMN TABLE EXT.TRYG_NOTES_EXTRACT  (Navn   varchar(255),         
MA_NR  varchar(255),    
AS_NR  varchar(255),    
INDL_LON_JANUAR  decimal(25,2),
INDL_REAL_JANUAR   decimal(25,2),
INDL_LON_FEBRUAR  decimal(25,2),
INDL_REAL_FEBRUAR  decimal(25,2),
INDL_LON_MARTS   decimal(25,2), 
INDL_REAL_MARTS   decimal(25,2), 
INDL_LON_APRIL    decimal(25,2),
INDL_REAL_APRIL    decimal(25,2),
INDL_LON_MAJ     decimal(25,2), 
INDL_REAL_MAJ     decimal(25,2), 
INDL_LON_JUNI   decimal(25,2),  
INDL_REAL_JUNI     decimal(25,2),
INDL_LON_JULI   decimal(25,2),  
INDL_REAL_JULI    decimal(25,2), 
INDL_LON_AUGUST   decimal(25,2),
INDL_REAL_AUGUST   decimal(25,2),
INDL_LON_SEPTEMB  decimal(25,2),
INDL_REAL_SEPTEMB  decimal(25,2),
INDL_LON_OKTOBER  decimal(25,2),
INDL_REAL_OKTOBER  decimal(25,2),
INDL_LON_NOV     decimal(25,2),
INDL_REAL_NOV    decimal(25,2), 
INDL_LON_DEC    decimal(25,2), 
INDL_REAL_DEC    decimal(25,2), 
SKADE_INDL_MAL     decimal(25,2), 
SKADE_INDL_REAL   decimal(25,2), 
PORTEFOLJEUDV_MAL  decimal(25,2), 
PORTEFOLJEUDV_REAL  decimal(25,2), 
FLY_AFG integer not null default 0,          
PORTEFOLJE_PRIMO   decimal(25,2),
SERVICE_MAL       decimal(25,2),
SERVICE_REAL    decimal(25,2), 
LONSOMHED_REAL   decimal(25,2),
NYE_KONTRAKT_REAL  decimal(25,2),
NYEKUNDER_MAL    decimal(25,2),  
NYEKUNDER_REAL     decimal(25,2),
EGNE_KONTRAKT_MAL  decimal(25,2),
EGNE_KONTRAKT_REAL  decimal(25,2))



delete from EXT.TRYG_NOTES_EXTRACT;
insert into EXT.TRYG_NOTES_EXTRACT(
Navn ,      
MA_NR,
S_NR,
PORTEFOLJEUDV_MAL,
PORTEFOLJEUDV_REAL,
PORTEFOLJE_PRIMO,
LONSOMHED_REAL,
NYE_KONTRAKT_REAL,
EGNE_KONTRAKT_MAL,
EGNE_KONTRAKT_REAL
)
select
mp.PARTICIPANTNAME,mp.PAYEEID,mp.POSITIONNAME,
-- mp.TITLE,
-- ,mp.PERIOD,
-- ip.YEARLY_REDEMPTION ,
-- ip.REALIZED_REDEMPTION,ip.REDEMPTION_TARGET, --incentives
-- ip.REDEMPTION_TARGET_PENSION_AGENTS,ip.REDEMPTION_PENSION_YTD,--incentives
-- ip.NEW_CUSTOMERS_TARGET,ip.REALIZED_CUSTOMERS --incentives

-- ip.SERVICE_TARGET,ip.REALIZED_SERVICE,----incentives
mp.PORTFOLIO_TARGET,mp.REALIZED_PORTFOLIO_DEVELOPMENT,mp.PORTFOLIO_PRIMO, --measurements
mp.PROFITABILITY,--measurements
mp.NEW_CONTRACTS,--measurements

mp.NEW_CONTRACTS_TARGET, --measurements
mp.REALIZED_CONTRACTS

from ext.notes_extract_measurement_prestage mp
where mp.payeeid='3301'
and mp.period='May 2022';


update EXT.TRYG_NOTES_EXTRACT ne set (
-- Navn ,      
-- MA_NR,
-- S_NR,
SKADE_INDL_MAL,
SKADE_INDL_REAL,
NYEKUNDER_MAL,
NYEKUNDER_REAL,
SERVICE_MAL,
SERVICE_REAL
) =(
select
-- mp.PARTICIPANTNAME,mp.PAYEEID,mp.POSITIONNAME,
-- mp.TITLE,mp.PERIOD,
-- ip.YEARLY_REDEMPTION ,
-- ip.REALIZED_REDEMPTION,
mp.REDEMPTION_TARGET_YTD, --incentives
-- mp.REDEMPTION_TARGET_PENSION_AGENTS,
mp.REALIZED_REDEMPTION_YTD,--incentives
mp.NEW_CUSTOMERS_TARGET,
mp.REALIZED_CUSTOMERS, --incentives

mp.SERVICE_TARGET,mp.REALIZED_SERVICE----incentives
-- mp.PORTFOLIO_TARGET,mp.REALIZED_PORTFOLIO_DEVELOPMENT,mp.PORTFOLIO_PRIMO, --measurements
-- mp.PROFITABILITY,--measurements
-- mp.NEW_CONTRACTS,--measurements

-- mp.NEW_CONTRACTS_TARGET, --measurements
-- mp.REALIZED_CONTRACTS


from ext.notes_extract_incentive_prestage mp
where mp.payeeid= ne.ma_nr
and mp.POSITIONNAME=ne.S_NR
and mp.period='May 2022');

insert into EXT.TRYG_NOTES_EXTRACT (
Navn ,      
MA_NR,
S_NR,
SKADE_INDL_MAL,
SKADE_INDL_REAL,
NYEKUNDER_MAL,
NYEKUNDER_REAL,
SERVICE_MAL,
SERVICE_REAL
) 

(select
mp.PARTICIPANTNAME,mp.PAYEEID,mp.POSITIONNAME,
-- mp.TITLE,mp.PERIOD,
-- ip.YEARLY_REDEMPTION ,
-- ip.REALIZED_REDEMPTION,
mp.REDEMPTION_TARGET_YTD, --incentives
-- mp.REDEMPTION_TARGET_PENSION_AGENTS,
mp.REALIZED_REDEMPTION_YTD,--incentives
mp.NEW_CUSTOMERS_TARGET,
mp.REALIZED_CUSTOMERS, --incentives

mp.SERVICE_TARGET,mp.REALIZED_SERVICE----incentives
-- mp.PORTFOLIO_TARGET,mp.REALIZED_PORTFOLIO_DEVELOPMENT,mp.PORTFOLIO_PRIMO, --measurements
-- mp.PROFITABILITY,--measurements
-- mp.NEW_CONTRACTS,--measurements

-- mp.NEW_CONTRACTS_TARGET, --measurements
-- mp.REALIZED_CONTRACTS

from ext.notes_extract_incentive_prestage mp
where not exists (select * from EXT.TRYG_NOTES_EXTRACT ne
where mp.payeeid= ne.ma_nr
and mp.POSITIONNAME=ne.S_NR)
and mp.period='May 2022');


update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_JANUAR=
(select yearly_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'January%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_JANUAR=(select realized_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'January%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_FEBRUAR=(select yearly_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'February%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_FEBRUAR=(select realized_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'February%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_MARTS=(select yearly_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'March%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_MARTS=(select realized_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'March%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_APRIL=(select yearly_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'April%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_APRIL=(select realized_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'April%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_MAJ=(select yearly_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'May%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_MAJ=(select realized_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'May%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_JUNI=(select yearly_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'June%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_JUNI=(select realized_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'June%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_JULI=(select yearly_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'July%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_JULI=(select realized_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'July%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_AUGUST=(select yearly_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'August%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_AUGUST=(select realized_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'August%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_SEPTEMB=(select yearly_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'September%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_SEPTEMB=(select realized_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'September%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_OKTOBER=(select yearly_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'October%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_OKTOBER=(select realized_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'October%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_NOV=(select yearly_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'November%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_NOV=(select realized_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'November%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_LON_DEC=(select yearly_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'December%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);
update ext.TRYG_NOTES_EXTRACT ne set INDL_REAL_DEC=(select realized_redemption
	from ext.notes_extract_incentive_prestage ip
	where ip.period like 'December%'
	and ip.participantname=ne.Navn
	and ip.payeeid=ne.ma_nr
	and ip.positionname=ne.s_nr
);



select * from cs_plrun where description like '%December 2022%'
select * from EXT.TRYG_NOTES_EXTRACT where s_nr='2860-50002298';
select * from cs_position where name='2860-50002298';
select genericnumber4,genericnumber3, genericnumber1  from cs_measurement where positionseq=4785074604081792
and name='SMO_TBVS11_Profitability_+/-Benchmark'
and periodseq=(select periodseq from cs_period where name='October 2022' and removedate='2200-01-01');

select * from cs_incentive where genericattribute1='New Customers Grade'
and periodseq=(select periodseq from cs_period where name='December 2022' and removedate='2200-01-01')
and positionseq=4785074604081792;

select * from cs_measurement where name='PMO_New_Contracts_YTD'
and periodseq=(select periodseq from cs_period where name='December 2022' and removedate='2200-01-01')
and positionseq=4785074604081792;








CREATE COLUMN TABLE EXT.TRYG_NOTES_EXTRACT_MEASUREMENT(
	PARTICIPANTNAME VARCHAR(100),
	PAYEEID VARCHAR(100),
	POSITIONNAME VARCHAR(100),
	TITLE VARCHAR(100),
	PERIOD VARCHAR(50),
	YEARLY_REDEMPTION DECIMAL(25, 2) CS_FIXED,
	REALIZED_REDEMPTION DECIMAL(25, 2) CS_FIXED,
	REDEMPTION_TARGET DECIMAL(25, 2) CS_FIXED,
	REDEMPTION_PENSION_YTD DECIMAL(25, 2) CS_FIXED,
	PORTFOLIO_TARGET DECIMAL(25, 2) CS_FIXED,
	REALIZED_PORTFOLIO_DEVELOPMENT DECIMAL(25, 2) CS_FIXED,
	PORTFOLIO_PRIMO DECIMAL(25, 2) CS_FIXED,
	SERVICE_TARGET DECIMAL(25, 2) CS_FIXED,
	REALIZED_SERVICE DECIMAL(25, 2) CS_FIXED,
	PROFITABILITY DECIMAL(25, 2) CS_FIXED,
	NEW_CONTRACTS DECIMAL(25, 2) CS_FIXED,
	NEW_CUSTOMERS_TARGET DECIMAL(25, 2) CS_FIXED,
	REALIZED_CUSTOMERS DECIMAL(25, 2) CS_FIXED,
	NEW_CONTRACTS_TARGET DECIMAL(25, 2) CS_FIXED,
	REALIZED_CONTRACTS DECIMAL(25, 2) CS_FIXED
)
UNLOAD PRIORITY 5 AUTO MERGE;

CREATE COLUMN TABLE EXT.TRYG_NOTES_EXTRACT_INCENTIVE(
	PARTICIPANTNAME VARCHAR(100),
	PAYEEID VARCHAR(100),
	POSITIONNAME VARCHAR(100),
	TITLE VARCHAR(100),
	PERIOD VARCHAR(50),
	YEARLY_REDEMPTION DECIMAL(25, 2) CS_FIXED,
	REALIZED_REDEMPTION DECIMAL(25, 2) CS_FIXED,
	REDEMPTION_TARGET_YTD DECIMAL(25, 2) CS_FIXED,
	REDEMPTION_TARGET_PENSION_AGENTS DECIMAL(25, 2) CS_FIXED,
	REALIZED_REDEMPTION_YTD DECIMAL(25, 2) CS_FIXED,
	PORTFOLIO_TARGET DECIMAL(25, 2) CS_FIXED,
	REALIZED_PORTFOLIO_DEVELOPMENT DECIMAL(25, 2) CS_FIXED,
	PORTFOLIO_PRIMO DECIMAL(25, 2) CS_FIXED,
	SERVICE_TARGET DECIMAL(25, 2) CS_FIXED,
	REALIZED_SERVICE DECIMAL(25, 2) CS_FIXED,
	PROFITABILITY DECIMAL(25, 2) CS_FIXED,
	NEW_CONTRACTS DECIMAL(25, 2) CS_FIXED,
	NEW_CUSTOMERS_TARGET DECIMAL(25, 2) CS_FIXED,
	REALIZED_CUSTOMERS DECIMAL(25, 2) CS_FIXED,
	NEW_CONTRACTS_TARGET DECIMAL(25, 2) CS_FIXED,
	REALIZED_CONTRACTS DECIMAL(25, 2) CS_FIXED
)
UNLOAD PRIORITY 5 AUTO MERGE;



