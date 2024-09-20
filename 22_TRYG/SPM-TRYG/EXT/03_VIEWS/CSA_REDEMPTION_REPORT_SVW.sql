
DROP VIEW "EXT"."CSA_REDEMPTION_REPORT_SVW";
CREATE VIEW "EXT"."CSA_REDEMPTION_REPORT_SVW"("ROW_NUM", 
	"YEAR", 
	"TABLE_NAME", 
	"PERIOD", 
	"POLICYNUMBER", 
	"INSURANCEGRPNAME", 
	"CUSTOMERNAME", 
	"SPLITFLAG", 
	"SPLITTYPE", 
	"WEIGHT", 
	"POLICY_COMP_DATE", 
	"POLICYSTARTDATE", 
	"NEWPREMIUM", 
	"OLDPREMIUM", 
	"BEREGNETPROVISION", 
	"ARSAG_CAUSE", 
	"ULTIMOPOINT_TEXT", 
	"TILGANGSTYPE", 
	"RABATGL", 
	"RABATNY", 
	"PAYMENTSTATUS", 
	"PAYMENTDATE", 
	"PAYMENTDATE_TEXT", 
	"PAYMENTDATE_TEXT2", 
	"PAYMENTDATE_YEAR", 
	"POSITIONNAME", 
	"POSITIONSEQ", 
	"PERIODSEQ", 
	"PARTICIPANT_NAME", 
	"PARTNERAGREEMENT1", 
	"PARTNERAGREEMENT2", 
	"FGR", 
	"ULTIMOPOINT")
AS
	SELECT 1 AS row_num, 
			right(
				period, 
				4
			) AS year, 
			table_name, 
			period, 
			POLICYNUMBER, 
			INSURANCEGRPNAME, 
			CUSTOMERNAME, 
			SPLITFLAG, 
			split_type AS SPLITTYPE, 
			to_varchar(
				WEIGHT, 
				'0'
			) AS WEIGHT, 
			policy_comp_date, 
			POLICYSTARTDATE, 
			to_varchar(
				NEWPREMIUM, 
				'0'
			) AS NEWPREMIUM, 
			to_varchar(
				OLDPREMIUM, 
				'0'
			) AS OLDPREMIUM, 
			to_varchar(
				BEREGNETPROVISION, 
				'0.00'
			) AS BEREGNETPROVISION, 
			CAUSE AS Arsag_Cause, 
			to_varchar(
				ULTIMOPOINT, 
				'0.00'
			) AS ULTIMOPOINT_TEXT, 
			TILGANGSTYPE, 
			to_varchar(
				RABATGL * 100, 
				'0'
			) || ' %' AS RABATGL, 
			to_varchar(
				RABATNY * 100, 
				'0'
			) || ' %' AS RABATNY, 
			PAYMENTSTATUS, 
			PAYMENTDATE, 
			to_char(
				PAYMENTDATE, 
				'Month yyyy'
			) AS Paymentdate_text, 
			to_char(
				PAYMENTDATE, 
				'Month yyyy'
			) AS Paymentdate_text2, 
			to_char(
				PAYMENTDATE, 
				'yyyy'
			) AS Paymentdate_year, 
			Agent AS positionname, 
			Positionseq, 
			Periodseq, 
			Participant_name, 
			Partneragreement1, 
			Partneragreement2, 
			to_varchar(FGR) AS FGR, 
			ULTIMOPOINT
		FROM EXT.TRYG_REDEMPTION_REPORT;
