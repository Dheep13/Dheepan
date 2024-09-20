DROP TABLE EXT.TRYG_REDEMPTION_REPORT;



CREATE COLUMN TABLE EXT.TRYG_REDEMPTION_REPORT (
  PolicyNumber VARCHAR(255),
  InsuranceGrpName VARCHAR(255),
  FGR SMALLINT,
  CustomerName VARCHAR(255),
  Count BIGINT,
  SplitFlag SMALLINT,
  Weight DECIMAL(25,10),
  PolicyStartDate DATE,
  NewPremium DECIMAL(25,10),
  OldPremium DECIMAL(25,10),
  BeregnetProvision DECIMAL(25,10),
  Agent VARCHAR(255),
  AgentTitle VARCHAR(255),
  Period VARCHAR(255),
  Cause VARCHAR(255),
  UltimoPoint DECIMAL(25,10),
  Tilgangstype VARCHAR(255),
  RabatGl VARCHAR(255),
  RabatNy VARCHAR(255),
  Reversal VARCHAR(255),
  PaymentStatus VARCHAR(255),
  PaymentDate VARCHAR(255),
  PartnerAgreement1 VARCHAR(255),
  PartnerAgreement2 VARCHAR(255),
  CommissionPercentage VARCHAR(255),
  ProcessingUnitSeq BIGINT
);
