
Incentive Rule - Per Credit(Commissions)

Position - TestTelco1

PMR_TelBMS_ConnCount_TOTAL - 800

SMR_TelBMS_ConnCount_TOTAL_Achievement - 2000% 
SMR_TelBMS_ConnCount_TOTAL - 800


ITR_TelBMS_Total_StepRateBonus - 9.96
ITR_TelBMS_Indv_Connection_Payout - 7,680

how?

SMR_TelBMS_ConnCount_TOTAL_Achievement  --  
---A Rule that Calculates other Key Performance Measures (Secondary Measurement)
---Amount is : 
Set Unit Type ( PMR_TelBMS_ConnCount_TOTAL:month / SMR_TelBMS_ConnCount_TOTAL_Target:month , percent ) 
--final value is
800/40

SMR_TelBMS_ConnCount_TOTAL_Target
---A Rule that Calculates other Key Performance Measures (Secondary Measurement)
-- Amount is : 
Convert Boolean to Value ( F_TelBMS_ConnCount_FVV_Target ) 
* FVV_TelBMS_ConnCount_Target_TOTAL + Convert Boolean to Value ( F_TelBMS_ConnCount_LookUp_Target ) 
* LTV_TelBMS_Target_Weights_Category_Channel ( Period.End Date , "TOTAL" , "TOTAL" , Position.Title.Name , Position.GA1:Commission Payout Currency , "New" , "Target" , Position.Title.GA1:Channel , "Default" , "Default" , 1 , 1 ) + 
    Convert Boolean to Value ( F_TelBMS_ConnCount_Transaction_Target ) * PMR_TelBMS_ConnCount_TOTAL_Target:month 

FVV_TelBMS_ConnCount_Target_TOTAL
--this refers to a fv with value 40. Hence the SMR_TelBMS_ConnCount_TOTAL_Target is 40


ITR_TelBMS_Total_StepRateBonus
---Measurements associated directly to the rep
---Source is - PMR_TelBMS_ConnCount_TOTAL 
value is 800
---An incentive that calculates an amount based on a rate that needs to be calculated
---Stepped rate based on a global rate table
---choose a rate table - RT_TelBMC_ConnectionVolume_Achv
---Contribution amount - Credit.Value
---Is rate based on Target attainment - yes - SMR_TelBMS_ConnCount_TOTAL_Achievement:month 


RT_TelBMC_ConnectionVolume_Achv
--for 800 rate is 
For 1st step 200 - 200 * 0.00 = 0
For 2nd step 300 - 300 * 0.02 = 6
For 3rd step 250 - 250 * 0.0375 =9.375
For 4th step 50  -   50 * 0.05 = 2.5

17.375


(120 * 0.02) +
(40 * 0.02)+
(120 *  0.02) +
(120 *  0.02) +
(72 * 0.02 )+
(120 * 0.02) +
(40 * 0.02 )+
(280 * 0.02) +
(40 * 0.02 )+
(200 * 0.02) +
(280 * 0.02)


2.4
0.08
2.4
2.4
0.14
2.4

