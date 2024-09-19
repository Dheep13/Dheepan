import os

# Define the directory containing the files
directory = r"C:\Users\I352471\Desktop\Singtel Files\29 30 31st Files\31st Files"

# Create a dictionary mapping original names to new names
file_map = {
    "BCC_SCII_BundleOrders_20240731060039.txt":"1756_BCCSCIIBundleOrders_20240731060039.txt",
"BCC_SCII_CancellationOrders_20240731060545.txt":"1756_BCCSCIICancellationOrders_20240731060545.txt",
"BCC_SCII_ChannelPartnerHierarchy_20240731060019.txt":"1756_BCCSCIIChannelPartnerHierarchy_20240731060019.txt",
"BCC_SCII_ChannelPartnerMaster_20240731060017.txt":"1756_BCCSCIIChannelPartnerMaster_20240731060017.txt",
"BCC_SCII_ClosedBroadBandOrders_20240731063813.txt":"1756_BCCSCIIClosedBroadBandOrders_20240731063813.txt",
"BCC_SCII_ClosedFixedVoiceStandaloneOrders_20240731060928.txt":"1756_BCCSCIIClosedFixedVoiceStandaloneOrders_20240731060928.txt",
"BCC_SCII_ClosedMobileOrders_20240731061224.txt":"1756_BCCSCIIClosedMobileOrders_20240731061224.txt",
"BCC_SCII_ClosedSmartHomeOrders_20240731060927.txt":"1756_BCCSCIIClosedSmartHomeOrders_20240731060927.txt",
"BCC_SCII_ClosedTVOrders_20240731061305.txt":"1756_BCCSCIIClosedTVOrders_20240731061305.txt",
"BCC_SCII_DiscountInfo_20240731060943.txt":"1756_BCCSCIIDiscountInfo_20240731060943.txt",
"BCC_SCII_MTPOSSalesOrders_20240731060446.txt":"1756_BCCSCIIMTPOSSalesOrders_20240731060446.txt",
"BCC_SCII_MTPOSStockInfo_20240731060023.txt":"1756_BCCSCIIMTPOSStockInfo_20240731060023.txt",
"BCC_SCII_ODS_Dash_20240731080002.txt":"1756_BCCSCIIODSDash_20240731080002.txt",
"BCC_SCII_ODS_Dash_Registration_20240731060927.txt":"1756_BCCSCIIODSDashRegistration_20240731060927.txt",
"BCC_SCII_PrepaidTopupErrorCases_20240731060129.txt":"1756_BCCSCIIPrepaidTopupErrorCases_20240731060129.txt",
"BCC_SCII_SubmittedBroadBandOrders_20240731063931.txt":"1756_BCCSCIISubmittedBroadBandOrders_20240731063931.txt",
"BCC_SCII_SubmittedFixedVoiceStandaloneOrders_20240731060929.txt":"1756_BCCSCIISubmittedFixedVoiceStandaloneOrders_20240731060929.txt",
"BCC_SCII_SubmittedMobileOrders_20240731061220.txt":"1756_BCCSCIISubmittedMobileOrders_20240731061220.txt",
"BCC_SCII_SubmittedSmartHomeOrders_20240731060927.txt":"1756_BCCSCIISubmittedSmartHomeOrders_20240731060927.txt",
"BCC_SCII_SubmittedTVOrders_20240731061310.txt":"1756_BCCSCIISubmittedTVOrders_20240731061310.txt",
"BCC_SCII_VoucherInfo_20240731060945.txt":"1756_BCCSCIIVoucherInfo_20240731060945.txt",
"EDW_SCII_CCOProfiles_20240731112108.txt":"1756_EDWSCIICCOProfiles_20240731112108.txt",
"EDW_SCII_DashSignUp_20240731041453.txt":"1756_EDWSCIIDashSignUp_20240731041453.txt",
"EDW_SCII_DashTopUp_20240731043319.txt":"1756_EDWSCIIDashTopUp_20240731043319.txt",
"EDW_SCII_mRemitTransactions_20240731060659.txt":"1756_EDWSCIImRemitTransactions_20240731060659.txt",
"HRCentral-SCII-SalesmanProfile_20240731215018.txt":"1756_HRCentralSCIISalesmanProfile_20240731215018.txt",
"ITDM_SCII_PrepaidTopup_20240731.txt":"1756_ITDMSCIIPrepaidTopup_20240731000000.txt",
"SAP_SCII_Equipmentprice_20240731020018.txt":"1756_SAPSCIIEquipmentprice_20240731020018.txt",
"SAP_SCII_PhoenixCard_20240731013018.csv":"1756_SAPSCIIPhoenixCard_20240731013018.csv",
"SAP_SCII_PrepaidSIM_20240731123918.csv":"1756_SAPSCIIPrepaidSIM_20240731123918.csv"
}

# Change the current working directory to the target directory
os.chdir(directory)

# Rename files based on the mapping
for original_name, new_name in file_map.items():
    if os.path.exists(original_name):
        os.rename(original_name, new_name)
        print(f'Renamed: {original_name} -> {new_name}')
    else:
        print(f'File not found: {original_name}')

print("File renaming completed.")
