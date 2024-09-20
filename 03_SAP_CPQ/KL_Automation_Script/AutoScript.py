UniqueEntryId=3
Del_UniqueEntryId=2
if Quote is not None:
    shippingMethod=Quote.GetCustomField("Shipping Method").Content
    StorageLocationItemCategory = Quote.GetCustomField("StorageLocationItemCategory").Content
    DeliverySite=Quote.GetCustomField("Delivery Site").Content
    DeliveryDate=Quote.GetCustomField("DeliveryDate").Content
    ShippingDistrict=Quote.GetCustomField("Shipping District").Content
    Trace.Write(shippingMethod)
    Trace.Write(StorageLocationItemCategory)
    Trace.Write(DeliverySite)
    Trace.Write(DeliveryDate)

for item in Quote.Items:
    item["ShippingMethod"].Value = shippingMethod
    #item_level=item["StorageLocationItemCategory"].Value
    #header_level=Quote.GetCustomField("StorageLocationItemCategory").Content.strip()
    item["StorageLocationItemCategory"].Value=str(UniqueEntryId)
    item["DeliverySite"].Value = str(Del_UniqueEntryId)
    item["DeliveryDate"].Value = UserPersonalizationHelper.CovertToDate(DeliveryDate)
    Trace.Write(item["DeliveryDate"].Value)



UniqueEntryId=1
#Del_UniqueEntryId=2
shippingMethod=Quote.GetCustomField("Shipping Method").Content
shippingDisctrict=Quote.GetCustomField("Shipping District")
codeList=shippingDisctrict.Content.split(',')
codeValue=str(codeList[len(codeList)-1])


for item in Quote.Items:
    item["StorageLocationItemCategory"].Value=str(UniqueEntryId)
    item["ShippingMethod"].Value = shippingMethod
    item["ShippingDistrict"].Value=codeValue.strip()



for item in Quote.Items:
    item["StorageLocationItemCategory"].Value=str(UniqueEntryId)
    item["ShippingMethod"].Value = shippingMethod
    item["ShippingDistrict"].Value=codeValue.strip()
    item["DeliverySite"].Value = str(Del_UniqueEntryId=1)
    item["ShipTo"].Value = codeValue2
    item["StorageLocation"].Value=StorageLocation 


#final
import CommonModule

UniqueEntryId=1
Del_UniqueEntryId=1
stor_loc_uniqueentryid=1
shippingMethod=Quote.GetCustomField("Shipping Method").Content
shippingDisctrict=Quote.GetCustomField("Shipping District")
codeList=shippingDisctrict.Content.split(',')
codeValue=str(codeList[len(codeList)-1])
DeliverySite=Quote.GetCustomField("Delivery Site").Content
DeliveryDate=Quote.GetCustomField("DeliveryDate").Content
ShipToID = Quote.GetCustomField("Ship To ID Hidden").Content
token = Quote.GetCustomField("StorageLocationItemCategory").Content.split(',')
#Trace.Write(token)
StorageLocation = token[0]
Trace.Write(StorageLocation)
PartnerFunction = "Ship-to Party"
ShipToAddress = CommonModule.GetPartnerFunction(Quote, ShipToID, PartnerFunction)
codeList=ShipToAddress.split(',')
codeValue2=str(codeList[len(codeList)-1]).strip()

for item in Quote.Items:
    item["StorageLocation"].Value=str(1000)
    item["StorageLocationItemCategory"].Value=str(UniqueEntryId)
    item["ShippingMethod"].Value = shippingMethod
    item["ShippingDistrict"].Value=codeValue.strip()
    item["DeliverySite"].Value = str(Del_UniqueEntryId)
    item["ShipTo"].Value = codeValue2
    item["DeliveryDate"].Value = UserPersonalizationHelper.CovertToDate(DeliveryDate)
