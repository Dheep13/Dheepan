def createSapOpportunityLine(cpqQuoteItem):
    item = CustQteCRMCrteReq_s_V1Itm()
    item.ID = str(cpqQuoteItem.QuoteItemGuid) 
    productItem = CustQteCRMCrteReq_s_V1ItmProd()
    productInternalID = ProductInternalID()
    itemTax = CustQteCRMCrteReq_s_V1ItmPrAndTxCalcPrComp()
    itemTax.PriceSpecificationElementTypeCode = PriceSpecificationElementTypeCode()
    productInternalID.Value = cpqQuoteItem.PartNumber
    productItem.EnteredProductInternalID = productInternalID
    item.Product = productItem
    itemRate = Rate()    
    itemRate.DecimalValue = cpqQuoteItem.NetPrice
    itemRate.CurrencyCode = Quote.SelectedMarket.CurrencyCode
    itemTax.Rate = itemRate
    taxes = Array[CustQteCRMCrteReq_s_V1ItmPrAndTxCalcPrComp]([itemTax])
    item.PriceAndTaxCalculation = taxes
    scheduleLine = CustQteCRMCrteReq_s_V1SchedLine()
    scheduleLine.TypeCode = 'TypeCode'
    quantity = Quantity()    
    quantity.Value = cpqQuoteItem.Quantity
    scheduleLine.Quantity = quantity
    dateTime = LOCAL_DateTime()
    dateTime.timeZoneCode = ''
    scheduleLine.DateTime = dateTime
    scheduleLines = Array[CustQteCRMCrteReq_s_V1SchedLine]([scheduleLine])
    item.ScheduleLine = scheduleLines
    arrayOfItems.Add(item)

try:
    ws = WebServiceHelper.Load('wsdl', 'http://sap_instance_web_service_url', 'username', 'passs')    
    clr.AddReference(clr.GetClrType(type(ws)).Assembly)
    import CustomerQuoteCRMCreateRequestMessage_sync_V1, MEDIUM_Name, CustQteCRMCrteReq_s_V1CustQte, BusinessTransactionDocumentID, CustQteCRMCrteReq_s_V1BuyrPty, PartyInternalID, CustQteCRMCrteReq_s_V1PrAndTxCalcPrComp, Rate,   CustQteCRMCrteReq_s_V1Itm, CustQteCRMCrteReq_s_V1ItmProd, ProductInternalID, CustQteCRMCrteReq_s_V1ItmPrAndTxCalcPrComp, PriceSpecificationElementTypeCode, CustQteCRMCrteReq_s_V1SchedLine, Quantity, LOCAL_DateTime, CustQteCRMCrteReq_s_V1BusTransacDocRef

    request = CustomerQuoteCRMCreateRequestMessage_sync_V1()
    customerQuote = CustQteCRMCrteReq_s_V1CustQte()

    #BuyerID
    buyerId = BusinessTransactionDocumentID()
    buyerId.Value = Quote.CompositeNumber
    customerQuote.BuyerID = buyerId

    #BuyerParty
    customerBuyer = CustQteCRMCrteReq_s_V1BuyrPty()
    internalId = PartyInternalID()
    internalId.Value = Quote.BillToCustomer.CrmAccountId
    customerBuyer.InternalID = internalId
    customerQuote.BuyerParty = customerBuyer

    #PriceAndTaxCalculation
    priceAndTax = CustQteCRMCrteReq_s_V1PrAndTxCalcPrComp()
    priceAndTax.PriceSpecificationElementTypeCode = PriceSpecificationElementTypeCode()
    priceAndTax.PriceSpecificationElementTypeCode.Value = 'ZD01'
    rate = Rate()
    rate.DecimalValue = Quote.Total.ShippingCostInMarket
    rate.CurrencyCode = Quote.SelectedMarket.CurrencyCode.ToString()
    priceAndTax.Rate = rate
    priceAndTaxes = Array[CustQteCRMCrteReq_s_V1PrAndTxCalcPrComp]([priceAndTax])
    customerQuote.PriceAndTaxCalculation = priceAndTaxes

    #BusinessTransactionDocumentReference
    businessTransaction = CustQteCRMCrteReq_s_V1BusTransacDocRef()
    businessId = BusinessTransactionDocumentID()
    businessId.Value = Quote.OpportunityId
    businessTransaction.ID = businessId
    businessTransaction.TypeCode = '72'
    businessTransaction.BusinessTransactionDocumentRelationshipRoleCode = '1'
    businessTransactions = Array[CustQteCRMCrteReq_s_V1BusTransacDocRef]([businessTransaction])
    customerQuote.BusinessTransactionDocumentReference = businessTransactions

    arrayOfItems = []

    #Fill the request with the CPQ Quote items
    for mainItem in Quote.MainItems:
        createSapOpportunityLine(mainItem)
        for lineItem in mainItem.LineItems:
            createSapOpportunityLine(lineItem)

    customerQuote.Item = Array[CustQteCRMCrteReq_s_V1Itm](arrayOfItems)    

    opname = MEDIUM_Name()
    opname.Value  = Quote.GetCustomField("Opportunity Name").Content
    customerQuote.Name = opname   
    request.CustomerQuote = customerQuote

    # invoke web service
    ws.CustomerQuoteCRMCreateRequestConfirmation_In_V1(request)

except Exception, e:
    Log.Write('Error while creating Quote in SAP CRM: ' + str(e))