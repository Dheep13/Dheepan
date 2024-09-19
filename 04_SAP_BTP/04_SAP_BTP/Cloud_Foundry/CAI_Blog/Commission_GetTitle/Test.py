###########################################################################
# Function: MirrorPaymentAndIncoterms
#
# Purpose: To mirror incoterms and payment terms for one to many scenario. For more details refer to incident -681112/2021
# Created By: Deepan S
# Event: On Landing from CRM and on change of custom fields
# Modifications: 
###########################################################################

# when status changes from Open to Customer Accepted
# if Quote.OrderStatus.Name=="Preparing":
#     Quote.CustomFields.Allow('Incoterms 1').Content
#     Quote.CustomFields.Allow('Incoterms 2').Content
    Quote.GetCustomField('Incoterms 1 Mirror').Content = Quote.GetCustomField('Incoterms 1').Content
    Quote.GetCustomField('Incoterms 2 Mirror').Content = Quote.GetCustomField('Incoterms 2').Content
    # Quote.CustomFields.Disallow('Incoterms 1 Mirror').Content
    # Quote.CustomFields.Disallow('Incoterms 2 Mirror').Content

    # Quote.CustomFields.Allow('Payment Condition Free Text').Content
    # Quote.CustomFields.Allow('Payment Condition Selection').Content
    Quote.GetCustomField('Payment Condition Free Text Mirror').Content = Quote.GetCustomField('Payment Condition Free Text').Content
    Quote.GetCustomField('Payment Condition Selection Mirror').Content = Quote.GetCustomField('Payment Condition Selection').Content
    # Quote.CustomFields.Disallow('Payment Condition Free Text Mirror').Content
    # Quote.CustomFields.Disallow('Payment Condition Selection Mirror').Content


    #Also copy to 1 to many field. This will be displayed in Order Info

    Quote.GetCustomField('Incoterms 1 1toN').Content = Quote.GetCustomField('Incoterms 1').Content
    Quote.GetCustomField('Incoterms 2 1toN').Content = Quote.GetCustomField('Incoterms 2').Content
    Quote.GetCustomField('Payment Condition Free Text 1toN').Content = Quote.GetCustomField('Payment Condition Free Text').Content
    Quote.GetCustomField('Payment Condition Selection 1toN').Content = Quote.GetCustomField('Payment Condition Selection').Content
    #Then hide Quote.GetCustomField('Incoterms 1') and Quote.GetCustomField('Incoterms 2')
	# Show Quote.GetCustomField('Incoterms 1 Mirror').Content,  Quote.GetCustomField('Incoterms 2 Mirror').Content in additional Info
	# make Quote.GetCustomField('Incoterms 1 1toN') and Quote.GetCustomField('Incoterms 2 1toN') visible in order info


#For every new partial order copy Quote.GetCustomField('Incoterms 1 1toN').Content to Quote.GetCustomField('Incoterms 1').Content
#and Quote.GetCustomField('Incoterms 2 1toN').Content to Quote.GetCustomField('Incoterms 2').Content
#This will ensure CPI picks the right value
if Quote.OrderStatus.Name !="Preparing":
    Quote.GetCustomField('Incoterms 1').Content = Quote.GetCustomField('Incoterms 1 1toN').Content
    Quote.GetCustomField('Incoterms 2').Content = Quote.GetCustomField('Incoterms 2 1toN').Content
    Quote.GetCustomField('Payment Condition Free Text').Content = Quote.GetCustomField('Payment Condition Free Text 1toN').Content
    Quote.GetCustomField('Payment Condition Selection').Content = Quote.GetCustomField('Payment Condition Selection 1toN').Content
    Quote.CustomFields.Allow('Payment Condition Free Text Mirror').Content
    Quote.CustomFields.Allow('Payment Condition Selection Mirror').Content
    Quote.CustomFields.Allow('Incoterms 1 Mirror').Content
    Quote.CustomFields.Allow('Incoterms 2 Mirror').Content
    Quote.CustomFields.Disallow('Payment Condition Free Text').Content
    Quote.CustomFields.Disallow('Payment Condition Selection').Content
    Quote.CustomFields.Disallow('Incoterms 1').Content
    Quote.CustomFields.Disallow('Incoterms 2').Content


Quote.Save()

Quote.GetCustomField('Incoterms 1 Mirror').Content = Quote.GetCustomField('Incoterms 1').Content
Quote.GetCustomField('Incoterms 2 Mirror').Content = Quote.GetCustomField('Incoterms 2').Content

Quote.GetCustomField('Payment Condition Free Text Mirror').Content = Quote.GetCustomField('Payment Condition Free Text').Content
Quote.GetCustomField('Payment Condition Selection Mirror').Content = Quote.GetCustomField('Payment Condition Selection').Content

#Also copy to 1 to many field. This will be displayed in Order Info

Quote.GetCustomField('Incoterms 1 1toN').Content = Quote.GetCustomField('Incoterms 1').Content
Quote.GetCustomField('Incoterms 2 1toN').Content = Quote.GetCustomField('Incoterms 2').Content
Quote.GetCustomField('Payment Condition Free Text 1toN').Content = Quote.GetCustomField('Payment Condition Free Text').Content
Quote.GetCustomField('Payment Condition Selection 1toN').Content = Quote.GetCustomField('Payment Condition Selection').Content