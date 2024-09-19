import bisect as b

rates = [0, 7.5, 10, 12, 15]   


brackets = [84000,        # first 84k
            98000,        # next  14k
            112000,       # next  14k
            126000,       # next  14k
            146000]       # next  20k


base_tax = [0,            # 84k * 0%
            14000,        # 14k * 7.5%
            14000,        # 14k * 7.5% + 14000
            14000,        # 14k * 7.5% + 14000 + 14000
            20000]        # 20k * 15% + 14000 + 14000 + 14000

income = 145000

def tax(income):
    i = b.bisect(brackets, income)
    print(i)
    if not i:
        print ('0')
    rate = rates[i]
    print('Rate is'+ str(rate))
    bracket = brackets[i-1]
    print('bracket is'+ str(bracket))
    income_in_bracket = income - bracket
    print('income_in_bracket is'+ str(income_in_bracket))
    tax_in_bracket = income_in_bracket * rate / 100
    print('tax_in_bracket is'+ str(tax_in_bracket))
    total_tax = base_tax[i-1] + tax_in_bracket
    print(total_tax)


tax(income)