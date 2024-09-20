startDate = '2022-03-06'
endDate = '2022-05-28'
sql = "select count(*) as a from quotes where DateCreated between '"+ startDate +"' and '"+ endDate +"' "
total = SqlHelper.GetFirst(sql)
Trace.Write(total.a)

rows= SqlHelper.GetList("select companyid, count(*) as countOfQuotes from quotes q, users u where q.DateCreated between '"+ startDate +"' and '"+ endDate +"' and u.USERNAME=q.Username group by companyid")


for row in rows:
    Trace.Write('company code '+str(row.companyid))
    Trace.Write('count of quotes ' + str(row.countOfQuotes))

