**********************************
before running this file, run the following code with q:

h:hopen `$":usinfkdb-d1:15019"

orders:h"select time,transactTime,sym,venue,exchangeOrderId,displayQty,orderQty,lastQty,leavesQty,cumQty from ustOrder where date=max date,transactTime within 21 22t"

save `:orders.csv
**********************************

Run this file in a python interpreter:

datadate() returns an ELKURL according to the correct date input
elkimport() takes the ELKURL and converts its data into a csv file
compareclordid() compares clOrdId in ELK and KDB
comparerows() compares other fields and outputs a csv file if there are discrepancies
