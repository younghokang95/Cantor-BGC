import datetime
import requests
import json
import pandas as pd
import csv
import numpy

def datadate():
	realdate = datetime.datetime.now().strftime ("%Y%m%d")
	date = raw_input("Please Enter Date (YYYY.MM.DD):  ")
	
	if len(date.replace(".","")) == 8:
		if date.replace(".","")[0:4] <= realdate[0:4]:
			if date.replace(".","")[4:] <= realdate[4:]:
				if date.replace(".","")[6:] <= '31':
					date == date
					url = "http://tbustevent-q2:9250/et-ustorder-%s/_search?pretty=true&size=10000" % date
				else:
					print "Wrong Date Input"
					print "Please Enter Another Date"
					datadate()
			else:
				print "Wrong Date Input"
				print "Please Enter Another Date"
				datadate()
		else:
			print "Wrong Date Input"
			print "Please Enter Another Date"
			datadate()
	else:
		
		print "Wrong Date Input"
		print "Please Enter Another Date"
		datadate()	
	return url

pass

def elkimport():
	getelk = requests.get(datadate())
	jsonelk = json.loads(getelk.text)
	fout = open("elk.json" , "w")
	json.dump(jsonelk, fout, indent = 4)
	fout.close()
	data = json.loads(getelk.content)['hits']['hits']
	messagedata = []
	for a in data:
		messagedata.append(json.loads(a["_source"]["message"]))
	pdelk = pd.DataFrame(messagedata, columns = ["transactTime", "sym","venue","exchangeOrderId","displayQty","orderQty","lastQty","leavesQty","cumQtyQty","execType","ordStatus","clOrdId"])
	pdelk.to_csv('dailyelk.csv')

pass

def compareclordid():
	csv1 = pd.read_csv("dailykdb.csv")
	csv2 = pd.read_csv("dailyelk.csv")
	csv1_dropna = csv1.dropna()
	csv1_dropdupclean = csv1_dropna.drop_duplicates(keep='last').reset_index()
	orderid1 = csv1_dropdupclean['clOrdId']
	orderid11 = list(orderid1)
	orderid002 = csv2.drop_duplicates(keep='last')
	orderid2 = (orderid002["clOrdId"])
	commonordid1 = []
	uncommonordid1 = []
	commonall_1 = []
	uncommonall_1 = []
	commonordid2 = []
	uncommonordid2 = []
	commonall_2 = []
	uncommonall_2 = []
	for row1 in range(0, len(orderid11)):
		if orderid11[row1] in list(orderid2):
			commonordid1.append(orderid11[row1])
		else:
			uncommonordid1.append(orderid11[row1])
	for row1 in range(0, len(csv1_dropdupclean)):
		if orderid1[row1] in commonordid1:
			commonall_1.append(csv1_dropdupclean.loc[row1])
		else:
			uncommonall_1.append(csv1_dropdupclean.loc[row1])
	for row2 in range(0, len(orderid2)):
		if orderid2[row2] in orderid11:
			commonordid2.append(orderid2[row2])
		else:
			uncommonordid2.append(orderid2[row2])
	for row2 in range(0, len(orderid002)):
		if orderid2[row2] in orderid11:
			commonall_2.append(csv2.loc[row2])
		else:
			uncommonall_2.append(csv2.loc[row2])
	dataonlykdb = pd.DataFrame(uncommonall_1)
	dataonlykdb.to_csv("uncommon_trades_onlykdb.csv")
	dataonlyelk = pd.DataFrame(uncommonall_2)
	dataonlyelk.to_csv("uncommon_trades_onlyelk.csv")

pass
	
def comparerows():
	csv1 = pd.read_csv("dailykdb.csv")
	csv2 = pd.read_csv("dailyelk.csv")
	csv1_dropna = csv1.dropna()
	csv1_dropdupclean = csv1_dropna.drop_duplicates(keep='last').reset_index()
	csv1_exo = list(csv1_dropdupclean['exchangeOrderId'])
	csv1_transacttime = list(csv1_dropdupclean['transactTime'])
	csv1_exectype = list(csv1_dropdupclean['execType'])
	csv1_ordstatus = list(csv1_dropdupclean['ordStatus'])
	csv2_dropdupclean = csv2.drop_duplicates(keep='last').reset_index()
	csv2_exo = list(csv2_dropdupclean['exchangeOrderId'])
	csv2_transacttime = list(csv2_dropdupclean['transactTime'])
	csv2_exectype = list(csv2_dropdupclean['execType'])
	csv2_ordstatus = list(csv2_dropdupclean['ordStatus'])
	commonallkdb = []
	commonallelk = []
	uncommonallkdb = []
	uncommonallelk = []
	for row1 in range(0, len(csv1_dropdupclean)):
		for row2 in range(0, len(csv2_dropdupclean)):
			if csv1_transacttime[row1][:10] == csv2_transacttime[row2][:10]:
				if csv1_transacttime[row1][11:16] == csv2_transacttime[row2][11:16]:
					if str(round((float(csv1_transacttime[row1][17:22])), 0 )) == str(round((float(csv2_transacttime[row2][17:22])),  0 )):
						if csv1_exo[row1] == csv2_exo[row2]:
							commonallkdb.append(csv1_dropdupclean.loc[row1])
	for row2 in range(0, len(csv2_dropdupclean)):
		for row1 in range(0, len(csv1_dropdupclean)):
			if csv2_transacttime[row2][:10] == csv1_transacttime[row1][:10]:
				if csv2_transacttime[row2][11:16] == csv1_transacttime[row1][11:16]:
					if str(round((float(csv2_transacttime[row2][17:22])), 0 )) == str(round((float(csv1_transacttime[row1][17:22])),  0 )):
						if csv2_exo[row2] == csv1_exo[row1]:
							commonallelk.append(csv2_dropdupclean.loc[row2])
	if len(commonallkdb) == len(csv1_dropdupclean):
		if len(commonallelk) == len(csv2_dropdupclean):
			print "There are no discrepancies" 
	for row1 in range(0, len(csv1_dropdupclean)):
		if csv1_exo[row1] not in csv2_exo:
			if csv1_exectype[row1] not in csv2_exectype:
				if csv1_ordstatus[row1] not in csv2_ordstatus:
					uncommonallkdb.append(csv1_dropdupclean.loc[row1])
	for row2 in range(0, len(csv2_dropdupclean)):
		if csv2_exo[row2] not in csv1_exo:
			if csv2_exectype[row1] not in csv1_exectype:
				if csv2_ordstatus[row1] not in csv1_ordstatus:
					uncommonallelk.append(csv2_dropdupclean.loc[row2])

	pduncommonallkdb = pd.DataFrame(uncommonallkdb)
	pduncommonallkdb.to_csv("missingDataInELK.csv")
	pduncommonallelk = pd.DataFrame(uncommonallelk)
	pduncommonallelk.to_csv("missingDataInKDB.csv")

pass
	
if __name__ == '__main__':
	elkimport()
	comparerows()