import pyodbc
import os 
import json
import time
import pandas as pd
from pandas import DataFrame
import requests		
import base64


from datetime import datetime
from dotenv import load_dotenv, find_dotenv


class PySageError(Exception):
	pass

_all_items = "SELECT * FROM {}".format('ItemNames')
_all_items_price = "SELECT * FROM {}".format('ItemSellingPrices')
_all_items_stock = "SELECT * FROM {}".format('Stock')
_all_sales = "SELECT TOP 20 * FROM {}".format('SaleTransaction') 
_all_sales_details = "SELECT TOP 20  * FROM {}".format('SaleTransactionDetails')

class Get_data():
	
	def __init__(self):
		load_dotenv(find_dotenv())
		# Create a connection string to dataBase 
		self.driver = '{Microsoft Access Driver (*.mdb)}'
		self.dbq = r'C:\Users\Droid\Desktop\SageData\SageRetail\POS\POSData.mdb'
		self.cnxn = pyodbc.connect(driver=self.driver, dbq=self.dbq)
		self.cursor = self.cnxn.cursor()
		self.sleepTime = 10
		self.serverAdreess = 'http://192.168.1.6:8000/api/v1'

		#Pandas db's
		self.items = False
		self.sales = False
	
	def _constructor_(self):
		
		while True:
			if not self._update_rools():
				break
			time.sleep(self.sleepTime)
			pass

	def _sage_db_query(self,sql,fileName,columns):
    	
		cnxn = pyodbc.connect(driver=self.driver ,dbq=self.dbq)
		df = pd.read_sql(sql, cnxn)
		df = DataFrame(df, columns = columns)
		df.to_json(fileName)

		return df

	def _get_connection_string(self):
		# Make sure environment variables loaded.
		try:
			try:
				# Python 2
				connection_string = os.environ['PYSAGE_CNXN'].decode('utf8')
			except AttributeError:
				# Python 3
				connection_string = os.environ['PYSAGE_CNXN']
		except KeyError:
			raise PySageError('Environment missing PYSAGE_CNXN setting. '
				+ 'Check for .env file looked here ??')

		return connection_string

	def _update_rools(self):

		## Grab the items data from sage db (odbc)
		self.items = self._items_table_update()

		##Todo-> Compare the id from the last sales created-on sage, with the local reference!
		#self.sales = self._sales_table_update()

		##Todo-> seend data to laravel on api
		self._send_to_server(str(json.dumps(self.items)))

		return True


	def _items_table_update(self):
		
		freshItemsData = self._sage_db_query(_all_items,'_temp_items.json', ['ShortDescription', 'ItemID', 'DtAlt'])
		freshItemsPricesData = self._sage_db_query(_all_items_price,'_temp_items_prices.json', ['TaxIncludedPrice', 'ItemID','PriceLineID'])
		#freshItemStockData = self._sage_db_query(_all_items_stock,'_temp_items_stock.json', ['ItemID','PhysicalUnits', 'PhysicalQty'])

		item_prices_json = {}
		for k, v in freshItemsPricesData['ItemID'].iteritems():
			k = int(k)
			v = str(v)
			if int(freshItemsPricesData['PriceLineID'][k]) == 1:
				item_prices_json[v] = str(freshItemsPricesData['TaxIncludedPrice'][k])


		item_json = {}
		for k, v in freshItemsData['ItemID'].iteritems():
			k = int(k)
			v = str(v)
			item_json[v] = { 'ShortDescription': str(freshItemsData['ShortDescription'][k].encode("UTF-8")),
							 'Price': item_prices_json[v],
							 'Created_at': freshItemsData['DtAlt'][k].strftime("%Y/%m/%d %H:%M")
						}


		#TODO-> save the information on a file
		self._save_json("_local/_Items.json",item_json)

		return item_json

	def _sales_table_update(self):

		#TODO-> develop a local verification (like cach) to prevente to loal all the times all data, 

		
		freshSalesData = self._sage_db_query(_all_sales,'_temp_sales.json', [u'TransSerial', u'CreateDate', 'TotalAmount','LineItemsCount'])
		freshSalesDetailsData = self._sage_db_query(_all_sales_details,'_temp_sales_details.json', [u'TransSerial' u'CreateDate', u'ItemID'])

		print freshSalesDetailsData
		
		sales_json = {}
		for k, v in freshSalesData['TransSerial'].iteritems():
			k = int(k)
			v = str(v)
			
			if v not in sales_json:
				sales_json[v] = []

			sales_json[v].append( {'CreateDate': freshSalesData['CreateDate'][k].strftime("%Y/%m/%d %H:%M"),
							'TotalAmount': str(freshSalesData['TotalAmount'][k]),
							'LineItemID': str(freshSalesData['LineItemID'][k])
							})

		#This has one relation with the above tableS
		sales_details_json = {}
		for k, v in freshSalesDetailsData['TransSerial'].iteritems():
			k = int(k)
			v = str(v)
			
			if v not in sales_details_json:
				sales_details_json[v] = []

			sales_details_json[v].append({'ItemID': freshSalesDetailsData['ItemID'][k],
											'Quantity': int(freshSalesDetailsData['Quantity'][k]),
											'CreateDate': freshSalesDetailsData['CreateDate'][k].strftime("%Y/%m/%d %H:%M")})


		organized_sales_json = 	{'_sales_details':sales_details_json, '_sales':sales_json}

		#TODO-> save the information on a file
		self._save_json("_local/_organized_sales.json",organized_sales_json)

		return organized_sales_json

	def _send_to_server(self,data):
		
		encoded = base64.b64encode(data)
		r = requests.post(self.serverAdreess,data={'data':encoded})
		print r.content


	def _save_json(self,fileName,jsonData):

		with open(fileName, "w") as write_file:
			json.dump(jsonData, write_file)

	def _save_file(self,fileString,dataToSave):
		#Save on File
		file = open(fileString, 'w')
		file.write(str(dataToSave))
		file.close()

	def _tables(self):

		for row in self.cursor.tables():
			self.tablesList = self.tablesList + row.table_name + "\n"

		#save on file
		self._save_file('table.txt', self.tablesList)

	def _debug_try(self):

		tableName = 'SaleTransactionLog' 

		#Define a query 
		query = "SELECT * FROM {}".format(tableName)


		#Execute the query
		self.cursor.execute(query)


		# Grab columns from the table and append it to the array
		columns = []

		#for row in self.cursor.columns():
			#products = products + row[1]
		#	if row[2] == tableName:
		#		columns.append(row.column_name)

		entry = []
		for w in self.cursor.fetchmany(self.cursor.rowcount):
			entry.append(w)

		print entry

	def _debug_sales_tables(self):

		tableName = 'SaleTransaction' 

		#Define a query 
		query = "SELECT * FROM {}".format(tableName)


		#Execute the query
		self.cursor.execute(query)


		# Grab columns from the table and append it to the array
		columns = []

		for row in self.cursor.columns():
			#products = products + row[1]
			if row[2] == tableName:
				columns.append(row.column_name)

		self._save_file('_debug/_salesTransaction_columns.txt',columns)	

		tableName = 'SaleTransactionDetails' 

		#Define a query 
		query = "SELECT * FROM {}".format(tableName)


		#Execute the query
		self.cursor.execute(query)


		# Grab columns from the table and append it to the array
		columns = []

		for row in self.cursor.columns():
			#products = products + row[1]
			if row[2] == tableName:
				columns.append(row.column_name)

		self._save_file('_debug/_salesTransactionDetails_columns.txt',columns)	

	def _debug_stock_tables(self):
		
		tableName = 'Stock'

		#Define a query 
		query = "SELECT * FROM {}".format(tableName)


		#Execute the query
		self.cursor.execute(query)


		# Grab columns from the table and append it to the array
		columns = []

		for row in self.cursor.columns():
			#products = products + row[1]
			if row[2] == tableName:
				columns.append(row.column_name)
	

		self._save_file('_debug/_stock_columns.txt',columns)

	def _sales(self):

		tableName = 'SaleTransaction' 

		#Define a query 
		query = "SELECT * FROM {}".format(tableName)


		#Execute the query
		self.cursor.execute(query)


		# Grab columns from the table and append it to the array
		columns = []

		for row in self.cursor.columns():
			#products = products + row[1]
			if row[2] == tableName:
				columns.append(row.column_name)

		#print list(columns)

		# Create the organized output of data.
		#entry = []
		#self.cursor.execute(query)

		#for w in self.cursor.fetchmany(self.cursor.rowcount):
			#entry = entry + columns[0] + ': ' + w[0] + '; ' + columns[2] + ': ' +  w[2] + "\n"

		#self._save_file('Items.txt', entry.encode('utf-8'))





classObject = Get_data()
#classObject._send_to_server()
#classObject._debug_try()
#classObject._sales()
#print(classObject._get_connection_string())

#Start processe on
classObject._constructor_()







