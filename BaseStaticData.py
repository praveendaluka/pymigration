# -*- coding: utf-8 -*-

import traceback
import time 

class BaseStaticData(object):
	def __init__(self,database_connection,combined_table):
		self.database_connection = database_connection
		self.combined_table = combined_table
		self.all_data_to_insert  = []
		self.table_columns = {}
		self.have_identity = False
		self.table_name =  self.__class__.__name__

	def addData(self,**kwars):
		self.all_data_to_insert.append({k: (v.decode('utf-8') if isinstance(v, str) else v) for k, v in kwars.items()})

	def truncateTable(self):
		try:
			self.database_connection.insert("""TRUNCATE TABLE %s"""%(self.table_name))
			return True
		except:
			traceback.print_exc()
			return False

	def dataToInsert(self):
		return self.all_data_to_insert

	def selectTableColumns(self):
		
		table_columns_result = self.database_connection.select("""SELECT DATA_TYPE, COLUMN_NAME, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'"""%(self.__class__.__name__))
		for column in table_columns_result:
			self.table_columns[column.COLUMN_NAME] = {"data_type":column.DATA_TYPE,"is_nullable":column.IS_NULLABLE,"is_identity":False}
		identity_column = self.database_connection.select("""SELECT SYSCOLUMNS.NAME FROM SYSOBJECTS INNER JOIN SYSCOLUMNS ON SYSOBJECTS.ID = SYSCOLUMNS.ID WHERE COLUMNPROPERTY(SYSOBJECTS.ID, SYSCOLUMNS.NAME, 'isIdentity') = 1 AND OBJECTPROPERTY(SYSOBJECTS.ID, 'isTable') = 1 AND SYSOBJECTS.NAME = '%s'"""%(self.__class__.__name__))
		if identity_column:
			self.have_identity = True
			self.table_columns[identity_column[0].NAME]["is_identity"] == True


	def validateData(self,each_row):
		keys = []
		values = []
		values_for_sql = "("
		for each_column_key in self.table_columns.keys():
			if each_column_key not in each_row:
				if self.table_columns[each_column_key]['is_nullable'] == False:
					return False
			else:
				values_for_sql = values_for_sql + "?,"
				keys.append(str(each_column_key))
				values.append(each_row[each_column_key])

		values_for_sql = values_for_sql[:-1]+")"
		return {'keys':keys,'values':values,'values_for_sql':values_for_sql}


	def insertData(self):
		try:
			if self.have_identity:
				self.database_connection.insert("""SET IDENTITY_INSERT %s ON;"""%(self.table_name))
			for each_row in self.all_data_to_insert:
				validation_result = self.validateData(each_row)				
				if validation_result:
					query = "INSERT INTO %s %s VALUES %s"%(self.table_name,str(tuple(validation_result['keys'])).replace("'",""),validation_result['values_for_sql'])
					self.database_connection.insert(query,*validation_result['values'])				
			if self.have_identity:
				self.database_connection.insert("""SET IDENTITY_INSERT %s OFF;"""%(self.table_name))
			return True
		except:
			traceback.print_exc()
			return False

	def runAddStaticData(self):
		try:
			self.selectTableColumns()
			if self.dataToInsert():
				if self.all_data_to_insert:
					if self.truncateTable():
						if self.insertData():
							self.database_connection.commit()
							return True
			print "Migracion de data estatica fallida"
			self.database_connection.rollback()
			return False
		except:
			traceback.print_exc()
			return False
