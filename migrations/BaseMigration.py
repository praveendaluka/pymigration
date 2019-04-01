
# -*- coding: utf-8 -*-  
import traceback
import time
import datetime

class BaseMigration(object):

	def __init__(self,file_datetime,file_name,database_connection,database_connection_old=None):
		self.file_datetime = file_datetime
		self.file_name = file_name
		self.database_connection = database_connection
		self.database_connection_old = database_connection_old

	def updateMigrationTable(self):
		try:
			self.database_connection.update("INSERT INTO t365_Migrations (migration_file,migration_file_date,migration_run_date) VALUES (?,?,?)",self.file_name,self.file_datetime,datetime.datetime.now())
			return True
		except:
			traceback.print_exc()
			return False
	def preMigration(self):
		return True

	def postMigration(self):
		return True

	def migration(self):
		return True

	def runMigration(self):
		print "Inicializando migracion de "+str(self.file_name)
		if self.preMigration():
			if self.migration():
				if self.postMigration():
					if self.updateMigrationTable():
						self.database_connection.commit()
						return True
		print "Migracion Fallida de "+str(self.file_name)
		self.database_connection.rollback()
		time.sleep(1)
		return False



   




