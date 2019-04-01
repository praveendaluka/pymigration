import pyodbc
import traceback
import datetime

class Database(object):
	def __init__(self,connection_string,autocommit=False):
		self.resultado = None
		self.connection_string = connection_string
		self.autocommit = autocommit
	def connect(self):
		try:
			self.cnxn = pyodbc.connect(self.connection_string,autocommit=self.autocommit)
			self.cursor = self.cnxn.cursor()
			return True
		except:
			return False
	def select(self,query,*datos):
		self.cursor.execute(query,datos)
		rows = self.cursor.fetchall()
		self.resultado = rows
		return self.resultado
	def selectOne(self,query,*dato):
		self.cursor.execute(query,dato)
		row = self.cursor.fetchone()
		self.resultado = row
		return self.resultado
	def insert(self,query,*datos):
		self.cursor.execute(query,datos)

	def update(self,query,*datos):
		self.cursor.execute(query,datos)

	def backupDatabase(self):
		try:
			now = datetime.datetime.now()
			now_str_format = now.strftime("%Y_%m_%d_%H_%M_%S_")
			backup_query = """BACKUP DATABASE [365DBV2]  
								TO DISK = '%s.Bak'  
								WITH COMPRESSION"""%(now_str_format)
			self.insert(backup_query)
			print 'Respaldo de la base de datos finalizado con el nombre %s.bak'%(now_str_format)
			return True
		except:
			print 'No se pudo realizar el respaldo de la base de datos'
			traceback.print_exc()
			return False

	def delete(self,query,dato):
		self.cursor.execute(query,dato)

	def commit(self):
		print 'Commit database changes'
		self.cnxn.commit()
	def rollback(self):
		print 'Rollback Database'
		self.cnxn.rollback()

	def currentLastId(self):
		return self.selectOne("SELECT @@Identity")
