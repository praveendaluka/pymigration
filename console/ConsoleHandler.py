
# -*- coding: utf-8 -*-  
from database.Database import Database
import traceback

class ConsoleHandler(object):
	def __init__(self):
		self.option_selected = None


	def printOptions(self):
		print 'Indique una de las siguientes opciones: '
		print '1 - Migracion automatica segun tabla migrations'
		print '2 - Actualizar data estatica'
		print '3 - Migrar BDV1 a BDV2'
		print '8 - Crear data estatica REVERSE'
		print '9 - Crear data estatica'
		print '10 - Crear Migracion'


	def setOption(self):
		self.option_selected = raw_input("Seleccione una opcion: ")
		self.option_selected = int(str(self.option_selected).strip())

	def backupDatabase(self,database_connection):
		selected_result = raw_input('Desea hacer un respaldo de la base de datos? (S/N)')
		if selected_result == 'S':
			print 'Estamos haciendo un respaldo, espere que la operacion puede tardar varios minutos'
			database_connection.backupDatabase()
			return True
		else:
			return False


	def returnMainMenu(self,callback):
		value = callback()
		if value:
			self.option_selected = None
		else:
			selected_result = raw_input('Desea volver al menu o reintentar? (V/R)')
			if selected_result == 'V':
				self.option_selected = None

	def connectToDatabase(self,msg,database_name,autocommit=False):
		try:
			print msg
			db_server = raw_input("Ingrese servidor: ")
			db_user = raw_input("Ingrese usuario: ")
			db_password = raw_input("Ingrese clave: ")
			connection_string = 'DRIVER={SQL Server Native Client 11.0};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s'%(db_server,database_name,db_user,db_password)
			database = Database(connection_string,autocommit=autocommit)
			if database.connect():
				print 'Conexion establecida con la base de datos %s'%(database_name)
				return database
			else:
				print 'No se pudo establecer conexion con la base de datos %s'%(database_name)
				return False
		except:
			print 'No se pudo establecer conexion con la base de datos %s'%(database_name)
			traceback.print_exc()
			return False

	def yesOrNot(self,msg):
		while True:
			opcion_error = raw_input(msg+". (S/N)").lower()
			if opcion_error == "s":
				return True
			elif opcion_error == "n":
				return False
			else:
				continue

	def retryOrBack(self,msg):
		while True:
			opcion_error = raw_input(msg+". Desea reintentar o volver atras (R/V)").lower()
			if opcion_error == "r":
				return True
			elif opcion_error == "v":
				return False
			else:
				continue

 






