from MigrationHandler import MigrationHandler
from ConsoleHandler import ConsoleHandler
from StaticDataHandler import StaticDataHandler
import sys
import datetime
from migrations import *
import importlib
from Database import Database

reload(sys)  
sys.setdefaultencoding('utf8')

class Migrations(object):
	def __init__(self):
		self.database_connection = None
		self.database_connection_old = None
		self.database_connection_backup = None
		self.console_handler = ConsoleHandler()
		self.migration_handler = MigrationHandler()
		self.static_data_handler = StaticDataHandler()

	def getDatabaseConnection(self,old=False):
		if not old:
			if not self.database_connection:
				self.database_connection = self.console_handler.connectToDatabase("Ingrese los datos de conexion a la base de datos V2","365DBV2")
				self.database_connection_backup = Database(self.database_connection.connection_string,autocommit=True)
				self.database_connection_backup.connect()
				self.console_handler.backupDatabase(self.database_connection_backup)
			return self.database_connection
		else:
			if not self.database_connection_old:
				self.database_connection_old = self.console_handler.connectToDatabase("Ingrese los datos de conexion a la base de datos V1","365DB")
			return self.database_connection_old
		

	def createMigrationFile(self):
		migration_name = raw_input("Ingrese el nombre de la nueva migracion: ")
		if migration_name:
			migration_name_fixed = migration_name.lower().replace(" ","_")
			if self.console_handler.yesOrNot("El nombre de la seleccionado es '%s', desea continuar?"%(migration_name_fixed)):
				self.migration_handler.createMigration(migration_name_fixed)
			return True
		else:
			print 'Debe ingresar un nombre para la migracion'
			return False


	def createStaticDataFile(self):
		migration_name = raw_input("Ingrese el nombre de la nueva tabla para data estatica: ")
		if migration_name:
			if self.console_handler.yesOrNot("El nombre de la seleccionado es '%s', desea continuar?"%(migration_name)):
				self.static_data_handler.createStaticData(migration_name)
			return True
		else:
			print 'El nombre de la tabla no puede ser vacio'
			return False

	def migrateStaticDataReverse(self):
		migration_name = raw_input("Ingrese el nombre de la tabla:")
		if migration_name:
			if self.console_handler.yesOrNot("El nombre de la seleccionado es '%s', desea continuar?"%(migration_name)):
				self.static_data_handler.reverseCreateStaticData(migration_name,self.database_connection)
			return True
		else:
			return False
      
	def migrateStaticData(self,combined_table=False):
		if self.static_data_handler.getAllStaticDataFiles():
			if not combined_table:
				combined_table = self.console_handler.yesOrNot('Desea borrar y reinsertar data estatica en tablas donde pueden haber agregado datos adicionales. Ejemplo: t365_Eventos')
			for each_file in self.static_data_handler.valid_static_data_files:
				module = importlib.import_module("static_data."+each_file['name'].split(".")[0])
				migration_obj = getattr(module,each_file['class'])(self.database_connection)
				print migration_obj.__class__.__name__
				if (migration_obj.combined_table == True and combined_table == True) or (migration_obj.combined_table == False):
					migration_obj.runAddStaticData()
			return True
		else:
			print 'No hay archivos de migracion de data estatica'
			return False

	def migrateMigrations(self,end=None):
		last_migration = self.migration_handler.getLastMigration(self.database_connection)
		if type(last_migration) == dict:
			if self.migration_handler.getAllMigrationsFiles(last_migration):
				for each_file in self.migration_handler.valid_files:
					module = importlib.import_module("migrations."+each_file['name'].split(".")[0])
					migration_obj = getattr(module,each_file['class'])(each_file["date"],each_file["name"],self.database_connection)
					if not migration_obj.runMigration() or end == each_file["name"]:
						break
			return True
		else:
			print 'Error al buscar la ultima migracion'
			return False



if __name__ == "__main__":
	Migration = Migrations()
	Console_Handler = Migration.console_handler
	while True:
		if Migration.getDatabaseConnection():
			if not Console_Handler.option_selected:
				Console_Handler.printOptions()
				Console_Handler.setOption()
			if Console_Handler.option_selected == 9:
				Console_Handler.returnMainMenu(Migration.createStaticDataFile) 
			elif Console_Handler.option_selected == 10:
				Console_Handler.returnMainMenu(Migration.createMigrationFile)
			elif Console_Handler.option_selected == 1:
				Console_Handler.returnMainMenu(Migration.migrateMigrations)
			elif Console_Handler.option_selected == 2:
				Console_Handler.returnMainMenu(Migration.migrateStaticData)
			elif Console_Handler.option_selected == 8:
				Console_Handler.returnMainMenu(Migration.migrateStaticDataReverse)

