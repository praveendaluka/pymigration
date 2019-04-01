
# -*- coding: utf-8 -*-  
import traceback
import datetime
import os

class MigrationHandler(object):


	def existMigrtionTable(self,database_connection):
		result_migration_database = database_connection.select("""SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 't365_Migrations'""")
		if result_migration_database:
			return True
		else:
			return False 



	def getLastMigration(self,database_connection):
		try:
			if not self.existMigrtionTable(database_connection):
				self.createMigrationsTable(database_connection)
			last_migration = database_connection.select("""SELECT TOP 1 * FROM t365_Migrations ORDER BY id DESC""")
			if last_migration:
				last_migration_file = last_migration[0].migration_file
				last_migration_file_date = last_migration[0].migration_file_date
				return {"name":last_migration_file,"date":last_migration_file_date}
			else:
				return {}
		except:
			traceback.print_exc()
			return False

	def getNameAndDateByFile(self,file):
		file_split = file.split('_',6)
		file_date = datetime.datetime(year=int(file_split[0]),month=int(file_split[1]),day=int(file_split[2]),hour=int(file_split[3]),minute=int(file_split[4]),second=int(file_split[5]))
		file_name = file
		class_name = file_split[6].split(".")[0].upper()
		return {"name":file_name,"date":file_date,"class":class_name}

	def getAllMigrationsFiles(self,last_migration=None):
		self.valid_files = []
		files = os.listdir('./migrations/')
		if files:
			files.sort()
			for name in files:
				name_split = name.split('.')
				if name_split[1] == 'py' and name != '__init__.py':
					file_data_dict = self.getNameAndDateByFile(name)
					if file_data_dict:
						if last_migration:
							if file_data_dict['date'] > last_migration['date']:
								self.valid_files.append(file_data_dict)
						else:
							self.valid_files.append(file_data_dict)
			if self.valid_files:
				return True
			else:
				return False
		else:
			return False

	def createMigrationsTable(self,database_connection):
		try:
			if not database_connection.select("""SELECT * from sysobjects where name='t365_Migrations' and xtype='U' """):		
				database_connection.insert("""CREATE TABLE [dbo].[t365_Migrations](
										[id] [int] IDENTITY(1,1) NOT NULL,
										[migration_file] [nvarchar](250) NOT NULL,
										[migration_file_date] [datetime] NOT NULL,
										[migration_run_date] [datetime] NOT NULL,
									CONSTRAINT [PK_t365_Migrations] PRIMARY KEY CLUSTERED 
									(
										[id] ASC
									)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
									) ON [PRIMARY]
								""")
				database_connection.commit()
				print "No existia la tabla migrations, ya fue creada."
				return True
			return True
		except:
			traceback.print_exc()
			database_connection.rollback()
			return False

	def createMigration(self,name):
		now = datetime.datetime.now()
		now_str_format = now.strftime("%Y_%m_%d_%H_%M_%S_")
		new_migration_file = open("./migrations/"+now_str_format+name+".py","w+")
		new_migration_file.write("# -*- coding: utf-8 -*-\n")
		new_migration_file.write("from BaseMigration import BaseMigration\n")
		new_migration_file.write("import traceback\n\n")
		new_migration_file.write("class %s(BaseMigration):\n\n"%name.upper())
		new_migration_file.write("	def __init__(self,file_datetime,file_name,database_connection,database_connection_old=None):\n")
		new_migration_file.write("		BaseMigration.__init__(self,file_datetime,file_name,database_connection,database_connection_old)\n\n")
		new_migration_file.write("	def preMigration(self):\n")
		new_migration_file.write("		try:\n")
		new_migration_file.write("			return True\n")
		new_migration_file.write("		except:\n")
		new_migration_file.write("			traceback.print_exc()\n")
		new_migration_file.write("			return False\n\n")
		new_migration_file.write("	def migration(self):\n")
		new_migration_file.write("		try:\n")
		new_migration_file.write("			return True\n")
		new_migration_file.write("		except:\n")
		new_migration_file.write("			traceback.print_exc()\n")
		new_migration_file.write("			return False\n\n")		
		new_migration_file.write("	def postMigration(self):\n")
		new_migration_file.write("		try:\n")
		new_migration_file.write("			return True\n")
		new_migration_file.write("		except:\n")
		new_migration_file.write("			traceback.print_exc()\n")
		new_migration_file.write("			return False\n\n")		
		new_migration_file.close()










