
# -*- coding: utf-8 -*-  
import traceback
import datetime
import os

class StaticDataHandler(object):

	def getAllStaticDataFiles(self):
		self.valid_static_data_files = []
		files = os.listdir('./static_data/')
		if files:
			for name in files:
				name_split = name.split('.')
				if name_split[1] == 'py' and name != '__init__.py':
					file_data_dict = {'name':name,'class':name_split[0]}
					if file_data_dict:
						self.valid_static_data_files.append(file_data_dict)		
			if self.valid_static_data_files:
				return True
			else:
				return False
		else:
			return False

	def createStaticData(self,name,all_lines_to_add_data=False):
		new_migration_file = open("./static_data/"+name+".py","w+")
		new_migration_file.write("# -*- coding: utf-8 -*-\n")
		new_migration_file.write("from BaseStaticData import BaseStaticData\n")
		new_migration_file.write("import traceback\n")
		new_migration_file.write("import datetime\n\n")
		new_migration_file.write("class %s(BaseStaticData):\n\n"%name)
		new_migration_file.write("	def __init__(self,database_connection,combined_table=True):\n")
		new_migration_file.write("		BaseStaticData.__init__(self,database_connection,combined_table)\n\n")
		new_migration_file.write("	def dataToInsert(self):\n")
		new_migration_file.write("		try:\n")
		if not all_lines_to_add_data:
			new_migration_file.write("			self.addData(id=1, data='anydata')\n")
		else:
			for each_data in all_lines_to_add_data:
				new_migration_file.write("			"+each_data+")\n".encode('utf8'))
		new_migration_file.write("			return True\n")
		new_migration_file.write("		except:\n")
		new_migration_file.write("			traceback.print_exc()\n")
		new_migration_file.write("			return False\n\n")	
		new_migration_file.close()

	def formatDataColumReverseFile(self,table,column,data,database_connection):

		if data != None:
			data_type = database_connection.select("""SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s'"""%(table,column))

			if data_type:
				if data_type[0].DATA_TYPE in ('int','tinyint','smallint','bigint','float','decimal'):
					return str(data)
				elif data_type[0].DATA_TYPE in ('varchar','nvarchar','char','text','nchar'):
					return "'"+data.replace("'","\\'")+"'"
				elif data_type[0].DATA_TYPE == 'bit':
					return str(data)
				elif data_type[0].DATA_TYPE in ('datetime','smalldatetime'):
					return "datetime.datetime(year=%s,month=%s,day=%s,hour=%s,minute=%s,second=%s)"%(data.year,data.month,data.day,data.hour,data.minute,data.second)
				elif data_type[0].DATA_TYPE == 'date':
					return "datetime.date(year=%s,month=%s,day=%s)"%(data.year,data.month,data.day)
				elif data_type[0].DATA_TYPE == 'time':
					return "datetime.time(hour=%s,minute=%s,second=%s)"%(data.hour,data.minute,data.second)					
				else:
					print 'TIPO DE DATO NO VALIDO'
					print data_type
					return str(data)

		else:
			return str(data)
			
	def createLinesToAddReverseFile(self,table_name,columns,data,database_connection):
		all_lines_to_add_data = []
		for each_row in data:
			texto = "self.addData("
			for index, each_colum in enumerate(columns):
				data_fixed = self.formatDataColumReverseFile(table_name,str(each_colum),each_row[index],database_connection)
				texto = texto + str(each_colum).replace("'","") + "=" +data_fixed+","
			all_lines_to_add_data.append(texto)
		return all_lines_to_add_data

	def reverseCreateStaticData(self,table_name,database_connection):
		try:
			all_data_in_table = database_connection.select("""SELECT * FROM %s"""%(table_name))
			if all_data_in_table:
				columns = [column[0] for column in database_connection.cursor.description]
				all_lines_to_add_data = self.createLinesToAddReverseFile(table_name,columns,all_data_in_table,database_connection)

				self.createStaticData(table_name,all_lines_to_add_data)
			else:
				return {}
		except:
			traceback.print_exc()
			return False










