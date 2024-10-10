from constants.CONSTANT import CONSTANT
from models.Mapper.MapperBean import MapperBean
from modules.DynamicPrinter import dynamicPrinter


import traceback

class Mapper:

    def __init__(self,json):
        self.json = json #json received to map
        self.predefinedSchema = json[CONSTANT.SERVICE.PARAMETERS.DATABASE_INFO][CONSTANT.SERVICE.PARAMETERS.PREDEFINED_SCHEMA] # table structure provided by user,
        self.tableInfo = json[CONSTANT.SERVICE.PARAMETERS.TABLE_INFO]
        self.source = self.predefinedSchema
        self.target = None
        self.responseJSON = None

    def datatype_mapping(self,sql_database):
        self.target = sql_database # getDatabaseName(sql_database)
        self.responseJSON = self.tableInfo

        if self.source == self.target:
            return self.tableInfo

        tblInfo = self.type_map(self.tableInfo)
        self.responseJSON = tblInfo

        return self.responseJSON


    # mapping the types from source database to target database
    def type_map(self,tableInfo):

        for table in tableInfo:
            columns = table[CONSTANT.TABLE.COLUMNS]
             # indexes = table['indexes']

            for column in columns:
                datatype = '' + str(self.short_term_mapping(column[CONSTANT.TABLE.DATA_TYPE].upper()))
                length = column[CONSTANT.TABLE.LENGTH]

                try:

                    datatypes = self.getDatatypesForTargetDatabase(self.source,self.target,datatype)
                    if datatypes != None:
                        column[CONSTANT.TABLE.DATA_TYPE] = datatypes[0]
                        other_supported_target_datatype = datatypes

                        datatype_info = self.getDatatypeFamily(self.target,datatype)

                        column[CONSTANT.TABLE.DATA_TYPE] = self.processedDataType(datatype,length,column[CONSTANT.TABLE.DATA_TYPE] ,datatype_info,other_supported_target_datatype)
                except Exception as e:
                    dynamicPrinter(f'Exception:: {e}\nException message:  ')
                    traceback.print_exc()
                # dynamicPrinter('***********')
        # print(tableInfo)
        return tableInfo

    def processedDataType(self,source_datatype,source_datatype_length,target_datatype,target_datatype_info,other_supported_target_datatypes):

        source_datatype = source_datatype
        source_datatype_length = source_datatype_length

        target_datatype = target_datatype
        target_datatype_info = target_datatype_info
        other_supported_target_datatypes = other_supported_target_datatypes

        source_datatype_family = self.getDatatypeFamily(self.source,source_datatype)#self.get_family(source_datatype)
        target_datatype_family = target_datatype_info #[CONSTANT.SERVICE.MAPPER.FAMILY]

        # if the datatype families are same for both the datatypes
        if source_datatype_family == target_datatype_family:
            # if the source length is given
            if source_datatype_length != None:
                    target_datatype = f'{target_datatype}'
                    dynamicPrinter(f'Target Priority: {self.getDatatypePriority(self.target,target_datatype)} |  Datatype: {self.target}\n{target_datatype}')
            elif source_datatype_length == None:

                # if the priority of datatypes source is less or equal to the priority of target datatype

                if self.getDatatypePriority(self.source,source_datatype) <= self.getDatatypePriority(self.target,target_datatype):

                        target_datatype = f'{target_datatype}'
                        dynamicPrinter(f'Target Priority: {self.getDatatypePriority(self.target,target_datatype)} |  Datatype: {self.source}\n{target_datatype}')

                # if the priority of datatypes source is greater than the priority of target datatype
                elif self.getDatatypePriority(self.source,source_datatype) > self.getDatatypePriority(self.target,target_datatype):
                    # check other support datatype and find one(datatype) with the same or greater priority
                    for datatype in other_supported_target_datatypes:
                        dynamicPrinter('Checking Alternates...')
                        if self.getDatatypePriority(self.source,datatype) >= self.getDatatypePriority(self.target,target_datatype):
                            dynamicPrinter(f'new target prioirty: {self.getDatatypePriority(self.target,target_datatype)} => {datatype}')

                            target_datatype = f'{datatype}'
                            dynamicPrinter(f'Target Priority: {self.getDatatypePriority(self.target,datatype)} |  Datatype: {datatype}\n{datatype}')
                            break

        else:
            dynamicPrinter("Mapper.py | Different Datatype Family Mapping")

        return target_datatype



    def getDatatypesForTargetDatabase(self,source_db,target_db,datatype):

        mapper = MapperBean()
        dt = mapper.getTargetDatatype(source_db,target_db,datatype)

        if dt == None or dt == []:
            return
        return dt


    def getDatatypeFamily(self,database,datatype):
        mapper = MapperBean()
        family = mapper.getDatatypeFamily(database,datatype)
        return family

    def getDatatypePriority(self,database_id,datatype):
        # datatype_info[self.source][source_datatype][CONSTANT.SERVICE.MAPPER.PRIORITY]
        mapper = MapperBean()
        priority = mapper.getDatatypePriority(database_id,datatype)
        return priority

    def short_term_mapping(self,datatype):
        if datatype == CONSTANT.MAPPER.DATATYPE.CHARACTER_VARYING or datatype == 'character varying':
            return CONSTANT.MAPPER.DATATYPE.VARCHAR
        elif  datatype == CONSTANT.MAPPER.DATATYPE.CHARACTER:
            return CONSTANT.MAPPER.DATATYPE.CHAR
        else:
            return datatype