lista_informs = []
#Definition of the inform function that informs in console and log at the same time
def inform (message:str):
    print(message)
    lista_informs.append(message)
# Import of the needed librarys
from motordecalidad.utilities import *
dbutils = get_dbutils()
import json
from datetime import datetime
from typing import List
from pyspark.sql.types import StructType,StructField,StringType,BooleanType,DoubleType,LongType,DecimalType, IntegerType, DateType, ShortType, TimestampType, FloatType
from motordecalidad.constants import *
from motordecalidad.rules import *
from pyspark.sql.functions import upper

inform(f"---> Motor de Calidad Version {MotorVersion}")

# Main function, Invokes all the parameters from the json, Optionally filters, starts the rule validation
# Writes and returns the summary of the validation 
def startValidation(inputspark,config,dfltPath="",dfltDataDate="2000-01-01",dfltQuery=""):
    global spark
    global DefaultPath
    global DefaultQuery
    DefaultQuery = dfltQuery
    DefaultPath = dfltPath
    spark = inputspark
    inform("########################################################################################")
    inform("##############################Inicio de validación######################################")
    inform("########################################################################################")
    requisitesData = [0,Rules.Pre_Requisites.code,Rules.Pre_Requisites.name,Rules.Pre_Requisites.property,Rules.Pre_Requisites.code + "/","100",PreRequisitesSucessMsg,"","100.00",0]
    object,output,country,project,entity,domain,subDomain,segment,area,rules,error,filtered,dataDate,validData,finalData, email = extractParamsFromJson(config)
    if Rules.Pre_Requisites.code in rules:
        inform("---> Inicializando regla de requisitos")
        columns = rules[Rules.Pre_Requisites.code].get(JsonParts.Fields)
        t = datetime.now()
        requisitesData = validateRequisites(object,columns)
        rdd = spark.sparkContext.parallelize([requisitesData])
        inform("---> Tiempo de ejecución: %s segundos" % (datetime.now() - t))
        inform(f"---> Fecha de ejecución {t}")
    filteredObject = applyFilter(object,filtered)
    registerAmount = filteredObject.count()
    validationData = validateRules(filteredObject,rules,registerAmount,entity,project,country,domain,subDomain,segment,area,error,dataDate,validData,finalData, email,output,dfltDataDate)
    writeDf(validationData, output)
    inform(f"---> Los resultados se pueden verificar en la ruta:{output.get(JsonParts.Path)}")
    return validationData



# Function that extracts the information from de JSON File
def extractParamsFromJson(config):
    try:
        file = open(config)
        data = json.load(file)
        inform("---> Se abre el json")
        input = data.get(JsonParts.Input)
        output = data.get(JsonParts.Output)
        country:str = input.get(JsonParts.Country)
        project:str = input.get(JsonParts.Project)
        entity:str = input.get(JsonParts.Entity)
        domain: str = input.get(JsonParts.Domain)
        subDomain: str = input.get(JsonParts.SubDomain)
        segment: str = input.get(JsonParts.Segment)
        area: str = input.get(JsonParts.Area)
        validData: str = input.get(JsonParts.ValidData)
        dataDate: str = input.get(JsonParts.DataDate)
        error = data.get(JsonParts.Error)
        filtered = data.get(JsonParts.Filter)
        finalData = data.get(JsonParts.Data)
        email = input.get(JsonParts.Email)
    except:
        inform("---> Error en el Json")
        raise Exception("Error en el Json")
    try:
        entityDf = readDf(input)
    except:
        inform("---> Error en la lectura del fichero")
        raise Exception("Error de la lectura del fichero")
    rules = data.get(JsonParts.Rules)
    inform("---> Extraccion de JSON completada")
    return entityDf,output,country,project,entity,domain,subDomain,segment,area,rules,error,filtered,dataDate,validData,finalData,email

# Function that reads the input File
def readDf(input):
    inform("---> Inicio de lectura de informacion")
    spark.conf.set(input.get(JsonParts.Account),input.get(JsonParts.Key)) 
    header = input.get(JsonParts.Header)
    if DefaultPath == "" : 
        return spark.read.option(Delimiter,input.get(JsonParts.Delimiter)).option(Header,header).csv(input.get(JsonParts.Path))
    else:
        return spark.read.option(Delimiter,input.get(JsonParts.Delimiter)).option(Header,header).csv(DefaultPath)

# Function that writes the output dataframe with the overwrite method
def writeDf(object:DataFrame,output):
    type = output.get(JsonParts.Type)
    spark.conf.set(output.get(JsonParts.Account),output.get(JsonParts.Key)) 
    header:bool = output.get(JsonParts.Header)
    partitions:List = output.get(JsonParts.Partitions)
    try:
        if len(partitions) > Zero :
            object.coalesce(One).write.partitionBy(*partitions).mode(Overwrite).option(PartitionOverwriteMode, DynamicMode).option(Delimiter,str(output.get(JsonParts.Delimiter))).option(Header,header).format(DatabricksCsv).save(str(output.get(JsonParts.Path)))
        else:
            object.coalesce(One).write.mode(Overwrite).option(Delimiter,str(output.get(JsonParts.Delimiter))).option(Header,header).format(DatabricksCsv).save(str(output.get(JsonParts.Path)))
    except:
        object.coalesce(One).write.mode(Overwrite).option(Delimiter,str(output.get(JsonParts.Delimiter))).option(Header,header).format(DatabricksCsv).save(str(output.get(JsonParts.Path)))
    inform("---> Se escribio en el blob")

#Function that creates the error DataFrame using the correct Data Types
def createErrorData(object:DataFrame) :
    columnsList = object.columns
    columnsTypes: List = []
    for dt in object.dtypes:
        if dt[One] == 'string':
            columnsTypes.append(StringType())
        elif dt[One] == 'boolean':
            columnsTypes.append(BooleanType())
        elif dt[One] == 'double':
            columnsTypes.append(DoubleType())
        elif dt[One] == 'bigint':
            columnsTypes.append(LongType())
        elif dt[One][0:7] == 'decimal':
            columnsTypes.append(DecimalType(int(dt[One].split("(")[1].split(",")[0]),int(dt[One].split("(")[1].split(",")[1].split(")")[0])))
        elif dt[One] == 'int' :
            columnsTypes.append(IntegerType())
        elif dt[One] == 'date':
            columnsTypes.append(DateType())
        elif dt[One] == 'smallint' :
            columnsTypes.append(ShortType())
        elif dt[One] == "timestamp" :
            columnsTypes.append(TimestampType())
        elif dt[One] == "float" :
            columnsTypes.append(FloatType())
    columnsTypes.extend([StringType(),StringType()])
    columnsList.extend(["error","run_time"])
    schema = StructType(
        list(map(lambda x,y: StructField(x,y),columnsList,columnsTypes))
        )
    return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

#Function that validate rules going through the defined options
def validateRules(object:DataFrame,rules:dict,registerAmount:int, entity: str, project:str,country: str,domain: str,subDomain: str,segment: str,area: str,error,dataDate:str,validData:str, Data,email,output,dfltDataDate):
    runTime = datetime.now()
    errorData:DataFrame = createErrorData(object)
    rulesData:List = []
    rulesNumber = 0
    for code in rules:
        if rules[code].get(JsonParts.Fields) not in [0,["0"],"0"] :
            rulesNumber = rulesNumber + 1
            if code[0:3] == Rules.NullRule.code:
                inform("---> Inicializando reglas de Nulos")
                data:List = []
                columns = rules[code].get(JsonParts.Fields)
                threshold:int = rules[code].get(JsonParts.Threshold)
                write = rules[code].get(JsonParts.Write)
                if columns[0] == "*" :
                    for field in object.columns:
                        t = datetime.now()
                        data, errorDf = validateNull(object,field,registerAmount,entity,threshold)
                        errorDesc = "Nulos - " + str(field)
                        if data[-One] > Zero :
                            errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                            .withColumn("run_time", lit(runTime))
                            if write != "FALSE" :
                                errorData = errorData.union(errorTotal)
                        rulesData.append(data)
                        inform("---> Tiempo de ejecución: %s segundos" % (datetime.now() - t))
                        inform(f"---> Fecha de ejecución{t}")
                else:
                    for field in columns:
                        t = datetime.now()
                        data, errorDf = validateNull(object,field,registerAmount,entity,threshold)
                        errorDesc = "Nulos - " + str(field)
                        if data[-One] > Zero :
                            errorTotal = errorDf.withColumn("error", lit(errorDesc))\
                            .withColumn("run_time", lit(runTime))
                            if write != "FALSE" :
                                errorData = errorData.union(errorTotal)
                        rulesData.append(data)
                        inform("---> Tiempo de ejecución: %s segundos" % (datetime.now() - t))
                        inform(f"---> Fecha de ejecución{t}")
    if errorData.count() > Zero:
        writeDf(errorData,error)
    inform(f"Se finaliza exitosamente la ejecución de calidad sobre el fichero {entity} con fecha de información {dataDateWidget}")
    informs_str = "\n".join(lista_informs)
    validationData:DataFrame = spark.createDataFrame(data = rulesData, schema = OutputDataFrameColumns)
    return validationData.select(
        DataDate.value(lit(dataDate)),
        CountryId.value(lit(country.upper())),
        Project.value(lit(project.upper())),
        Entity.value(lit(entity.upper())),
        TestedFields.value(upper(TestedFields.column)),
        Domain.value(lit(domain.upper())),
        SubDomain.value(lit(subDomain.upper())),
        Segment.value(lit(segment.upper())),
        Area.value(lit(area.upper())),
        AuditDate.value(lit(datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))),
        FunctionCode.column,
        RuleCode.column,
        DataRequirement.column,
        Threshold.column,
        RuleGroup.column,
        RuleProperty.column,
        TestedRegisterAmount.column,
        PassedRegistersAmount.value(TestedRegisterAmount.column - FailedRegistersAmount.column),
        SucessRate.column,
        FailedRegistersAmount.column,
        FailRate.value(lit(OneHundred)-SucessRate.column),
        LibraryVersion.value(lit(f"{MotorVersion}"))
        )