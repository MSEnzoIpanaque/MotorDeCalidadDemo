from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import (collect_list, concat_ws, current_date,
                                   datediff, expr, length, lit, mean,
                                   regexp_replace, split, stddev, sum, to_date,
                                   to_timestamp)
from pyspark.sql.window import Window
from motordecalidad.constants import *
from motordecalidad.utilities import *
from motordecalidad.functions import inform

#Function that validates if the DataFrame has data and has the required columns
def validateRequisites(object:DataFrame, field:list):
    fields = ','.join(field)
    error_list = list(set(field) - set(object.columns))
    rowsNumber = object.count()
    if len(error_list) == Zero and rowsNumber != Zero :
        inform("Ha superado la validación de requisitos exitosamente.\n")
        return (rowsNumber,Rules.Pre_Requisites.code,Rules.Pre_Requisites.name,Rules.Pre_Requisites.property,Rules.Pre_Requisites.code + "/" + fields,"100",PreRequisitesSucessMsg,fields,"100.00",0)
    elif len(error_list) != Zero:
        inform(f"Falta columna o la columna tiene un nombre distinto. Por favor chequear que el input tiene un esquema válido: {','.join(error_list)}")
        return (rowsNumber,Rules.Pre_Requisites.code,Rules.Pre_Requisites.name,Rules.Pre_Requisites.property,Rules.Pre_Requisites.code + "/" + fields,"100",f"Error en esquema de la tabla, revisar los siguientes campos: {','.join(error_list)}",fields,"0.00",rowsNumber)
    elif rowsNumber == Zero :
        inform("El dataframe de entrada no contiene registros")
        return (rowsNumber,Rules.Pre_Requisites.code,Rules.Pre_Requisites.name,Rules.Pre_Requisites.property,Rules.Pre_Requisites.code + "/" + fields,"100","DataFrame no contiene Registros",fields,"0.00",rowsNumber)

#Function that valides the amount of Null registers for certain columns of the dataframe
def validateNull(object:DataFrame,field: str,registersAmount: int,entity: str,threshold):
    dataRequirement = f"El atributo {entity}.{field} debe ser obligatorio (NOT NULL)."
    errorDf = object.filter(col(field).isNull())
    nullCount = object.select(field).filter(col(field).isNull()).count()
    notNullCount = registersAmount - nullCount
    ratio = (notNullCount/ registersAmount) * OneHundred
    return [registersAmount,Rules.NullRule.code,Rules.NullRule.name,Rules.NullRule.property,Rules.NullRule.code + "/" + entity + "/" + field,threshold,dataRequirement,field,ratio,nullCount], errorDf