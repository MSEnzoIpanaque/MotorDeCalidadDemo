from motordecalidad.constants import *
from pyspark.sql import DataFrame
from operator import eq, ge, gt, le, lt, ne
from pyspark.sql.functions import lit

#Function to define the dbutils library from Azure Databricks
def get_dbutils():
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils

def applyFilter(object:DataFrame, filtered) :
    try:
        filteredColumn = filtered.get(JsonParts.Fields)
        filterValue = filtered.get(JsonParts.Values)
        print("Extracción de parametros de filtrado finalizada")
        return object.filter(col(filteredColumn)==filterValue)
    except:
        print("Se omite filtro")
        return object
    
def convert_field_to_struct(object, list_campos: list):
    list_struct_fields = []

    for campo in list_campos:
        type = object.schema[campo].dataType
        list_struct_fields.append(StructField(campo, type))

    return StructType(list_struct_fields)

def chooseOper(col,op:str):
    if op=='+':
        return col.__add__
    if op=='-':
        return col.__sub__
    if op=='*':
        return col.__mul__
    if op=='/':
        return col.__div__
    if op=='==':
        return ne
    if op=='!=':
        return eq
    if op=='<=':
        return gt
    if op=='>=':
        return lt
    if op=='>':
        return le
    if op=='<':
        return ge
    
#Function that chooses the comparision operators based on operation type
def chooseComparisonOparator(includeLimitLeft:bool,includeLimitRight:bool,inclusive:bool):
    res=[]
    if inclusive:
        if includeLimitLeft:
            res.append(lt)
        else:
            res.append(le)

        if includeLimitRight:
            res.append(gt)
        else:
            res.append(ge)

    else:
        if includeLimitLeft:
            res.append(ge)
        else:
            res.append(gt)

        if includeLimitRight:
            res.append(le)
        else:
            res.append(lt)
    
    return res[Zero],res[One]

def operation(object:DataFrame,
                      input:str):
    originalColumns=object.columns
    aux= input.split()
    if(len(aux)==3):
        try:
            num1=float(aux[0])
            oper=chooseOper(lit(num1),aux[1])
            try:
                num2=float(aux[2])
                res=oper(lit(num2))
            except:
                res=oper(object[aux[2]])
        except:
            oper=chooseOper(object[aux[0]],aux[1])
            try:
                num2=float(aux[2])
                res=oper(lit(num2))
            except:
                res=oper(object[aux[2]])
           
        return object.withColumn('ss',res)
    try:
        f=0
        while(True):
           
            par1=aux.index('(')
            par2=aux.index(')')
            newInput=' '.join(aux[par1+1:par2])
            res=operation(object,newInput)
            newInput=' '.join(aux[:par1])+' VAL'+str(f)+' '+' '.join(aux[par2+1:])
            originalColumns.append('VAL'+str(f))
            object=res.withColumnRenamed(res.columns[-1],('VAL'+str(f)))
            object=object.select(originalColumns)
            f+=1
            aux=newInput.split()
           
    except:
        try:
            f=0
            while(True):
                mul1=aux.index('*')
                newInput=' '.join(aux[mul1-1:mul1+2])
                res=operation(object,newInput)
                newInput=' '.join(aux[:mul1-1])+' MUL'+str(f)+' '+' '.join(aux[mul1+2:])
                object=res.withColumnRenamed('ss',('MUL'+str(f)))
                f+=1
                aux=newInput.split()
        except:
            try:
                f=0
                while(True):
                    div1=aux.index('/')
                    newInput=' '.join(aux[div1-1:div1+2])
                    res=operation(object,newInput)
                    newInput=' '.join(aux[:div1-1])+' DIV'+str(f)+' '+' '.join(aux[div1+2:])
                    object=res.withColumnRenamed('ss',('DIV'+str(f)))
                    f+=1
                    aux=newInput.split()
            except:
                try:
                    f=0
                    while(True):
                        res1=aux.index('-')
                        newInput=' '.join(aux[res1-1:res1+2])
                        res=operation(object,newInput)
                        newInput=' '.join(aux[:res1-1])+' RES'+str(f)+' '+' '.join(aux[res1+2:])
                        object=res.withColumnRenamed('ss',('RES'+str(f)))
                        f+=1
                        aux=newInput.split()
                except:
                    try:
                        f=0
                        while(True):
                            su1=aux.index('+')
                           
                            newInput=' '.join(aux[su1-1:su1+2])
                            res=operation(object,newInput)
                            newInput=' '.join(aux[:su1-1])+' SUM'+str(f)+' '+' '.join(aux[su1+2:])
                            object=res.withColumnRenamed('ss',('SUM'+str(f)))
                            f+=1
                            aux=newInput.split()
                    except:
                        return object