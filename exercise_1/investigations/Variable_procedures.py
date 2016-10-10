from pyspark import SparkConf,SparkContext
from pyspark import HiveContext
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as func
from pyspark.sql.functions import  col
from pyspark.sql import Column
from pyspark.sql.functions import monotonicallyIncreasingId

# define standard deviation
def get_sd(col):
    return(func.sqrt(func.avg(col * col) - func.avg(col) * func.avg(col)))

hive_context = HiveContext(sc)
# load data
procedure = hive_context.table("default.procedure")
hospitals = hive_context.table("default.hospital")
measures = hive_context.table("default.measure")
procedure_typecast = procedure.withColumn("score", procedure["score"].cast(DoubleType())).withColumn("sample",procedure["sample"].cast(IntegerType())).withColumn("denominator",procedure["denominator"].cast(IntegerType()))
# join procedure with hospitals
procedure_hospital = procedure_typecast.join(hospitals,procedure_typecast.provider_id==hospitals.provider_id)
# subset for those procedures that have a score not higher than 100 and a sample of at least 50, calculate standard deviation
proc_svd = procedure_hospital.where((procedure_hospital['score']<=100)&(procedure_hospital['sample']>50)).groupby('measure_id').agg(get_sd(procedure_hospital['score']).alias("score_sd"))
# join with measures, sort and print
proc_svd_measures= proc_svd.join(measures,measures.measure_id==proc_svd.measure_id)
proc_svd_measures.sort(proc_svd_measures['score_sd'].desc()).select("measure_id","measure_name","score_sd").show(10)


