from pyspark import SparkConf,SparkContext
from pyspark import HiveContext
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as func
from pyspark.sql.functions import  col



hive_context = HiveContext(sc)
procedure = hive_context.table("default.procedure")
hospitals = hive_context.table("default.hospital")
procedure_typecast = procedure.withColumn("score", procedure["score"].cast(DoubleType())).withColumn("sample",procedure["sample"].cast(IntegerType())).withColumn("denominator",procedure["denominator"].cast(IntegerType()))
procedure_hospital = procedure_typecast.join(hospital,procedure_typecast.provider_id==hospital.provider_id)
# subset for those procedures that have a score not higher than 100 and a sample of at least 50
score_avg = procedure_hospital.where((procedure_hospital['score']<=100)&(procedure_hospital['sample']>50)).groupby('state').agg(func.avg('score'))
# show the 10 best states
best_states = score_avg.sort(score_avg['avg(score)'].desc()).show(10)

