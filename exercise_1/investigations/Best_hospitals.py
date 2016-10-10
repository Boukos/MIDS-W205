from pyspark import SparkConf,SparkContext
from pyspark import HiveContext
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as func
from pyspark.sql.functions import  col



hive_context = HiveContext(sc)
procedure = hive_context.table("default.procedure")
procedure_typecast = procedure.withColumn("score", procedure["score"].cast(DoubleType())).withColumn("sample",procedure["sample"].cast(IntegerType())).withColumn("denominator",procedure["denominator"].cast(IntegerType()))
score_avg = procedure_typecast.where((procedure_typecast['score']<100)&(procedure_typecast['sample']>50)).groupby('provider_id','care_type').agg(func.avg('score'))
best_hospital_scores = score_avg.where(score_avg['avg(score)']==100)
best_hospital_scores.show(best_hospital_scores.count())
hospital = hive_context.table("default.hospital")
# all of the hospitals whose average scores across all of the measures is 100
best_hospital_scores.join(hospital,best_hospital_scores.provider_id==hospital.provider_id).select('hospital_name').show(best_hospital_scores.count(),False)

