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

score_avg.where().sort(col("avg(score)").desc())
df_score = procedure.select("provider_id","score")
pro

df.withColumn("y", replace(col("y")).show()
# converts score to list

procedure = hive_context.table("default.procedure")
df_score = procedure.select("provider_id","score")
# filter for None type
df=df_score.map(lambda x:filter(partial(is_not,None),x))
df.selectExpr("_1 as provider_id","_2 as score")

score = df_score.select('score').flatMap(lambda x:filter(partial(is_not,None),x)).collect()
df_score.map(lambda x:filter(partial(is_not,None),x)).collect()


>>> score_exists = filter(partial(is_not,None),score)




# instantiate SparkContext for spark-submit
conf = SparkConf().setAppName("transforming effective care and readmissions")
sc = SparkContext(conf=conf)
# read Hive tables
hive_context = HiveContext(sc)
procedure = hive_context.table("default.procedure")
# change score, sample and denominator columns to numeric

df_score = procedure.select("provider_id","score")
rdd_score = df_score.rdd
df_score.withColumn("score",df_score["score"].cast(DoubleType()))
score_avg = procedure_typecast.groupby('provider_id','care_type').agg(func.avg('score'))
score_sum = df_score["score"].map(lambda)

def f(a):
    return(a*2)

f(4)

from numpy import sum

def weighted_mean(vals):
    vals = list(vals)  # save the values from the iterator
    sum_of_weights = sum(tup[1] for tup in vals)
    return sum(1. * tup[0] * tup[1] / sum_of_weights for tup in vals)

df.map(
    lambda x: (x[0], tuple(x[1:]))  # reshape to (key, val) so grouping could work
).groupByKey().mapValues(
    weighted_mean
).collect()
