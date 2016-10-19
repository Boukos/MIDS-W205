from pyspark import SparkConf,SparkContext
from pyspark import HiveContext
from pyspark.sql.functions import monotonicallyIncreasingId
from pyspark.sql.functions import lit
from pyspark.sql.functions import StringType


# instantiate SparkContext for spark-submit
conf = SparkConf().setAppName("transforming effective care and readmissions")
sc = SparkContext(conf=conf)
# read Hive tables
hive_context = HiveContext(sc)
measure_effective_care = hive_context.table("default.measure_effective_care")
# filter
measure_effective_care.registerTempTable("effective_temp")
measure_effective_selected = hive_context.sql(" SELECT provider_id, measure_id,score,sample,measure_start,measure_end FROM effective_temp")
# add a column to flag the data as effective care
measure_effective_with_care_type = measure_effective_selected.withColumn('care_type',lit("effective").cast(StringType()))
# add an empty column for readmission denominators
measure_effective_with_care_type_denominator = measure_effective_with_care_type.withColumn('denominator',lit(None).cast(StringType()))

# read in readmission data
measure_readmission = hive_context.table("default.measure_readmission")
measure_readmission.registerTempTable("readmission_temp")
measure_readmission_selected = hive_context.sql(" SELECT provider_id, measure_id,denominator,score,measure_start,measure_end FROM readmission_temp")
# prepare readmissions for a union with effective measures: add empty column for sample
measure_readmission_with_sample = measure_readmission_selected.withColumn('sample',lit(None).cast(StringType()))
# add a column to flag the data as readmission
measure_readmission_with_sample_care_type = measure_readmission_with_sample.withColumn('care_type',lit("readmission").cast(StringType()))

readmission_ordered = measure_readmission_with_sample_care_type.select("provider_id","measure_id","score","measure_start","measure_end","sample","denominator","care_type")
effective_ordered = measure_effective_with_care_type_denominator.select("provider_id","measure_id","score","measure_start","measure_end","sample","denominator","care_type")


# merge timely and effective and readmitted measures
procedure_temp = effective_ordered.unionAll(readmission_ordered)
# add id
procedure = procedure_temp.select(monotonicallyIncreasingId().alias("procedure_id"),"*")
procedure.write.saveAsTable("procedure",mode="overwrite")


