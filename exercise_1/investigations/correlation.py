from pyspark import SparkConf,SparkContext
from pyspark import HiveContext
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as func


hive_context = HiveContext(sc)
# load data
procedure = hive_context.table("default.procedure")
hospitals = hive_context.table("default.hospital")
measures = hive_context.table("default.measure")
surveys = hive_context.table("default.survey")
# cast types
procedure_typecast = procedure.withColumn("score", procedure["score"].cast(DoubleType())).withColumn("sample",procedure["sample"].cast(IntegerType())).withColumn("denominator",procedure["denominator"].cast(IntegerType()))
surveys_typecast = surveys.withColumn("hcahps_base",surveys["hcahps_base"].cast(IntegerType())).withColumn("hcahps_consistency",surveys["hcahps_consistency"].cast(IntegerType()))
surveys_selected = surveys_typecast.select('provider_id','hcahps_base','hcahps_consistency')
procedures_selected = procedure_typecast.select('provider_id','sample','score','measure_id')
# calculate hospital average scores
hospital_avg = procedure_typecast.where((procedure_typecast['score']<100)&(procedure_typecast['sample']>50)).groupby('provider_id').agg(func.avg('score'))
hosp_avg_surv =hospital_avg.join(surveys_selected,surveys_selected.provider_id==hospital_avg.provider_id)
# calculate correlation of hospital average score with HCAHPS base score
hosp_avg_surv.stat.corr('avg(score)','hcahps_base')
# -0.09947309348326736        
   hosp_avg_surv.stat.corr('avg(score)','hcahps_consistency')
# 0.03405962378975164 
measure_avg = procedure_typecast.where((procedure_typecast['score']<100)&(procedure_typecast['sample']>50)).groupby('measure_id').agg(func.avg('score'))
procedures_surveys = procedures_selected.join(procedures_selected,surveys_selected.provider_id==procedures_selected.provider_id)
measure_avg_surv = measure_avg.join(procedures_surveys,procedures_surveys.measure_id==measure_avg.measure_id)
measure_avg_surv.stat.corr('avg(score)','hcahps_base')
#-0.0005003159809636114   
measure_avg_surv.stat.corr('avg(score)','hcahps_consistency')
#-0.0003420560781465772   
measure_avg_surv.stat.corr('hcahps_base','hcahps_consistency')
#0.6512279291606266    
