# Investigating the Medicare dataset


1. What hospitals are models of high-quality care? That is, which hospitals have the most consistently high scores for a variety of procedures.

 The best hospitals are defined by those hospitals who had a sample of more than 50 for each procedure, and whose average scores, in the procedures table equal to the maximum score, 100.
 
 In PySpark:
 ```
 # filter for those whose scores that are not higher than 100 and their calculate average
 score_avg = procedure_typecast.where((procedure_typecast['score']<=100)&(procedure_typecast['sample']>50)).groupby
                                                                             ('provider_id','care_type').agg(func.avg('score'))
 # choose those which have an average score of 1000                                                                            
 best_hospital_scores = score_avg.where(score_avg['avg(score)']==100)
 # print the name of all of the hospitals whose average scores across all of the measures is 100
 best_hospital_scores.join(hospital,best_hospital_scores.provider_id==hospital.provider_id).select('hospital_name').show
                                                                             (best_hospital_scores.count(),False)
 ```
 In the script above, procedure_typecast is the dataframe which contains the correctly cast scores (doubles) and sample sizes (integers). Running, the script produces the table below. For more information, refer to the [full PySpark script](https://github.com/adamlenart/MIDS-w205/blob/adamlenart-investigations/exercise_1/investigations/Best_hospitals.py).
 
|          hospital_name                       | 
| -------------------------------------------- |
| VA EASTERN KANSAS HEALTHCARE SYSTEM          | 
| ANDROSCOGGIN VALLEY HOSPITAL                 | 
| NEW LONDON HOSPITAL                          |                                 
| GREENE COUNTY MEDICAL CENTER                 |
| CLARKE COUNTY HOSPITAL                       |
| LINCOLN COUNTY MEDICAL CENTER                |
| MANNING REGIONAL HEALTHCARE CENTER           |
| CROSSRIDGE COMMUNITY HOSPITAL                |
| GRAND JUNCTION VA MEDICAL CENTER             |
| ONECORE HEALTH                               |
| PERRY MEMORIAL HOSPITAL                      |
| MINIMALLY INVASIVE SURGERY HOSPITAL          |
| HILLSBORO AREA HOSPITAL                      |
| SAINT JOSEPH BEREA                           |
| PHILADELPHIA VA MEDICAL CENTER               |
| VA BLACK HILLS HEALTHCARE SYSTEM - FORT MEADE |
 
 2. What states are models of high-quality care?
 
 The best states are defined by those states whose hospitals had a sample of more than 50 for each procedure.

 ```
 # join hospitals with procedures to select the best hospitals based on their procedures
 procedure_hospital = procedure_typecast.join(hospital,procedure_typecast.provider_id==hospital.provider_id)
 # subset for those procedures that have a score not higher than 100 and a sample of at least 50
 score_avg = procedure_hospital.where((procedure_hospital['score']<=100)&(procedure_hospital['sample']>50)).groupby('state')
                                                                                                           .agg(func.avg('score'))
 # show the 10 best states
 best_states = score_avg.sort(score_avg['avg(score)'].desc()).show(10)
 ```
 The commands are very similar to the previous script; the only difference is that here we need to group the hospitals by state and take their averages. For more information, refer to the [full PySpark script](https://github.com/adamlenart/MIDS-w205/blob/adamlenart-investigations/exercise_1/investigations/best_states.py).  
                                                       
|state|       avg(score)|
|-----|-----------------|
|   VI|            89.65|
|   PR|88.94219653179191|
|   MD|88.84135107471853|
|   NH|88.15384615384616|
|   DE|87.76404494382022|
|   DC|            87.39|
|   NE|86.60236886632826|
|   CT| 86.3762102351314|
|   NJ|86.21102982554868|
|   MT|86.13196480938416|

 3. Which procedures have the greatest variability between hospitals?
 
  
 Most variable procedures are defined by those procedures that had a sample of more than 50 for each procedure and a score not higher than 100. Here, we need to define an additional Python function (`get_sd`) to calculate the sample standard deviation as the in-built function for standard deviation calculates the variabiliy of the population.

 ```
 # join procedure with hospitals
 procedure_hospital = procedure_typecast.join(hospitals,procedure_typecast.provider_id==hospitals.provider_id)
 # subset for those procedures that have a score not higher than 100 and a sample of at least 50, calculate standard deviation
 proc_svd = procedure_hospital.where((procedure_hospital['score']<=100)&(procedure_hospital['sample']>50)).groupby('measure_id').agg (get_sd(procedure_hospital['score']).alias("score_sd"))
 # join with measures, sort and print
 proc_svd_measures= proc_svd.join(measures,measures.measure_id==proc_svd.measure_id)
 proc_svd_measures.sort(proc_svd_measures['score_sd'].desc()).select("measure_id","measure_name","score_sd").show(10)
```
 For more information, refer to the [full PySpark script](https://github.com/adamlenart/MIDS-w205/blob/adamlenart-investigations/exercise_1/investigations/Variable_procedures.py).

|measure_name                                                                               |score_sd          |
|-------------------------------------------------------------------------------------------|------------------|
|Thrombolytic Therapy                                                                       |23.87990443101587 |
|Median Time to Transfer to Another Facility for Acute Coronary Intervention- Reporting Rate|21.3652105588085  |
|Admit Decision Time to ED Departure Time for Admitted Patients                             |20.492656736528936|
|Venous Thromboembolism Warfarin Therapy Discharge Instructions                             |17.146172313152178|
|Median Time from ED Arrival to Provider Contact for ED patients                            |15.66896236494397 |
|Median Time to Pain Management for Long Bone Fracture                                      |15.467446280014649|
|Venous Thromboembolism Prophylaxis                                                         |14.154072029938963|
|Home Management Plan of Care (HMPC) Document Given to Patient/Caregiver                    |11.700089736028156|
|Influenza Immunization                                                                     |11.24515533289924 |
|Median Time from ED Arrival to ED Departure for Admitted ED Patients                       |10.812138722288019|

 4. Are average scores for hospital quality or procedural variability correlated with patient survey responses?
 
 
 The correlations are defined based on those procedures that had a sample of more than 50 for each procedure, and a score of 100 or less.

 As the output of the code shows below, the correlations between each of these measures is very low along both dimensions of HCAHPS survey measurements, indicating that patient surveys are independent of hospital averages and procedural averages. However, the base and consistency HCAHPS scores are highly correlated.
 
| Variable   | HCAHPS  | Correlation |
| ---------- | ----------- | ----------- |
| Hospital  |    Base | -0.099 |
| Hospial | Consistency | 0.034 |
 | Procedure | Base | -0.001 |
 | Procedure | Consistency | -0.000 |
 | HCAHPS Base | Consistency | 0.651 |
 ```
 surveys_selected = surveys_typecast.select('provider_id','hcahps_base','hcahps_consistency')
 procedures_selected =  procedure_typecast.select('provider_id','sample','score','measure_id')
 # calculate hospital average scores
 hospital_avg =  procedure_typecast.where((procedure_typecast['score']<100)&(procedure_typecast['sample']>50)).groupby
                                                            ('provider_id').agg(func.avg('score'))
 hosp_avg_surv =hospital_avg.join(surveys_selected,surveys_selected.provider_id==hospital_avg.provider_id)
 # calculate correlation of hospital average score with HCAHPS base score
 hosp_avg_surv.stat.corr('avg(score)','hcahps_base')
 # -0.09947309348326736           
 hosp_avg_surv.stat.corr('avg(score)','hcahps_consistency')
 # 0.03405962378975164 measure_avg = procedure_typecast.where((procedure_typecast['score']<100)&
                                                 (procedure_typecast['sample']>50)).groupby('measure_id').agg(func.avg('score'))
 procedures_surveys = procedures_selected.join(procedures_selected,surveys_selected.provider_id==procedures_selected.provider_id)
 measure_avg_surv = measure_avg.join(procedures_surveys,procedures_surveys.measure_id==measure_avg.measure_id)
 measure_avg_surv.stat.corr('avg(score)','hcahps_base')
 #-0.0005003159809636114   
 measure_avg_surv.stat.corr('avg(score)','hcahps_consistency')
 #-0.0003420560781465772   
 measure_avg_surv.stat.corr('hcahps_base','hcahps_consistency')#
 0.6512279291606266
 ```
For more information, refer to the [full PySpark script](https://github.com/adamlenart/MIDS-w205/blob/adamlenart-investigations/exercise_1/investigations/correlation.py)
