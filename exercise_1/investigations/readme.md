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
 The commands are very similar to the previous script; the only difference is that here we need to group the hospitals by state and take their averages. 
                                                       
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

