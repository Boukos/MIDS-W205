# Investigating the Medicare dataset


1. What hospitals are models of high-quality care? That is, which hospitals have the most consistently high scores for a variety of procedures.

 The best hospitals are defined by those hospitals who had a sample of more than 50 for each procedure, and whose average scores, in the procedures table equal to the maximum score, 100.
 
 In PySpark:
```
# filter for those whose scores are under 100 and calculate average
 score_avg = procedure_typecast.where((procedure_typecast['score']<=100)&(procedure_typecast['sample']>50)).groupby
                                                                             ('provider_id','care_type').agg(func.avg('score'))
 best_hospital_scores = score_avg.where(score_avg['avg(score)']==100)
 # print the name of all of the hospitals whose average scores across all of the measures is 100
 best_hospital_scores.join(hospital,best_hospital_scores.provider_id==hospital.provider_id).select('hospital_name').show(best_hospital_scores.count(),False)
```
 
| Tables        | 
| ------------- |
| col 3 is      | 
| col 2 is      | 
| zebra stripes | 
                                  
 | hospital_name                               |
 | --- |
 | VA EASTERN KANSAS HEALTHCARE SYSTEM          |
 | ANDROSCOGGIN VALLEY HOSPITAL                 |
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
 
 
 
