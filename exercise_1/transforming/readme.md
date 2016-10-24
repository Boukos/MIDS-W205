# Transforming the data

Transforming the data from the data lake to the format specified in the [ER-diagram](https://github.com/adamlenart/MIDS-w205/blob/MIDS-w205/exercise_1/loading_and_modelling/W205-Exercise_1.png) requires:

1. Creating a procedures entity.
 
 The answers to the questions about hospital quality are based on scores of procedures that were performed on a relatively large sample.
 
 As there are two tables in the data lake, measure_effective_care and measure_readmission, they need to be joined to construct the procedures entity. Additionally, the [ER-diagram](https://github.com/adamlenart/MIDS-w205/blob/MIDS-w205/exercise_1/loading_and_modelling/W205-Exercise_1.png) necessitates adding a *procedure_id* primary key to the procedures table which is easier to achieve via PySpark than Hive-SQL. The procedures table also contains *provider_id* and *measure_id* foreign keys.
 
2. Creating a hospital entity.

 From hospital_info, the hospital ID can be created by selecting only the columns that are used in the analysis with *provider_id* as its primary key.

3. Creating a survey entity.

 From the surveys table the survey entity can be createdd by dropping some of the variables and keeping those that were intended to use for the analysis. In the end, only the HCAHPS Base and Consistency scores were used which are the aggregated measures of the other available subscores in the columns of the table.

4. Create a measure entity.

 Essentially, just rename the measure_info table.
 
