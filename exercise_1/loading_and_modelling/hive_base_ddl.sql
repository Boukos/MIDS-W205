DROP TABLE hospital_info;
CREATE EXTERNAL TABLE hospital_info
(
  provider_id string,
  hospital_name string,
  address string,
  city string,
  state string,
  zip_code int,
  county_name string,
  phone_number bigint,
  hospital_type string,
  hospital_ownership string,
  emergency_services boolean
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/exercise_1/hospitals';

DROP TABLE measure_effective_care;
CREATE EXTERNAL TABLE measure_effective_care
(
  provider_id string,
  hospital_name string,
  address string,
  city string,
  state string,
  zip_code int,
  county_name string,
  phone_number bigint,
  condition string,
  measure_id string,
  measure_name string,
  score string,
  sample string,
  footnote string,
  measure_start date,
 measure_end date
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/exercise_1/effective_care';


DROP TABLE measure_readmission;
CREATE EXTERNAL TABLE measure_readmission
(
  provider_id string,
  hospital_name string,
  address string,
  city string,
  state string,
  zip_code int,
  county_name string,
  phone_number bigint,
  measure_name string,
  measure_id string,
  national_comparison string,
  denominator int,
  score decimal,
  lower_estimate decimal,
  higher_estimate decimal,
  footnote string,
  measure_start date,
  measure_end date
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/exercise_1/readmissions';


DROP TABLE measure_info;
CREATE EXTERNAL TABLE measure_info
(
  measure_name string,
  measure_id string,
  measure_start_quarter string,
  measure_start timestamp,
  measure_end_quarter string,
  measure_end timestamp
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/exercise_1/measures';


DROP TABLE surveys;
CREATE EXTERNAL TABLE surveys
(
  provider_id string,
  hospital_name string,
  address string,
  city string,
  state string,
  zip_code int,
  county_name string,
  communication_nurses_achieved string,
  communication_nurses_improved string,
  communication_nurses_dimension string,
  communication_doctors_achieved string,
  communication_doctors_improved string,
  communication_doctors_dimension string,
  responsiveness_achieved string,
  responsiveness_improved string,
  responsiveness_dimension string,
  pain_management_achieved string,
  pain_management_improved string,
  pain_management_dimension string,
  communication_medicines_achieved string,
  communication_medicines_improved string,
  communication_medicines_dimension string,
  cleanliness_achieved string,
  cleanliness_improved string,
  cleanliness_dimension string,
  discharge_information_achieved string,
  discharge_information_improved string,
  discharge_information_dimension string,
  overall_achieved string,
  overall_improved string,
  overall_dimension string,
  hcahps_base int,
  hcahps_consistency int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/exercise_1/surveys_responses';

