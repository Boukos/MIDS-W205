--Select only the columns pertinent to the analysis, cast score and sample to int, let the string values be lost--
DROP TABLE IF EXISTS procedure;
CREATE TABLE procedure AS
    SELECT provider_id,
                     measure_id,
                     cast(score AS decimal),
                     cast(sample AS int),
                     measure_start,
                     measure_end
     FROM measure_effective_care
     UNION ALL
     SELECT provider_id,
                      measure_id,
                      score,
                      Null as sample,
                      measure_start,
                      measure_end
       FROM measure_readmission;

ALTER TABLE procedure
    ADD COLUMNS (procedure_id serial);
