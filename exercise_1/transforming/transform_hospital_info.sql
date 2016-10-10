--Select only the columns pertinent to the analysis, the others are stored in the data lake--
DROP TABLE IF EXISTS hospital;
CREATE TABLE hospital AS
    SELECT provider_id,
                     hospital_type,
                     hospital_name,
                     state, 
                     hospital_ownership, 
                     emergency_services
      FROM hospital_info; 


