--Select only the columns pertinent to the analysis, the others are stored in the data lake--
DROP TABLE IF EXISTS measure;
CREATE TABLE measure AS
    SELECT *
     FROM measure_info; 
