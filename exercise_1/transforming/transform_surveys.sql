
DROP TABLE IF EXISTS survey;
CREATE TABLE survey AS
   SELECT provider_id,
                    communication_nurses_achieved,
                    communication_nurses_improved,
                    communication_nurses_dimension,
                    communication_doctors_achieved,
                    communication_doctors_improved,
                    communication_doctors_dimension,
                   responsiveness_achieved,
                   responsiveness_improved,
                   responsiveness_dimension,
                   pain_management_achieved,
                   pain_management_improved,
                   pain_management_dimension,
                   communication_medicines_achieved,
                   communication_medicines_improved,
                   communication_medicines_dimension,
                   cleanliness_achieved,
                   cleanliness_improved,
                   cleanliness_dimension,
                   discharge_information_achieved,
                   discharge_information_improved,
                   discharge_information_dimension,
                   overall_achieved,
                   overall_improved,
                   overall_dimension,
                   hcahps_base,
                   hcahps_consistency
       FROM surveys;

