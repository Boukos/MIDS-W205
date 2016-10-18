# Loading and modelling data

In order to compare the hospitals in the [Medicare](https://data.medicare.gov/data/hospital-compare) sample, we need to load the data into a Hadoop distributed file system, and propose an Entity-Relationship diagram which provides an appropriate model to the data to answer the research questions.


1. What hospitals are models of high-quality care? That is, which hospitals have the most consistently high scores for a variety of procedures.

 Answering this question requires two entities: hospitals and procedures, and the relationship between them is that hospitals perform procedures. One hospital may perform many procedures. The primary keys for these entities are *hospital_id* and *procedure_id*, respectively. The procedures table contains the foreign key *hospital_id* which allows the connection of these tables. 

2. What states are models of high-quality care?

 Similarly to the previous question, the hospitals table contain information on the state the hospital is located in. We may identify the states with the highest quality care by connecting the hospitals with the procedures.

3. Which procedures have the greatest variability between hospitals?

 In order to answer this question, we need to add a new entity, measures, to our entity-relationship model. Measures describe the procedures. One measure may describe many procedures as they might have been performed many times by a hospital. The procedures table contains the foreign key *measure_id* which is the primary key of the measures table.

4. Are average scores for hospital quality or procedural variability correlated with patient survey responses?

 Finding correlations of hospital quality or procedural variability requires adding yet another entity, surveys. As each survey evaluates on hospital, surveys are a weak entity connected to hospitals by the foreign key *hospital_id*.

![](https://github.com/adamlenart/MIDS-w205/blob/MIDS-w205/exercise_1/loading_and_modelling/W205-Exercise_1.png)
