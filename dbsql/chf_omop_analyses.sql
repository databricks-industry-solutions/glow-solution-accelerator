-- Databricks notebook source
-- MAGIC %md # OMOP queries for CHF conditions

-- COMMAND ----------

-- MAGIC %md ## Due Diligence

-- COMMAND ----------

-- Sanity check of out OMOP531 database

SELECT * FROM omop531.source_to_standard_vocab_map WHERE SOURCE_CODE_DESCRIPTION LIKE "Congestive heart failure%"

-- COMMAND ----------

-- Sanity check of out OMOP531 database

SELECT * FROM omop531.condition_occurrence WHERE CONDITION_CONCEPT_ID LIKE "4229%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Find a condition by keyword
-- MAGIC This query enables search of vocabulary entities by keyword. The query does a search of standard concepts names in the CONDITION domain (SNOMED-CT clinical findings and MedDRA concepts) and their synonyms to return all related concepts.
-- MAGIC
-- MAGIC It does not require prior knowledge of where in the logic of the vocabularies the entity is situated.
-- MAGIC
-- MAGIC The following is a sample run of the query to run a search of the Condition domain for keyword 'myocardial infarction'. The input parameters are highlighted in blue.

-- COMMAND ----------

USE OMOP531;

-- COMMAND ----------

SELECT 
	  T.Entity_Concept_Id, 
	  T.Entity_Name, 
	  T.Entity_Code, 
	  T.Entity_Type, 
	  T.Entity_concept_class, 
	  T.Entity_vocabulary_id, 
	  T.Entity_vocabulary_name 
	FROM ( 
	  SELECT 
	    C.concept_id Entity_Concept_Id, 
		C.concept_name Entity_Name, 
		C.CONCEPT_CODE Entity_Code, 
		'Concept' Entity_Type, 
		C.concept_class_id Entity_concept_class, 
		C.vocabulary_id Entity_vocabulary_id, 
		V.vocabulary_name Entity_vocabulary_name, 
		NULL Entity_Mapping_Type, 
		C.valid_start_date, 
		C.valid_end_date 
	  FROM concept C 
	  JOIN vocabulary V ON C.vocabulary_id = V.vocabulary_id 
	  LEFT JOIN concept_synonym S ON C.concept_id = S.concept_id 
	  WHERE 
	    (C.vocabulary_id IN ('SNOMED', 'MedDRA') OR LOWER(C.concept_class_id) = 'clinical finding' ) AND 
		C.concept_class_id IS NOT NULL AND 
		( LOWER(C.concept_name) like 'congestive heart failure' OR -- e.g. myocardial infarction
		  LOWER(S.concept_synonym_name) like '%congestive heart failure%' ) 
	  ) T
	WHERE current_date BETWEEN valid_start_date AND valid_end_date 
	ORDER BY 6,2;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Find condition by concept ID
-- MAGIC Find condition by condition ID is the lookup for obtaining condition or disease concept details associated with a concept identifier. This query is a tool for quick reference for the name, class, level and source vocabulary details associated with a concept identifier, either SNOMED-CT clinical finding or MedDRA. 

-- COMMAND ----------

SELECT 
	  C.concept_id Condition_concept_id, 
	  C.concept_name Condition_concept_name, 
	  C.concept_code Condition_concept_code, 
	  C.concept_class_id Condition_concept_class,
	  C.vocabulary_id Condition_concept_vocab_ID, 
	  V.vocabulary_name Condition_concept_vocab_name, 
	  CASE C.vocabulary_id 
	    WHEN 'SNOMED' THEN CASE lower(C.concept_class_id)   
		WHEN 'clinical finding' THEN 'Yes' ELSE 'No' END 
		WHEN 'MedDRA' THEN 'Yes'
		ELSE 'No' 
	  END Is_Disease_Concept_flag 
	FROM concept C, vocabulary V 
	WHERE 
	  C.concept_id = 4229440 AND -- CHF
	  C.vocabulary_id = V.vocabulary_id AND 
	  current_date BETWEEN valid_start_date AND valid_end_date;

-- COMMAND ----------

SELECT 
	  C.concept_id Condition_concept_id, 
	  C.concept_name Condition_concept_name, 
	  C.concept_code Condition_concept_code, 
	  C.concept_class_id Condition_concept_class,
	  C.vocabulary_id Condition_concept_vocab_ID, 
	  V.vocabulary_name Condition_concept_vocab_name, 
	  CASE C.vocabulary_id 
	    WHEN 'SNOMED' THEN CASE lower(C.concept_class_id)   
		WHEN 'clinical finding' THEN 'Yes' ELSE 'No' END 
		WHEN 'MedDRA' THEN 'Yes'
		ELSE 'No' 
	  END Is_Disease_Concept_flag 
	FROM concept C, vocabulary V 
	WHERE 
	  C.concept_id = 4023479 AND -- CHF
	  C.vocabulary_id = V.vocabulary_id AND 
	  current_date BETWEEN valid_start_date AND valid_end_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Translate a given condition to source codes
-- MAGIC This query allows to search all source codes that are mapped to a SNOMED-CT clinical finding concept. It can be used to translate SNOMED-CT to ICD-9-CM, ICD-10-CM, Read or OXMIS codes.

-- COMMAND ----------

SELECT DISTINCT
	  c1.concept_code,
	  c1.concept_name,
	  c1.vocabulary_id source_vocabulary_id,
	  VS.vocabulary_name source_vocabulary_description,
	  C1.domain_id,
	  C2.concept_id target_concept_id,
	  C2.concept_name target_Concept_Name,
	  C2.concept_code target_Concept_Code,
	  C2.concept_class_id target_Concept_Class,
	  C2.vocabulary_id target_Concept_Vocab_ID,
	  VT.vocabulary_name target_Concept_Vocab_Name
	FROM
	  concept_relationship cr,
	  concept c1,
	  concept c2,
	  vocabulary VS,
	  vocabulary VT
	WHERE
	  cr.concept_id_1 = c1.concept_id AND
	  cr.relationship_id = 'Maps to' AND
	  cr.concept_id_2 = c2.concept_id AND
	  c1.vocabulary_id = VS.vocabulary_id AND
	  c1.domain_id = 'Condition' AND
	  c2.vocabulary_id = VT.vocabulary_id AND
	  c1.concept_id = 4023479  --                                            
	  AND c1.vocabulary_id =
	'SNOMED'                                          
	AND
	current_date                                           
	BETWEEN c2.valid_start_date AND c2.valid_end_date;

-- COMMAND ----------

-- MAGIC %md ## CHF Occurrence in out OMOP database

-- COMMAND ----------

SELECT * FROM condition_occurrence WHERE CONDITION_SOURCE_CONCEPT_ID = 4023479

-- COMMAND ----------

SELECT * FROM condition_occurrence WHERE CONDITION_SOURCE_CONCEPT_ID = 4229440

-- COMMAND ----------

-- MAGIC %md ## Number of patients identified for ```4229440``` grouped by zip code of residence

-- COMMAND ----------

DESCRIBE TABLE PERSON

-- COMMAND ----------

SELECT * FROM person P WHERE P.LOCATION_ID IS NOT NULL --INNER JOIN location L ON P.LOCATION_ID = L.LOCATION_ID 

-- COMMAND ----------

SELECT year_of_birth, COUNT(person_id) AS Num_Persons_count
    FROM person
    GROUP BY year_of_birth
    ORDER BY year_of_birth;

-- COMMAND ----------

SELECT * FROM person P, condition_occurrence CO INNER JOIN PERSON ON CO.PERSON_ID WHERE CO.CONDITION_SOURCE_CONCEPT_ID = 4229440

-- COMMAND ----------

SELECT P.PERSON_ID, P.YEAR_OF_BIRTH ,CO.CONDITION_OCCURRENCE_ID, CO.CONDITION_TYPE_CONCEPT_ID, CO.VISIT_DETAIL_ID, CO.CONDITION_CONCEPT_ID, CO.CONDITION_SOURCE_VALUE, CO.CONDITION_SOURCE_CONCEPT_ID
FROM person P 
INNER JOIN condition_occurrence CO ON P.PERSON_ID = CO.PERSON_ID 
WHERE CO.CONDITION_SOURCE_CONCEPT_ID = 4229440
AND P.RACE_SOURCE_VALUE = "black" 

-- COMMAND ----------

SELECT year_of_birth, COUNT(person_id) AS Num_Persons_count
    FROM (SELECT P.PERSON_ID, P.YEAR_OF_BIRTH ,CO.CONDITION_OCCURRENCE_ID, CO.CONDITION_TYPE_CONCEPT_ID, CO.VISIT_DETAIL_ID, CO.CONDITION_CONCEPT_ID, 
                 CO.CONDITION_SOURCE_VALUE, CO.CONDITION_SOURCE_CONCEPT_ID
          FROM person P 
          INNER JOIN condition_occurrence CO ON P.PERSON_ID = CO.PERSON_ID 
          WHERE CO.CONDITION_SOURCE_CONCEPT_ID = 4229440 AND P.RACE_SOURCE_VALUE = "black")
    GROUP BY year_of_birth
    ORDER BY year_of_birth;

-- COMMAND ----------

SELECT P.PERSON_ID, P.YEAR_OF_BIRTH ,CO.CONDITION_OCCURRENCE_ID, CO.CONDITION_TYPE_CONCEPT_ID, CO.VISIT_DETAIL_ID, CO.CONDITION_CONCEPT_ID, CO.CONDITION_SOURCE_VALUE, CO.CONDITION_SOURCE_CONCEPT_ID
FROM person P 
INNER JOIN condition_occurrence CO ON P.PERSON_ID = CO.PERSON_ID 
WHERE CO.CONDITION_SOURCE_CONCEPT_ID = 4229440
AND P.RACE_SOURCE_VALUE = "black" AND P.ETHNICITY_SOURCE_VALUE = "hispanic"

-- COMMAND ----------

SELECT year_of_birth, COUNT(person_id) AS Num_Persons_count
    FROM (SELECT P.PERSON_ID, P.YEAR_OF_BIRTH ,CO.CONDITION_OCCURRENCE_ID, CO.CONDITION_TYPE_CONCEPT_ID, CO.VISIT_DETAIL_ID, CO.CONDITION_CONCEPT_ID, 
                 CO.CONDITION_SOURCE_VALUE, CO.CONDITION_SOURCE_CONCEPT_ID
          FROM person P 
          INNER JOIN condition_occurrence CO ON P.PERSON_ID = CO.PERSON_ID 
          WHERE CO.CONDITION_SOURCE_CONCEPT_ID = 4229440 AND P.RACE_SOURCE_VALUE = "black" AND P.ETHNICITY_SOURCE_VALUE = "hispanic")
    GROUP BY year_of_birth
    ORDER BY year_of_birth;

-- COMMAND ----------

SELECT state, NVL( zip, '9999999' ) AS zip, count(*) Num_Persons_count
    FROM person 
    LEFT OUTER JOIN location
    USING( location_id )
    GROUP BY state, NVL( zip, '9999999' )
    ORDER BY 1, 2;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Analyze OMOP data with variants

-- COMMAND ----------

SELECT * FROM 

-- COMMAND ----------

SELECT year_of_birth, COUNT(person_id) AS Num_Persons_count
    FROM (SELECT P.PERSON_ID, P.YEAR_OF_BIRTH ,CO.CONDITION_OCCURRENCE_ID, CO.CONDITION_TYPE_CONCEPT_ID, CO.VISIT_DETAIL_ID, CO.CONDITION_CONCEPT_ID, 
                 CO.CONDITION_SOURCE_VALUE, CO.CONDITION_SOURCE_CONCEPT_ID
          FROM person P 
          INNER JOIN condition_occurrence CO ON P.PERSON_ID = CO.PERSON_ID 
          WHERE CO.CONDITION_SOURCE_CONCEPT_ID = 4229440 AND P.RACE_SOURCE_VALUE = "black" AND P.ETHNICITY_SOURCE_VALUE = "hispanic")
    GROUP BY year_of_birth
    ORDER BY year_of_birth;
