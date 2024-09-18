

-- main query -----------------------------------------
SELECT *
FROM pts
LEFT JOIN cdx USING (patient_id)
LEFT JOIN mdx USING (patient_id)
where met_date>='2018-01-01'
;



DROP TABLE IF EXISTS #her2all;
CREATE TABLE #her2all AS (
SELECT *
FROM (
     SELECT patient_id,
            test_name_code,
            test_date,
            test_collection_date,
            test_method_name,
            test_name_name,
            test_value_name,
            test_value_numeric,
            test_unit_name,

            CASE  -- labeling all test methods since some are null when method is specified in test name
                WHEN (lower(test_method_name) SIMILAR TO '%immun%' OR lower(test_name_name) SIMILAR TO '%immun%') THEN 'IHC'
                WHEN (lower(test_method_name) SIMILAR TO '%ish%|%in situ%' OR lower(test_name_name) SIMILAR TO '%ish%|%in situ%') THEN 'FISH'
                ELSE 'Other/Not Specified'
            END AS test_method,

            CASE --cleaning up test results
                WHEN (test_method = 'IHC' AND
                     test_value_name IN ('Borderline','Indeterminate','Intermediate', 'Not Recorded','Low') AND
                     test_value_numeric IN ('1.0','1','1+','1-2')) THEN 'Low'
                WHEN (test_method = 'IHC' AND
                     test_value_name IN ('Borderline','Indeterminate','Intermediate', 'Not Recorded') AND
                     test_value_numeric IN ('2.0','2','2+')) THEN 'Borderline'
                WHEN test_method = 'IHC' AND test_value_name = 'Negative' AND test_value_numeric IN ('1','1+','1.0') THEN 'Low'
                WHEN test_method = 'IHC' AND test_value_name IN ('Borderline', 'Intermediate','Indeterminate','Equivocal') AND test_value_numeric IS NULL THEN 'Borderline'
                WHEN test_value_name IN ('Positive', 'High') THEN 'Positive'
                WHEN test_value_name IN ('Negative', 'Wild Type') THEN 'Negative'
                WHEN test_value_name IN ('Insufficient sample', 'Not Recorded', 'Suppressed', 'Unknown','Not performed') OR
                     test_value_name IS NULL THEN 'Unknown'
                ELSE test_value_name
            END AS test_result,

            CASE
                WHEN test_value_numeric LIKE '%\\+' THEN replace(test_value_numeric,'+','')
                WHEN test_value_numeric IS NULL THEN '000-000-000'
                ELSE test_value_numeric
            END AS test_value_numeric_cleaned, -- cleaning up numeric column, no nulls so can partition

--          DENSE_RANK() OVER (PARTITION BY patient_id, test_date, test_name_name, test_method_name ORDER BY test_value_name) as num_distinct_res,

         CASE
                WHEN test_method = 'IHC' AND --if value all negative and one or more have num value 1 but others are null, then mark for change to Low
                     ((MIN(test_value_name) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) =
                      (MAX(test_value_name) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)))
                         AND
                     ((MIN(test_result) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) IN ('Low','Negative') AND
                      (MAX(test_result) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) IN ('Low','Negative'))
                        AND
                    (((MIN(test_value_numeric_cleaned) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name))  = '1' AND
                      (MAX(test_value_numeric_cleaned) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) = '000-000-000')
                         OR
                     ((MIN(test_value_numeric_cleaned) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) = '000-000-000' AND
                      (MAX(test_value_numeric_cleaned) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) = '1')) THEN 'change'

                WHEN ((MIN(test_result) OVER
                        (PARTITION BY patient_id, test_date,test_name_name, test_method_name)) <>
                      (MAX(test_result) OVER
                        (PARTITION BY patient_id, test_date, test_name_name,test_method_name)))
                    THEN 'conflicting'
            ELSE 'none' END AS sameday_ind,

         curation_indicator

     FROM patient_test
     WHERE ((test_name_code in ('18474-7', '72383-3', '31150-6', '49683-6', '74860-8') or
                  (test_name_code = '3430' and LOWER(test_method_name) similar to '(%immun%|%ish%|%in situ%)') or
                  (test_name_code = 'C16152' and
                   lower(test_method_name) similar to '(%immun%|%ish%|%in situ%)' and
                   (lower(genetic_test_type_name) similar to '(copy number%|%amplifi%)' or genetic_test_type_name is null)) or
                  (test_name_code = '48676-1' and (test_value_numeric in ('0', '0+', '1', '1+', '2', '2+', '3', '3+')
                                        or test_value_numeric is null))))
                  ) all_her2

 ); --select count(distinct patient_id) from #her2all;



-- DROP TABLE IF EXISTS #her2all_cleaned;
-- CREATE TABLE #her2all_cleaned AS (
--     SELECT *,
--          -- removing conflicting results from same test dates, and marking some borderline/intermed/pos conflicting results to change to positive
--         CASE
--             WHEN (MAX(num_distinct_res) OVER
--           (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) > 2 THEN 'Yes'
--             ELSE 'No'
--     END AS multiple_values
--
--         FROM #her2all
-- );
--

-- select * from #her2all_cleaned where sameday_ind = 'change' order by patient_id, test_date,test_method;


-- UPDATE #her2all_cleaned
--     SET sameday_ind = 'conflicting'
--     WHERE multiple_values = 'Yes';

 UPDATE #her2all
     SET test_result = 'Low',sameday_ind = 'none'
     WHERE sameday_ind = 'change';

select * from #her2all where test_method IN ('IHC', 'FISH') AND test_result = 'Negative' order by 1;
select count(distinct patient_id) from #her2all where test_method IN ('IHC', 'FISH') AND test_result = 'Negative'; -- 32840
select count(distinct patient_id) from #her2all where test_method IN ('IHC', 'FISH') AND test_result = 'Negative' and sameday_ind <> 'none'; -- 4115
select * from #her2all where test_method IN ('IHC', 'FISH') AND sameday_ind <> 'none' AND
                             patient_id in (select distinct patient_id from #her2all where test_result = 'Negative' and sameday_ind <> 'none') order by 1,3;
select count(distinct patient_id) from #her2all where test_method ='IHC' AND test_result = 'Negative'; --27356
------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #her2all_current;
CREATE TABLE #her2all_current AS (
SELECT *
FROM (
     SELECT patient_id,
            test_name_code,
            test_date,
            test_collection_date,
            test_method_name,
            test_name_name,
            test_value_name,
            test_value_numeric,
            test_unit_name,

            CASE  -- labeling all test methods since some are null when method is specified in test name
                WHEN (lower(test_method_name) SIMILAR TO '%immun%' OR lower(test_name_name) SIMILAR TO '%immun%') THEN 'IHC'
                WHEN (lower(test_method_name) SIMILAR TO '%ish%|%in situ%' OR lower(test_name_name) SIMILAR TO '%ish%|%in situ%') THEN 'FISH'
                ELSE 'Other/Not Specified'
            END AS test_method,

            CASE --cleaning up test results
                WHEN test_method = 'IHC' AND test_value_numeric IN ('1.0','1','1+','1-2') THEN 'Low1+'
                WHEN test_method = 'IHC' AND test_value_numeric IN ('2.0','2','2+','2-3') THEN 'Low2+'
--                 WHEN test_method = 'IHC' AND test_value_name IN ('Borderline', 'Intermediate','Indeterminate','Equivocal') AND test_value_numeric IS NULL THEN 'Borderline'
                WHEN test_value_name IN ('Positive', 'High') THEN 'Positive'
                WHEN test_value_name IN ('Negative', 'Wild Type') THEN 'Negative'
                WHEN test_value_name IN ('Insufficient sample', 'Not Recorded', 'Suppressed', 'Unknown','Not performed') OR
                     test_value_name IS NULL THEN 'Unknown'
                ELSE test_value_name
            END AS test_result,

            CASE
                WHEN test_value_numeric LIKE '%\\+' THEN replace(test_value_numeric,'+','')
                WHEN test_value_numeric IS NULL THEN '000-000-000'
                ELSE test_value_numeric
            END AS test_value_numeric_cleaned, -- cleaning up numeric column, no nulls so can partition

         CASE
                WHEN test_method = 'IHC' AND --if value all negative and one or more have num value 1 but others are null, then mark for change to Low
                     ((MIN(test_value_name) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) =
                      (MAX(test_value_name) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)))
                         AND
                     ((MIN(test_result) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) IN ('Low1+','Negative') AND
                      (MAX(test_result) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) IN ('Low1+','Negative'))
                        AND
                    (((MIN(test_value_numeric_cleaned) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name))  = '1' AND
                      (MAX(test_value_numeric_cleaned) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) = '000-000-000')
                         OR
                     ((MIN(test_value_numeric_cleaned) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) = '000-000-000' AND
                      (MAX(test_value_numeric_cleaned) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) = '1')) THEN 'change'

                WHEN ((MIN(test_result) OVER
                        (PARTITION BY patient_id, test_date,test_name_name, test_method_name)) <>
                      (MAX(test_result) OVER
                        (PARTITION BY patient_id, test_date, test_name_name,test_method_name)))
                    THEN 'conflicting'
            ELSE 'none' END AS sameday_ind,

         curation_indicator

     FROM patient_test
     WHERE ((test_name_code in ('18474-7', '72383-3', '31150-6', '49683-6', '74860-8') or
                  (test_name_code = '3430' and LOWER(test_method_name) similar to '(%immun%|%ish%|%in situ%)') or
                  (test_name_code = 'C16152' and
                   lower(test_method_name) similar to '(%immun%|%ish%|%in situ%)' and
                   (lower(genetic_test_type_name) similar to '(copy number%|%amplifi%)' or genetic_test_type_name is null)) or
                  (test_name_code = '48676-1' and (test_value_numeric in ('0', '0+', '1', '1+', '2', '2+', '3', '3+')
                                        or test_value_numeric is null))))
                  ) all_her2

 );

 UPDATE #her2all_current
     SET test_result = 'Low1+',sameday_ind = 'none'
     WHERE sameday_ind = 'change';

select * from #her2all_current where test_method IN ('IHC', 'FISH') AND test_result = 'Negative' order by 1;
select count(distinct patient_id) from #her2all where test_method IN ('IHC', 'FISH') AND test_result = 'Negative'; -- 32840
select count(distinct patient_id) from #her2all_current where test_method IN ('IHC', 'FISH') AND test_result = 'Negative'; -- 32834

select * from #her2all where test_method IN ('IHC', 'FISH') AND
                             patient_id not in (select distinct patient_id from #her2all_current where test_method IN ('IHC', 'FISH') AND test_result = 'Negative') order by 1,3;

select count(distinct patient_id) from #her2all where test_method IN ('IHC', 'FISH') AND test_result = 'Negative' AND sameday_ind <> 'none'; --4113
select * from #her2all where test_method IN ('IHC', 'FISH') AND sameday_ind <> 'none' AND
                             patient_id in (select distinct patient_id from #her2all where test_result = 'Negative' and sameday_ind <> 'none') order by 1,3; --
select count(distinct patient_id) from #her2all where test_method ='IHC' AND test_result = 'Negative'; --27342



------------------------------------------------------------------------------------------------------------


-- select * from #her2all limit 10;


DROP TABLE IF EXISTS #her2lowtests;
WITH her2_fish AS ( --identify HER2 FISH tests
     SELECT *
     FROM #her2all
     WHERE test_method = 'FISH' AND sameday_ind = 'none' AND
           (test_value_numeric in ('0','0-1','1','1+','1-2','2','2+','2-3','3','3+') OR test_value_numeric IS NULL) -- get only ones with clean num or null
 )

--identify HER2 IHC tests and filter for IHC tests that would qualify for HER2low (test values of 1 or 2), only 2 for AZ
, her2_ihc AS (
    SELECT *
    FROM #her2all
    WHERE test_method = 'IHC' AND sameday_ind = 'none' AND
          (test_value_numeric in ('0','0+','1','1.0','1+','1-2','2','2.0','2+','2-3','3','3.0','3+') OR test_value_numeric IS NULL)
)

--join valid IHC tests to fish tests to find FISH tests which occurred within 5 days prior and 30 days post IHC test
 , ihc_fish AS (select ihc.patient_id,
                       ihc.test_date   as ihc_test_date,
                       ihc.test_result   as ihc_test_result,
                       ihc.test_method   as ihc_test_method,
                       fish.test_date    as fish_test_date,
                       fish.test_result   as fish_test_result,
                       fish.test_method   as fish_test_method,

                       DATEDIFF(day, ihc_test_date, fish_test_date) as datediffer,

                       dense_rank()
                       over (PARTITION BY ihc.patient_id, ihc.test_date ORDER BY ABS(DATEDIFF(day, ihc.test_date, fish.test_date))) as closest_test
                FROM her2_ihc ihc
                         LEFT JOIN her2_fish fish on ihc.patient_id = fish.patient_id
                where ihc_test_date is not null
)

select *
into #her2lowtests
from ihc_fish where closest_test = 1; --(datediffer between -5 and 120)

-- select count(distinct patient_id), fish_test_result from #her2lowtests group by fish_test_result;
-- select count(distinct patient_id) from #her2lowtests;


DROP TABLE IF EXISTS #her2lowtests_current;
WITH her2_fish AS ( --identify HER2 FISH tests
     SELECT *
     FROM #her2all_current
     WHERE test_method = 'FISH' AND sameday_ind = 'none' AND
           (test_value_numeric in ('0','0-1','1','1+','1-2','2','2+','2-3','3','3+') OR test_value_numeric IS NULL) -- get only ones with clean num or null
 )

--identify HER2 IHC tests and filter for IHC tests that would qualify for HER2low (test values of 1 or 2), only 2 for AZ
, her2_ihc AS (
    SELECT *
    FROM #her2all_current
    WHERE test_method = 'IHC' AND sameday_ind = 'none' AND
          (test_value_numeric in ('0','0+','1','1.0','1+','1-2','2','2.0','2+','2-3','3','3.0','3+') OR test_value_numeric IS NULL)
)

--join valid IHC tests to fish tests to find FISH tests which occurred within 5 days prior and 30 days post IHC test
 , ihc_fish AS (select ihc.patient_id,
                       ihc.test_date   as ihc_test_date,
                       ihc.test_result   as ihc_test_result,
                       ihc.test_method   as ihc_test_method,
                       fish.test_date    as fish_test_date,
                       fish.test_result   as fish_test_result,
                       fish.test_method   as fish_test_method,

                       DATEDIFF(day, ihc_test_date, fish_test_date) as datediffer,

                       dense_rank()
                       over (PARTITION BY ihc.patient_id, ihc.test_date ORDER BY ABS(DATEDIFF(day, ihc.test_date, fish.test_date))) as closest_test
                FROM her2_ihc ihc
                         LEFT JOIN her2_fish fish on ihc.patient_id = fish.patient_id
                where ihc_test_date is not null
)

select *
into #her2lowtests_current
from ihc_fish where closest_test = 1;

select count(distinct patient_id) from #her2lowtests where ihc_test_result = 'Negative'; -- 24526
select count(distinct patient_id) from #her2lowtests_current where ihc_test_result = 'Negative'; --24510

select * from #her2lowtests where ihc_test_result = 'Negative' AND
                                        patient_id not in (select distinct patient_id from #her2lowtests_current where ihc_test_result = 'Negative'); --





--      SELECT ihc.patient_id,
--             ihc.test_date AS ihc_test_date,
--             ihc.test_collection_date AS ihc_test_collection_date,
--             ihc.test_method_name AS ihc_test_method,
--             fish.test_date AS fish_test_date,
--             fish.test_collection_date AS fish_test_collection_date,
--             ihc.test_value_numeric AS ihc_test_value_numeric,
--             ihc.test_result AS ihc_test_value_name,
--             fish.test_result AS fish_test_value_name,
--             ihc.curation_indicator AS ihc_curation_ind,
--             fish.curation_indicator AS fish_curation_ind,
--             ABS(DATEDIFF(DAY, ihc.test_date, fish.test_date)) as datediffer,
--             ROW_NUMBER() OVER (
--                 PARTITION BY ihc.patient_id, ihc.test_date
--                 ORDER BY ABS(DATEDIFF(DAY, ihc.test_date, fish.test_date) ) --closest to met diagnosis date
--                 ) AS priority
-- FROM her2_ihc ihc
-- LEFT JOIN her2_fish fish USING (patient_id)
-- )
-- SELECT * INTO #her2lowtests
--          FROM ihc_fish
-- WHERE priority = 1 and ihc_test_date is not null;

-- select * from #her2lowtests;
-- select count(*) from #her2lowtests;
-- select count(distinct patient_id) from #her2lowtests; --37773



 --look for IHC and FISH tests either collected on the same day or test dates where FISH confirmation in the interval of 5 days prior to 30 days post IHC test

--filter to find FISH negative tests to confirm HER2low
-- SELECT *
-- INTO #her2lowtests
-- FROM ihc_fish
-- WHERE abs_test_datediff = min_datediff ;--or abs_test_datediff is null;
--
-- select count(distinct patient_id),ihc_test_value_name from #her2lowtests group by ihc_test_value_name;
-- -- select count(distinct patient_id),fish_test_value_name from #her2lowtests group by fish_test_value_name;
--
-- SELECT * FROM #her2lowtests limit 10; --35288



---------------------------------------
-- ER and PR

DROP TABLE IF EXISTS #hr_her2;
CREATE TABLE #hr_her2 AS (
WITH erpr AS (
SELECT patient_id,
       test_name_name,
       test_method_name,
       test_value_name,
       test_value_numeric,
       test_date,

       CASE -- labeling biomarkers
           WHEN (test_name_code IN ('14228-1', '40556-3', '16112-5') OR
                        (test_name_code = '3467' AND test_method_name ILIKE '%immun%')) THEN 'ER'
           WHEN  (test_name_code IN ('16113-3', '14230-7', '40557-1') OR
                        (test_name_code = '8910' AND test_method_name ILIKE '%immun%')) THEN 'PR'
           WHEN (test_name_name ILIKE '%hormone recept%' AND
                        LOWER(test_method_name) ILIKE '%immun%') THEN 'ER/PR'-- hormone receptor
           END AS biomarker,

       CASE  -- labeling all test methods since some are null when method is specified in test name
           WHEN LOWER(test_method_name) SIMILAR TO '%immun%' OR LOWER(test_name_name) SIMILAR TO '%immun%' THEN 'IHC'
           ELSE 'Not Specified'
       END AS test_method,

       CASE -- cleaning up result category
            WHEN test_value_name IN ('Negative', 'Wild Type') THEN 'Negative'
            WHEN test_value_name IN ('Indeterminate', 'Insufficient sample', 'Not Recorded', 'Suppressed', 'Unknown') THEN 'Unknown'
            WHEN test_value_name IS NULL THEN 'Unknown'
            ELSE test_value_name
       END AS test_result,

       replace(test_value_numeric,'+','') as test_value_numeric_cleaned--, -- cleaning up numeric column

FROM patient_test
WHERE (test_name_code IN ('14228-1', '40556-3', '16112-5') OR (test_name_code = '3467' AND test_method_name ILIKE '%immunohisto%')) --er
      OR (test_name_code IN ('16113-3', '14230-7', '40557-1') OR (test_name_code = '8910' AND test_method_name ILIKE '%immunohisto%')) -- pr
      OR (test_name_name ILIKE '%hormone recept%' AND LOWER(test_method_name) ILIKE 'immun%') -- hormone receptor
UNION
    SELECT patient_id,  -- pulling from condition table
           NULL AS test_name_name,
           NULL AS test_method_name,
           NULL AS test_value_name,
           NULL AS test_value_numeric,
           diagnosis_date AS test_date,
           'ER' AS biomarker,
           'IHC' AS test_method,
           CASE
                    WHEN diagnosis_code_name ILIKE '%positive%' THEN 'Positive'
                    WHEN diagnosis_code_name ILIKE '%negative%' THEN 'Negative'
           END AS  test_result,

           NULL AS test_value_numeric_cleaned

    FROM condition
    WHERE diagnosis_code_code ILIKE '%Z17.%'
), erpr_clean as (
    select *,
           CASE
            WHEN (MIN(test_result) OVER
                      (PARTITION BY patient_id, test_date, test_name_name, test_method_name) <>
                   MAX(test_result) OVER
                       (PARTITION BY patient_id, test_date, test_name_name, test_method_name))
                             THEN 'conflicting'
            ELSE 'none'
       END AS sameday_conflict
    from erpr
),

    her2_tests AS ( --reformatting her2low tests so can union with er and pr tests
        SELECT --IHC
            patient_id,
            'HER2' AS biomarker,
            ihc_test_result AS test_result,
            ihc_test_date AS test_date,
            'IHC' AS test_method
        FROM #her2lowtests
        UNION
        SELECT --FISH
            patient_id,
            'HER2' AS biomarker,
            fish_test_result AS test_result,
            fish_test_date AS test_date,
            'FISH' AS test_method
        FROM #her2lowtests
    )
SELECT
    patient_id,
    biomarker,
    test_result,
    test_date,
    test_method
FROM erpr_clean
WHERE sameday_conflict = 'none'
UNION
SELECT * FROM her2_tests);





DROP TABLE IF EXISTS #hr_her2_current;
CREATE TABLE #hr_her2_current AS (
WITH erpr AS (
SELECT patient_id,
       test_name_name,
       test_method_name,
       test_value_name,
       test_value_numeric,
       test_date,

       CASE -- labeling biomarkers
           WHEN (test_name_code IN ('14228-1', '40556-3', '16112-5') OR
                        (test_name_code = '3467' AND test_method_name ILIKE '%immun%')) THEN 'ER'
           WHEN  (test_name_code IN ('16113-3', '14230-7', '40557-1') OR
                        (test_name_code = '8910' AND test_method_name ILIKE '%immun%')) THEN 'PR'
           WHEN (test_name_name ILIKE '%hormone recept%' AND
                        LOWER(test_method_name) ILIKE '%immun%') THEN 'ER/PR'-- hormone receptor
           END AS biomarker,

       CASE  -- labeling all test methods since some are null when method is specified in test name
           WHEN LOWER(test_method_name) SIMILAR TO '%immun%' OR LOWER(test_name_name) SIMILAR TO '%immun%' THEN 'IHC'
           ELSE 'Not Specified'
       END AS test_method,

       CASE -- cleaning up result category
            WHEN test_value_name IN ('Negative', 'Wild Type') THEN 'Negative'
            WHEN test_value_name IN ('Indeterminate', 'Insufficient sample', 'Not Recorded', 'Suppressed', 'Unknown') THEN 'Unknown'
            WHEN test_value_name IS NULL THEN 'Unknown'
            ELSE test_value_name
       END AS test_result,

       replace(test_value_numeric,'+','') as test_value_numeric_cleaned--, -- cleaning up numeric column

FROM patient_test
WHERE (test_name_code IN ('14228-1', '40556-3', '16112-5') OR (test_name_code = '3467' AND test_method_name ILIKE '%immunohisto%')) --er
      OR (test_name_code IN ('16113-3', '14230-7', '40557-1') OR (test_name_code = '8910' AND test_method_name ILIKE '%immunohisto%')) -- pr
      OR (test_name_name ILIKE '%hormone recept%' AND LOWER(test_method_name) ILIKE 'immun%') -- hormone receptor
UNION
    SELECT patient_id,  -- pulling from condition table
           NULL AS test_name_name,
           NULL AS test_method_name,
           NULL AS test_value_name,
           NULL AS test_value_numeric,
           diagnosis_date AS test_date,
           'ER' AS biomarker,
           'IHC' AS test_method,
           CASE
                    WHEN diagnosis_code_name ILIKE '%positive%' THEN 'Positive'
                    WHEN diagnosis_code_name ILIKE '%negative%' THEN 'Negative'
           END AS  test_result,

           NULL AS test_value_numeric_cleaned

    FROM condition
    WHERE diagnosis_code_code ILIKE '%Z17.%'
), erpr_clean as (
    select *,
           CASE
            WHEN (MIN(test_result) OVER
                      (PARTITION BY patient_id, test_date, test_name_name, test_method_name) <>
                   MAX(test_result) OVER
                       (PARTITION BY patient_id, test_date, test_name_name, test_method_name))
                             THEN 'conflicting'
            ELSE 'none'
       END AS sameday_conflict
    from erpr
),

    her2_tests AS ( --reformatting her2low tests so can union with er and pr tests
        SELECT --IHC
            patient_id,
            'HER2' AS biomarker,
            ihc_test_result AS test_result,
            ihc_test_date AS test_date,
            'IHC' AS test_method
        FROM #her2lowtests_current
        UNION
        SELECT --FISH
            patient_id,
            'HER2' AS biomarker,
            fish_test_result AS test_result,
            fish_test_date AS test_date,
            'FISH' AS test_method
        FROM #her2lowtests_current
    )
SELECT
    patient_id,
    biomarker,
    test_result,
    test_date,
    test_method
FROM erpr_clean
WHERE sameday_conflict = 'none'
UNION
SELECT * FROM her2_tests);

select * from #hr_her2 limit 50;
select count(distinct patient_id) from #hr_her2 where biomarker = 'HER2' and test_result = 'Negative'; --26516
select count(distinct patient_id) from #hr_her2_current where biomarker = 'HER2' and test_result = 'Negative'; --25847

select * from #hr_her2 where biomarker = 'HER2' and test_result = 'Negative' AND
                             patient_id not in (select distinct patient_id from #hr_her2_current where biomarker = 'HER2' and test_result = 'Negative'); --26516




-- select count(distinct patient_id), test_result,biomarker from #hr_her2 group by test_result,biomarker;
-- select count(distinct patient_id) from #hr_her2; --44069
-- select * from #hr_her2 limit 50;
-- select count(*) from #hr_her2;

-- get closest test dates to met diagnosis
DROP TABLE IF EXISTS #test_priority;
with met_prior AS (
    SELECT patient_id,
           met_date,
           test_date,
           biomarker,
           test_method,
           test_result,

           DATEDIFF(day,met_date, test_date) as datediffer,

           DENSE_RANK() OVER (PARTITION BY patient_id,met_date,biomarker,test_method ORDER BY ABS(DATEDIFF(day,met_date, test_date))) as closest_test

    FROM #hr_her2
    INNER JOIN breast_pts_mets using (patient_id)
    where test_date is not null
)
SELECT *
INTO #test_priority
FROM met_prior
    WHERE closest_test = 1; -- AND (datediffer BETWEEN -5 AND 120) ORDER BY 1,2;

select * from #test_priority order by datediffer asc;



DROP TABLE IF EXISTS #test_priority_current;
with met_prior AS (
    SELECT patient_id,
           met_date,
           test_date,
           biomarker,
           test_method,
           test_result,

           DATEDIFF(day,met_date, test_date) as datediffer,

           DENSE_RANK() OVER (PARTITION BY patient_id,met_date,biomarker,test_method ORDER BY ABS(DATEDIFF(day,met_date, test_date))) as closest_test

    FROM #hr_her2_current
    INNER JOIN breast_pts_mets using (patient_id)
    where test_date is not null
)
SELECT *
INTO #test_priority_current
FROM met_prior
    WHERE closest_test = 1;




-- CREATE TABLE #test_priority AS
-- (SELECT DISTINCT patient_id,
--                                                 met_date,
--                                                 test_date,
--                                                 test_result,
--                                                 biomarker,
--                                                 test_method,
--                                                 datediffer,
--                                                 priority
--                                 FROM (SELECT a.patient_id,
--                                              met_date,
--                                              a.test_date,
--                                              a.test_result,
--                                              a.biomarker,
--                                              a.test_method,
--                                              ABS(DATEDIFF(DAY, trunc(met_date), trunc(a.test_date))) AS datediffer,
--                                              ROW_NUMBER() OVER (
--                                                  PARTITION BY a.patient_id, met_date, a.biomarker,a.test_method
--                                                  ORDER BY ABS(DATEDIFF(DAY, met_date, a.test_date)) --closest to met diagnosis date
--                                                  )                                                   AS priority
--                                       FROM #hr_her2 a
--                                                INNER JOIN breast_pts_mets USING (patient_id)) prioritize
-- WHERE priority = 1);
-- )select * from test order by 1,2;

-- select count(distinct patient_id) from #test_priority; --3695


-- pivot to wider
drop table if exists #er_pr_her2_wide;
create table #er_pr_her2_wide as (
select patient_id,
       met_date,
       MAX(case when  biomarker = 'ER' and test_method = 'IHC'then test_result end) as er,
       MAX(case when biomarker = 'PR' and test_method = 'IHC' then test_result end) as pr,
       MAX(case when biomarker = 'ER/PR' and test_method = 'IHC' then test_result end) as er_pr,
       MAX(case when biomarker = 'HER2' and test_method = 'IHC' then test_result else null end) as her2_ihc,
       MAX(case when biomarker = 'HER2' and test_method = 'FISH' then test_result else null end) as her2_fish,
       case when er='Positive' or pr='Positive' or er_pr = 'Positive' then 'Positive'
            when (er='Negative' and pr='Negative') or er_pr = 'Negative' then 'Negative'
       else NULL end as hr_status
FROM #test_priority
where test_method = 'FISH' or test_method = 'IHC'
group by patient_id, met_date);

drop table if exists #er_pr_her2_wide_current;
create table #er_pr_her2_wide_current as (
select patient_id,
       met_date,
       MAX(case when  biomarker = 'ER' and test_method = 'IHC'then test_result end) as er,
       MAX(case when biomarker = 'PR' and test_method = 'IHC' then test_result end) as pr,
       MAX(case when biomarker = 'ER/PR' and test_method = 'IHC' then test_result end) as er_pr,
       MAX(case when biomarker = 'HER2' and test_method = 'IHC' then test_result else null end) as her2_ihc,
       MAX(case when biomarker = 'HER2' and test_method = 'FISH' then test_result else null end) as her2_fish,
       case when er='Positive' or pr='Positive' or er_pr = 'Positive' then 'Positive'
            when (er='Negative' and pr='Negative') or er_pr = 'Negative' then 'Negative'
       else NULL end as hr_status
FROM #test_priority_current
where test_method = 'FISH' or test_method = 'IHC'
group by patient_id, met_date);


select count(distinct patient_id) from #er_pr_her2_wide; --3697
select count(distinct patient_id) from #er_pr_her2_wide_current; -- 3675

select count(distinct patient_id) from #er_pr_her2_wide
where her2_ihc is not null or her2_fish is not null; --3379

select count(distinct patient_id) from #er_pr_her2_wide_current
where her2_ihc is not null or her2_fish is not null; -- 3178

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Positive' and ((her2_ihc = 'Negative' and (her2_fish = 'Negative' or her2_fish is null)) or (her2_ihc is null and her2_fish = 'Negative')); -- 1170

select count(distinct patient_id) from #er_pr_her2_wide_current
where hr_status = 'Positive' and ((her2_ihc = 'Negative' and (her2_fish = 'Negative' or her2_fish is null)) or (her2_ihc is null and her2_fish = 'Negative')); -- 1247

select * from #er_pr_her2_wide_current
where hr_status = 'Positive' and ((her2_ihc = 'Negative' and (her2_fish = 'Negative' or her2_fish is null)) or (her2_ihc is null and her2_fish = 'Negative')) AND
      patient_id not in (select distinct patient_id from #er_pr_her2_wide
                                where hr_status = 'Positive' and ((her2_ihc = 'Negative' and (her2_fish = 'Negative' or her2_fish is null)) or (her2_ihc is null and her2_fish = 'Negative')));




select * from #er_pr_her2_wide limit 50;


-- -- HR+ HER2low
select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Positive' and (her2_ihc = 'Borderline' and her2_fish = 'Negative'); -- 77, 153 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Positive' and (her2_ihc = 'Borderline' and (her2_fish = 'Negative' or her2_fish is null)); -- 78, 339 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Positive' and ((her2_ihc = 'Borderline' or her2_ihc = 'Low') and her2_fish = 'Negative'); -- 96 counts for HER2-low 1+ and 2+, 211 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Positive' and ((her2_ihc = 'Borderline' or her2_ihc = 'Low') and (her2_fish = 'Negative' or her2_fish is null)); -- 97, 716 with no date restriction


-- -- HR- HER2low
select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Negative' and (her2_ihc = 'Borderline' and her2_fish = 'Negative'); --12, 28 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Negative' and (her2_ihc = 'Borderline' and (her2_fish = 'Negative' or her2_fish is null));--12, 55 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Negative' and ((her2_ihc = 'Borderline' or her2_ihc = 'Low') and her2_fish = 'Negative'); --16 counts for HER2-low 1+ and 2+, 39 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Negative' and ((her2_ihc = 'Borderline' or her2_ihc = 'Low') and (her2_fish = 'Negative' or her2_fish is null)); --16, 114 with no date restriction


-- HR+ HER2-negative
select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Positive' and (her2_ihc = 'Negative' or (her2_ihc is null and her2_fish = 'Negative')); -- 65, 1215 with no date restriction


-- Triple negative
select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Negative' and (her2_ihc = 'Negative' or (her2_ihc is null and her2_fish = 'Negative')); -- 21, 295 with no date restriction



--------------------------------------------------------------------------------------------------------

-- GN360

SET SEARCH_PATH TO 'c3_gn360_202212_breast';

select * from genetic_test limit 50;

select patient_id,
       report_date,
       biomarker_name,
       biomarker_test_type,
       biomarker_variant_type,
       test_result,
       test_result_1_numeric,
       test_result_1_unit,
       test_result_2_numeric,
       test_result_2_unit
FROM genetic_test
    where biomarker = 'ERBB2' AND biomarker_test_type IN ();

select count(distinct patient_id), biomarker_test_type, biomarker_variant_type
FROM genetic_test
WHERE biomarker_name = 'ERBB2' AND biomarker_test_type ='ICC' group by biomarker_test_type, biomarker_variant_type;

select distinct biomarker_test_type FROM genetic_test
WHERE biomarker_name = 'ERBB2';

select count(distinct patient_id), biomarker_test_type FROM genetic_test group by biomarker_test_type;

select distinct test_result_1_numeric, test_result_1_unit from genetic_test where biomarker_name = 'ERBB2' AND biomarker_test_type IN ('Chromogenic In Situ Hybridization','FISH');
select distinct test_result_2_numeric, test_result_2_unit from genetic_test where biomarker_name = 'ERBB2' AND biomarker_test_type IN ('Chromogenic In Situ Hybridization','FISH');
select count(distinct patient_id),test_unit_name
from patient_test
where test_name_name SIMILAR TO '%ERBB2%|%HER2%' and test_value_numeric = '2+'
group by test_unit_name; -- AND test_method_name IN ('Chromogenic In Situ Hybridization','FISH') limit 50;


DROP TABLE IF EXISTS #her2all;
CREATE TABLE #her2all AS (
SELECT *
FROM (
     SELECT patient_id,
            test_name_code,
            test_date,
            test_collection_date,
            test_method_name,
            test_name_name,
            test_value_name,
            test_value_numeric,
            test_unit_name,

            CASE  -- labeling all test methods since some are null when method is specified in test name
                WHEN (lower(test_method_name) SIMILAR TO '%immun%' OR lower(test_name_name) SIMILAR TO '%immun%') THEN 'IHC'
                WHEN (lower(test_method_name) SIMILAR TO '%ish%|%in situ%' OR lower(test_name_name) SIMILAR TO '%ish%|%in situ%') THEN 'FISH'
                ELSE 'Other/Not Specified'
            END AS test_method,

            CASE --cleaning up test results
                WHEN (test_method = 'IHC' AND
                     test_value_name IN ('Borderline','Indeterminate','Intermediate', 'Not Recorded','Low') AND
                     test_value_numeric IN ('1.0','1','1+','1-2')) THEN 'Low'
                WHEN (test_method = 'IHC' AND
                     test_value_name IN ('Borderline','Indeterminate','Intermediate', 'Not Recorded') AND
                     test_value_numeric IN ('2.0','2','2+')) THEN 'Borderline'
                WHEN test_method = 'IHC' AND test_value_name = 'Negative' AND test_value_numeric IN ('1','1+','1.0') THEN 'Low'
                WHEN test_method = 'IHC' AND test_value_name IN ('Borderline', 'Intermediate','Indeterminate','Equivocal') AND test_value_numeric IS NULL THEN 'Borderline'
                WHEN test_value_name IN ('Positive', 'High') THEN 'Positive'
                WHEN test_value_name IN ('Negative', 'Wild Type') THEN 'Negative'
                WHEN test_value_name IN ('Insufficient sample', 'Not Recorded', 'Suppressed', 'Unknown','Not performed') OR
                     test_value_name IS NULL THEN 'Unknown'
                ELSE test_value_name
            END AS test_result,

            CASE
                WHEN test_value_numeric LIKE '%\\+' THEN replace(test_value_numeric,'+','')
                WHEN test_value_numeric IS NULL THEN '000-000-000'
                ELSE test_value_numeric
            END AS test_value_numeric_cleaned, -- cleaning up numeric column, no nulls so can partition

         CASE
                WHEN test_method = 'IHC' AND --if value all negative and one or more have num value 1 but others are null, then mark for change to Low
                     ((MIN(test_value_name) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) =
                      (MAX(test_value_name) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)))
                         AND
                     ((MIN(test_result) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) IN ('Low','Negative') AND
                      (MAX(test_result) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) IN ('Low','Negative'))
                        AND
                    (((MIN(test_value_numeric_cleaned) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name))  = '1' AND
                      (MAX(test_value_numeric_cleaned) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) = '000-000-000')
                         OR
                     ((MIN(test_value_numeric_cleaned) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) = '000-000-000' AND
                      (MAX(test_value_numeric_cleaned) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) = '1')) THEN 'change'

                WHEN ((MIN(test_result) OVER
                        (PARTITION BY patient_id, test_date,test_name_name, test_method_name)) <>
                      (MAX(test_result) OVER
                        (PARTITION BY patient_id, test_date, test_name_name,test_method_name)))
                    THEN 'conflicting'
            ELSE 'none' END AS sameday_ind,

         curation_indicator

     FROM patient_test
     WHERE ((test_name_code in ('18474-7', '72383-3', '31150-6', '49683-6', '74860-8') or
                  (test_name_code = '3430' and LOWER(test_method_name) similar to '(%immun%|%ish%|%in situ%)') or
                  (test_name_code = 'C16152' and
                   lower(test_method_name) similar to '(%immun%|%ish%|%in situ%)' and
                   (lower(genetic_test_type_name) similar to '(copy number%|%amplifi%)' or genetic_test_type_name is null)) or
                  (test_name_code = '48676-1' and (test_value_numeric in ('0', '0+', '1', '1+', '2', '2+', '3', '3+')
                                        or test_value_numeric is null))))
                  ) all_her2

 );
--select count(distinct patient_id) from #her2all;



 UPDATE #her2all
     SET test_result = 'Low',sameday_ind = 'none'
     WHERE sameday_ind = 'change';

-- select * from #her2all limit 10;


DROP TABLE IF EXISTS #her2lowtests;
WITH her2_fish AS ( --identify HER2 FISH tests
     SELECT *
     FROM #her2all
     WHERE test_method = 'FISH' AND sameday_ind = 'none' AND
           (test_value_numeric in ('0','0-1','1','1+','1-2','2','2+','2-3','3','3+') OR test_value_numeric IS NULL) -- get only ones with clean num or null
 )

--identify HER2 IHC tests and filter for IHC tests that would qualify for HER2low (test values of 1 or 2), only 2 for AZ
, her2_ihc AS (
    SELECT *
    FROM #her2all
    WHERE test_method = 'IHC' AND sameday_ind = 'none' AND
          (test_value_numeric in ('0','0+','1','1.0','1+','1-2','2','2.0','2+','2-3','3','3.0','3+') OR test_value_numeric IS NULL)
)

--join valid IHC tests to fish tests to find FISH tests which occurred within 5 days prior and 30 days post IHC test
 , ihc_fish AS (select ihc.patient_id,
                       ihc.test_date   as ihc_test_date,
                       ihc.test_result   as ihc_test_result,
                       ihc.test_method   as ihc_test_method,
                       fish.test_date    as fish_test_date,
                       fish.test_result   as fish_test_result,
                       fish.test_method   as fish_test_method,

                       DATEDIFF(day, ihc_test_date, fish_test_date) as datediffer,

                       dense_rank()
                       over (PARTITION BY ihc.patient_id, ihc.test_date ORDER BY ABS(DATEDIFF(day, ihc.test_date, fish.test_date))) as closest_test
                FROM her2_ihc ihc
                         LEFT JOIN her2_fish fish on ihc.patient_id = fish.patient_id
                where ihc_test_date is not null
)

select *
into #her2lowtests
from ihc_fish where closest_test = 1; --(datediffer between -5 and 120)



-- SELECT * FROM #her2lowtests limit 10; --35288



---------------------------------------
-- ER and PR

DROP TABLE IF EXISTS #hr_her2;
CREATE TABLE #hr_her2 AS (
WITH erpr AS (
SELECT patient_id,
       test_name_name,
       test_method_name,
       test_value_name,
       test_value_numeric,
       test_date,

       CASE -- labeling biomarkers
           WHEN (test_name_code IN ('14228-1', '40556-3', '16112-5') OR
                        (test_name_code = '3467' AND test_method_name ILIKE '%immun%')) THEN 'ER'
           WHEN  (test_name_code IN ('16113-3', '14230-7', '40557-1') OR
                        (test_name_code = '8910' AND test_method_name ILIKE '%immun%')) THEN 'PR'
           WHEN (test_name_name ILIKE '%hormone recept%' AND
                        LOWER(test_method_name) ILIKE '%immun%') THEN 'ER/PR'-- hormone receptor
           END AS biomarker,

       CASE  -- labeling all test methods since some are null when method is specified in test name
           WHEN LOWER(test_method_name) SIMILAR TO '%immun%' OR LOWER(test_name_name) SIMILAR TO '%immun%' THEN 'IHC'
           ELSE 'Not Specified'
       END AS test_method,

       CASE -- cleaning up result category
            WHEN test_value_name IN ('Negative', 'Wild Type') THEN 'Negative'
            WHEN test_value_name IN ('Indeterminate', 'Insufficient sample', 'Not Recorded', 'Suppressed', 'Unknown') THEN 'Unknown'
            WHEN test_value_name IS NULL THEN 'Unknown'
            ELSE test_value_name
       END AS test_result,

       replace(test_value_numeric,'+','') as test_value_numeric_cleaned--, -- cleaning up numeric column

FROM patient_test
WHERE (test_name_code IN ('14228-1', '40556-3', '16112-5') OR (test_name_code = '3467' AND test_method_name ILIKE '%immunohisto%')) --er
      OR (test_name_code IN ('16113-3', '14230-7', '40557-1') OR (test_name_code = '8910' AND test_method_name ILIKE '%immunohisto%')) -- pr
      OR (test_name_name ILIKE '%hormone recept%' AND LOWER(test_method_name) ILIKE 'immun%') -- hormone receptor
UNION
    SELECT patient_id,  -- pulling from condition table
           NULL AS test_name_name,
           NULL AS test_method_name,
           NULL AS test_value_name,
           NULL AS test_value_numeric,
           diagnosis_date AS test_date,
           'ER' AS biomarker,
           'IHC' AS test_method,
           CASE
                    WHEN diagnosis_code_name ILIKE '%positive%' THEN 'Positive'
                    WHEN diagnosis_code_name ILIKE '%negative%' THEN 'Negative'
           END AS  test_result,

           NULL AS test_value_numeric_cleaned

    FROM condition
    WHERE diagnosis_code_code ILIKE '%Z17.%'
), erpr_clean as (
    select *,
           CASE
            WHEN (MIN(test_result) OVER
                      (PARTITION BY patient_id, test_date, test_name_name, test_method_name) <>
                   MAX(test_result) OVER
                       (PARTITION BY patient_id, test_date, test_name_name, test_method_name))
                             THEN 'conflicting'
            ELSE 'none'
       END AS sameday_conflict
    from erpr
),

    her2_tests AS ( --reformatting her2low tests so can union with er and pr tests
        SELECT --IHC
            patient_id,
            'HER2' AS biomarker,
            ihc_test_result AS test_result,
            ihc_test_date AS test_date,
            'IHC' AS test_method
        FROM #her2lowtests
        UNION
        SELECT --FISH
            patient_id,
            'HER2' AS biomarker,
            fish_test_result AS test_result,
            fish_test_date AS test_date,
            'FISH' AS test_method
        FROM #her2lowtests
    )
SELECT
    patient_id,
    biomarker,
    test_result,
    test_date,
    test_method
FROM erpr_clean
WHERE sameday_conflict = 'none'
UNION
SELECT * FROM her2_tests);


-- get closest test dates to met diagnosis
DROP TABLE IF EXISTS #test_priority;
with met_prior AS (
    SELECT patient_id,
           met_date,
           test_date,
           biomarker,
           test_method,
           test_result,

           DATEDIFF(day,met_date, test_date) as datediffer,

           DENSE_RANK() OVER (PARTITION BY patient_id,met_date,biomarker,test_method ORDER BY ABS(DATEDIFF(day,met_date, test_date))) as closest_test

    FROM #hr_her2
    INNER JOIN breast_pts_mets using (patient_id)
    where test_date is not null
)
SELECT *
INTO #test_priority
FROM met_prior
    WHERE closest_test = 1; -- AND (datediffer BETWEEN -5 AND 120) ORDER BY 1,2;

select * from #test_priority order by datediffer asc;



-- pivot to wider
drop table if exists #er_pr_her2_wide;
create table #er_pr_her2_wide as (
select patient_id,
       met_date,
       MAX(case when  biomarker = 'ER' and test_method = 'IHC'then test_result end) as er,
       MAX(case when biomarker = 'PR' and test_method = 'IHC' then test_result end) as pr,
       MAX(case when biomarker = 'ER/PR' and test_method = 'IHC' then test_result end) as er_pr,
       MAX(case when biomarker = 'HER2' and test_method = 'IHC' then test_result else null end) as her2_ihc,
       MAX(case when biomarker = 'HER2' and test_method = 'FISH' then test_result else null end) as her2_fish,
       case when er='Positive' or pr='Positive' or er_pr = 'Positive' then 'Positive'
            when (er='Negative' and pr='Negative') or er_pr = 'Negative' then 'Negative'
       else NULL end as hr_status
FROM #test_priority
where test_method = 'FISH' or test_method = 'IHC'
group by patient_id, met_date);

select count(distinct patient_id) from #er_pr_her2_wide; --2317

select * from #er_pr_her2_wide limit 50;


-- -- HR+ HER2low
select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Positive' and (her2_ihc = 'Borderline' and her2_fish = 'Negative'); -- 77, 153 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Positive' and (her2_ihc = 'Borderline' and (her2_fish = 'Negative' or her2_fish is null)); -- 78, 339 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Positive' and ((her2_ihc = 'Borderline' or her2_ihc = 'Low') and her2_fish = 'Negative'); -- 96 counts for HER2-low 1+ and 2+, 211 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Positive' and ((her2_ihc = 'Borderline' or her2_ihc = 'Low') and (her2_fish = 'Negative' or her2_fish is null)); -- 97, 716 with no date restriction


-- -- HR- HER2low
select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Negative' and (her2_ihc = 'Borderline' and her2_fish = 'Negative'); --12, 28 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Negative' and (her2_ihc = 'Borderline' and (her2_fish = 'Negative' or her2_fish is null));--12, 55 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Negative' and ((her2_ihc = 'Borderline' or her2_ihc = 'Low') and her2_fish = 'Negative'); --16 counts for HER2-low 1+ and 2+, 39 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Negative' and ((her2_ihc = 'Borderline' or her2_ihc = 'Low') and (her2_fish = 'Negative' or her2_fish is null)); --16, 114 with no date restriction


-- HR+ HER2-negative
select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Positive' and (her2_ihc = 'Negative' or (her2_ihc is null and her2_fish = 'Negative')); -- 65, 1215 with no date restriction


-- Triple negative
select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Negative' and (her2_ihc = 'Negative' or (her2_ihc is null and her2_fish = 'Negative')); -- 21, 295 with no date restriction


-----------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------

-- GN360
SET SEARCH_PATH TO 'c3_gn360_202212_breast';
select * from genetic_test where biomarker_name = 'ERBB2' limit 50;
select distinct biomarker_test_type from genetic_test where biomarker_name = 'ERBB2';
select distinct test_result_1_numeric, test_result_1_unit from genetic_test where biomarker_name = 'ERBB2' AND biomarker_test_type IN ('Chromogenic In Situ Hybridization','FISH');
select distinct test_result_2_numeric, test_result_2_unit from genetic_test where biomarker_name = 'ERBB2' AND biomarker_test_type IN ('Chromogenic In Situ Hybridization','FISH');

select distinct test_result_1_numeric, test_result_1_unit from genetic_test where biomarker_name = 'ERBB2' AND biomarker_test_type ='IHC';
select distinct test_result_2_numeric, test_result_2_unit from genetic_test where biomarker_name = 'ERBB2' AND biomarker_test_type ='IHC';

select distinct test_result_1_unit from genetic_test where biomarker_name = 'ERBB2' and test_result_1_numeric in ('0','0.0','1','1.0','2','2.0','3','3.0');
select distinct test_result, test_result_1_unit ,test_result_1_numeric,test_result_2_unit ,test_result_2_numeric from genetic_test where biomarker_name = 'ERBB2' and test_result_1_numeric in ('0','0.0','1','1.0','2','2.0','3','3.0') AND biomarker_test_type IN ('Chromogenic In Situ Hybridization','FISH');
select distinct test_result, test_result_1_unit ,test_result_1_numeric,test_result_2_unit ,test_result_2_numeric from genetic_test where biomarker_name = 'ERBB2' and biomarker_test_type = 'IHC';


select count(distinct patient_id),test_unit_name
from patient_test
where test_name_name SIMILAR TO '%ERBB2%|%HER2%' and test_value_numeric = '2+'
group by test_unit_name; -- AND test_method_name IN ('Chromogenic In Situ Hybridization','FISH') limit 50;



DROP TABLE IF EXISTS #her2all_g;
CREATE TABLE #her2all_g AS (
WITH her2 AS (SELECT patient_id,
                     test_date,
                     'HER2' AS biomarker,
                     test_collection_date,
                     test_method_name,
                     test_name_name,
                     test_value_name,
                     test_value_numeric,
                     test_unit_name
              FROM patient_test

              WHERE ((test_name_code in ('18474-7', '72383-3', '31150-6', '49683-6', '74860-8') or
                      (test_name_code = '3430' and LOWER(test_method_name) similar to '(%immun%|%ish%|%in situ%)') or
                      (test_name_code = 'C16152' and
                       lower(test_method_name) similar to '(%immun%|%ish%|%in situ%)' and
                       (lower(genetic_test_type_name) similar to '(copy number%|%amplifi%)' or
                        genetic_test_type_name is null)) or
                      (test_name_code = '48676-1' and
                       (test_value_numeric in ('0', '0+', '1', '1+', '2', '2+', '3', '3+')
                           or test_value_numeric is null))))
              UNION

              SELECT patient_id,
                     report_date             AS test_date,
                     'HER2'                  AS biomarker,
                     specimen_collected_date AS test_collection_date,
                     biomarker_test_type     AS test_method_name,
                     biomarker_name          AS test_name_name,
                     test_result             AS test_value_name,
                     test_result_1_numeric   AS test_value_numeric,
                     test_result_1_unit      AS test_unit_name

              FROM genetic_test
              WHERE biomarker_name = 'ERBB2'
                AND LOWER(biomarker_test_type) SIMILAR TO '%(ihc|in situ|ish)%'
--                 AND test_value_numeric IN ('0', '0.0', '1', '1.0', '2', '2.0', '3', '3.0')
                AND test_unit_name IN
                    ('Staining Intensity (+)', 'ERBB2:CEP17', 'HER2/CEP-17 ratio', 'Her2/Neu/Chromosome 17 Ratio')

              UNION

              SELECT patient_id,
                     report_date             AS test_date,
                     'HER2'                  AS biomarker,
                     specimen_collected_date AS test_collection_date,
                     biomarker_test_type     AS test_method_name,
                     biomarker_name          AS test_name_name,
                     test_result             AS test_value_name,
                     test_result_2_numeric   AS test_value_numeric,
                     test_result_2_unit      AS test_unit_name

              FROM genetic_test
              WHERE biomarker_name = 'ERBB2'
                AND LOWER(biomarker_test_type) SIMILAR TO '%(ihc|in situ|ish)%'
--                 AND test_value_numeric IN ('0', '0.0', '1', '1.0', '2', '2.0', '3', '3.0')
                AND test_unit_name IN
                    ('Staining Intensity (+)', 'ERBB2:CEP17', 'HER2/CEP-17 ratio', 'Her2/Neu/Chromosome 17 Ratio')
)
    SELECT *,

            CASE  -- labeling all test methods since some are null when method is specified in test name
                WHEN (lower(test_method_name) SIMILAR TO '%(immun|ihc)%' OR lower(test_name_name) SIMILAR TO '%(immun|ihc)%') THEN 'IHC'
                WHEN (lower(test_method_name) SIMILAR TO '%ish%|%in situ%' OR lower(test_name_name) SIMILAR TO '%ish%|%in situ%') THEN 'FISH'
                ELSE 'Other/Not Specified'
            END AS test_method,

            CASE --cleaning up test results
                WHEN (test_method = 'IHC' AND
                     test_value_name IN ('Borderline','Indeterminate','Intermediate', 'Not Recorded','Low') AND
                     test_value_numeric IN ('1.0','1','1+','1-2')) THEN 'Low'
                WHEN (test_method = 'IHC' AND
                     test_value_name IN ('Borderline','Indeterminate','Intermediate', 'Not Recorded') AND
                     test_value_numeric IN ('2.0','2','2+')) THEN 'Borderline'
                WHEN test_method = 'IHC' AND test_value_name = 'Negative' AND test_value_numeric IN ('1','1+','1.0') THEN 'Low'
                WHEN test_method = 'IHC' AND test_value_name IN ('Borderline', 'Intermediate','Indeterminate','Equivocal') AND test_value_numeric IS NULL THEN 'Borderline'
                WHEN test_value_name IN ('Positive', 'High') THEN 'Positive'
                WHEN test_value_name IN ('Negative', 'Wild Type') THEN 'Negative'
                WHEN test_value_name IN ('Insufficient sample', 'Not Recorded', 'Suppressed', 'Unknown','Not performed') OR
                     test_value_name IS NULL THEN 'Unknown'
                ELSE test_value_name
            END AS test_result,

            CASE
                WHEN test_value_numeric LIKE '%\\+' THEN replace(test_value_numeric,'+','')
                WHEN test_value_numeric IS NULL THEN '000-000-000'
                ELSE test_value_numeric
            END AS test_value_numeric_cleaned, -- cleaning up numeric column, no nulls so can partition

         CASE
                WHEN test_method = 'IHC' AND --if value all negative and one or more have num value 1 but others are null, then mark for change to Low
                     ((MIN(test_value_name) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) =
                      (MAX(test_value_name) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)))
                         AND
                     ((MIN(test_result) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) IN ('Low','Negative') AND
                      (MAX(test_result) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) IN ('Low','Negative'))
                        AND
                    (((MIN(test_value_numeric_cleaned) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name))  = '1' AND
                      (MAX(test_value_numeric_cleaned) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) = '000-000-000')
                         OR
                     ((MIN(test_value_numeric_cleaned) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) = '000-000-000' AND
                      (MAX(test_value_numeric_cleaned) OVER
                        (PARTITION BY patient_id, test_date, test_name_name, test_method_name)) = '1')) THEN 'change'

                WHEN ((MIN(test_result) OVER
                        (PARTITION BY patient_id, test_date,test_name_name, test_method_name)) <>
                      (MAX(test_result) OVER
                        (PARTITION BY patient_id, test_date, test_name_name,test_method_name)))
                    THEN 'conflicting'
            ELSE 'none' END AS sameday_ind

     FROM her2


 );
select count(distinct patient_id) from #her2all_g; -- 2088
-- select * from #her2all_g order by 1,2;



 UPDATE #her2all_g
     SET test_result = 'Low',sameday_ind = 'none'
     WHERE sameday_ind = 'change';

-- select * from #her2all limit 10;


DROP TABLE IF EXISTS #her2lowtests_g;
WITH her2_fish AS ( --identify HER2 FISH tests
     SELECT *
     FROM #her2all_g
     WHERE test_method = 'FISH' AND sameday_ind = 'none' AND
           (test_value_numeric in ('0','0-1','1','1+','1-2','2','2+','2-3','3','3+') OR test_value_numeric IS NULL) -- get only ones with clean num or null
 )

--identify HER2 IHC tests and filter for IHC tests that would qualify for HER2low (test values of 1 or 2), only 2 for AZ
, her2_ihc AS (
    SELECT *
    FROM #her2all_g
    WHERE test_method = 'IHC' AND sameday_ind = 'none' AND
          (test_value_numeric in ('0','0+','1','1.0','1+','1-2','2','2.0','2+','2-3','3','3.0','3+') OR test_value_numeric IS NULL)
)

--join valid IHC tests to fish tests to find FISH tests which occurred within 5 days prior and 30 days post IHC test
 , ihc_fish AS (select ihc.patient_id,
                       ihc.test_date   as ihc_test_date,
                       ihc.test_result   as ihc_test_result,
                       ihc.test_method   as ihc_test_method,
                       fish.test_date    as fish_test_date,
                       fish.test_result   as fish_test_result,
                       fish.test_method   as fish_test_method,

                       DATEDIFF(day, ihc_test_date, fish_test_date) as datediffer,

                       dense_rank()
                       over (PARTITION BY ihc.patient_id, ihc.test_date ORDER BY ABS(DATEDIFF(day, ihc.test_date, fish.test_date))) as closest_test
                FROM her2_ihc ihc
                         LEFT JOIN her2_fish fish on ihc.patient_id = fish.patient_id
                where ihc_test_date is not null
)

select *
into #her2lowtests_g
from ihc_fish where closest_test = 1; --(datediffer between -5 and 120)



---------------------------------------
-- ER and PR

DROP TABLE IF EXISTS #hr_her2_g;
CREATE TABLE #hr_her2_g AS (
WITH erpr AS (
SELECT patient_id,
       test_date,
       test_collection_date,
       test_method_name,
       test_name_name,
       test_value_name,
       test_value_numeric,
       test_unit_name,


       CASE -- labeling biomarkers
           WHEN (test_name_code IN ('14228-1', '40556-3', '16112-5') OR
                        (test_name_code = '3467' AND test_method_name ILIKE '%immun%')) THEN 'ER'
           WHEN  (test_name_code IN ('16113-3', '14230-7', '40557-1') OR
                        (test_name_code = '8910' AND test_method_name ILIKE '%immun%')) THEN 'PR'
           WHEN (test_name_name ILIKE '%hormone recept%' AND
                        LOWER(test_method_name) ILIKE '%immun%') THEN 'ER/PR'-- hormone receptor
           END AS biomarker

       FROM patient_test

       WHERE (test_name_code IN ('14228-1', '40556-3', '16112-5') OR (test_name_code = '3467' AND test_method_name ILIKE '%immunohisto%')) --er
            OR (test_name_code IN ('16113-3', '14230-7', '40557-1') OR (test_name_code = '8910' AND test_method_name ILIKE '%immunohisto%')) -- pr
            OR (test_name_name ILIKE '%hormone recept%' AND LOWER(test_method_name) ILIKE 'immun%') -- hormone receptor

    UNION

              SELECT patient_id,
                     report_date             AS test_date,
                     specimen_collected_date AS test_collection_date,
                     biomarker_test_type     AS test_method_name,
                     biomarker_name          AS test_name_name,
                     test_result             AS test_value_name,
                     test_result_2_numeric   AS test_value_numeric,
                     test_result_2_unit      AS test_unit_name,

                     CASE
                         WHEN test_name_name = 'ESR1' THEN 'ER'
                         WHEN test_name_name = 'PGR' THEN 'PR'
                     END AS biomarker

              FROM genetic_test

              WHERE biomarker_name SIMILAR TO '(ESR1|PGR)' AND
                    biomarker_test_type SIMILAR TO '(IHC|ICC)'

    UNION
            SELECT patient_id,  -- pulling from condition table
                   diagnosis_date AS test_date,
                   NULL AS test_date_collected,
                   'IHC' AS test_method_name,
                   NULL AS test_name_name,

                   CASE
                       WHEN diagnosis_code_name ILIKE '%positive%' THEN 'Positive'
                       WHEN diagnosis_code_name ILIKE '%negative%' THEN 'Negative'
                   END AS  test_value_name,

                   NULL AS test_value_numeric,
                   NULL AS test_unit_name,
                   'ER' AS biomarker

    FROM condition
    WHERE diagnosis_code_code ILIKE '%Z17.%'



), erpr_clean as (
    select *,

           CASE  -- labeling all test methods since some are null when method is specified in test name
                WHEN LOWER(test_method_name) SIMILAR TO '%immun%' OR LOWER(test_name_name) SIMILAR TO '%immun%' THEN 'IHC'
                ELSE 'Other/Not Specified'
           END AS test_method,

           CASE -- cleaning up result category
                WHEN test_value_name IN ('Negative', 'Wild Type') THEN 'Negative'
                WHEN test_value_name IN ('Indeterminate', 'Insufficient sample', 'Not Recorded', 'Suppressed', 'Unknown') THEN 'Unknown'
                WHEN test_value_name IS NULL THEN 'Unknown'
                ELSE test_value_name
           END AS test_result,

           replace(test_value_numeric,'+','') as test_value_numeric_cleaned,--, -- cleaning up numeric column

           CASE
                WHEN (MIN(test_result) OVER
                      (PARTITION BY patient_id, test_date, test_name_name, test_method_name) <>
                   MAX(test_result) OVER
                       (PARTITION BY patient_id, test_date, test_name_name, test_method_name))
                             THEN 'conflicting'
                ELSE 'none'
           END AS sameday_conflict

        FROM erpr
),

    her2_tests AS ( --reformatting her2low tests so can union with er and pr tests
        SELECT --IHC
            patient_id,
            'HER2' AS biomarker,
            ihc_test_result AS test_result,
            ihc_test_date AS test_date,
            'IHC' AS test_method
        FROM #her2lowtests_g
        UNION
        SELECT --FISH
            patient_id,
            'HER2' AS biomarker,
            fish_test_result AS test_result,
            fish_test_date AS test_date,
            'FISH' AS test_method
        FROM #her2lowtests_g
    )
SELECT
    patient_id,
    biomarker,
    test_result,
    test_date,
    test_method
FROM erpr_clean
WHERE sameday_conflict = 'none'
UNION
SELECT * FROM her2_tests);


DROP TABLE IF EXISTS #test_priority;
with met_prior AS (
    SELECT patient_id,
           met_date,
           test_date,
           biomarker,
           test_method,
           test_result,

           DATEDIFF(day,met_date, test_date) as datediffer,

           DENSE_RANK() OVER (PARTITION BY patient_id,met_date,biomarker,test_method ORDER BY ABS(DATEDIFF(day,met_date, test_date))) as closest_test

    FROM #hr_her2_g
    INNER JOIN breast_pts_mets using (patient_id)
    where test_date is not null
)
SELECT *
INTO #test_priority
FROM met_prior
    WHERE closest_test = 1; -- AND (datediffer BETWEEN -5 AND 120) ORDER BY 1,2;

-- select * from #test_priority order by datediffer asc;



-- pivot to wider
drop table if exists #er_pr_her2_wide;
create table #er_pr_her2_wide as (
select patient_id,
       met_date,
       MAX(case when  biomarker = 'ER' and test_method = 'IHC'then test_result end) as er,
       MAX(case when biomarker = 'PR' and test_method = 'IHC' then test_result end) as pr,
       MAX(case when biomarker = 'ER/PR' and test_method = 'IHC' then test_result end) as er_pr,
       MAX(case when biomarker = 'HER2' and test_method = 'IHC' then test_result else null end) as her2_ihc,
       MAX(case when biomarker = 'HER2' and test_method = 'FISH' then test_result else null end) as her2_fish,
       case when er='Positive' or pr='Positive' or er_pr = 'Positive' then 'Positive'
            when (er='Negative' and pr='Negative') or er_pr = 'Negative' then 'Negative'
       else NULL end as hr_status
FROM #test_priority
where test_method = 'FISH' or test_method = 'IHC'
group by patient_id, met_date);

-- select count(distinct patient_id) from #er_pr_her2_wide; --1033

select * from #er_pr_her2_wide limit 50;


-- -- HR+ HER2low
select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Positive' and (her2_ihc = 'Borderline' and her2_fish = 'Negative'); -- 47 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Positive' and (her2_ihc = 'Borderline' and (her2_fish = 'Negative' or her2_fish is null)); --  99 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Positive' and ((her2_ihc = 'Borderline' or her2_ihc = 'Low') and her2_fish = 'Negative'); --  counts for HER2-low 1+ and 2+, 60 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Positive' and ((her2_ihc = 'Borderline' or her2_ihc = 'Low') and (her2_fish = 'Negative' or her2_fish is null)); -- 219 with no date restriction


-- -- HR- HER2low
select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Negative' and (her2_ihc = 'Borderline' and her2_fish = 'Negative'); -- 99 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Negative' and (her2_ihc = 'Borderline' and (her2_fish = 'Negative' or her2_fish is null));-- 60 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Negative' and ((her2_ihc = 'Borderline' or her2_ihc = 'Low') and her2_fish = 'Negative'); -- counts for HER2-low 1+ and 2+, 11 with no date restriction

select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Negative' and ((her2_ihc = 'Borderline' or her2_ihc = 'Low') and (her2_fish = 'Negative' or her2_fish is null)); --8 with no date restriction


-- HR+ HER2-negative
select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Positive' and (her2_ihc = 'Negative' or (her2_ihc is null and her2_fish = 'Negative')); -- 358 with no date restriction


-- Triple negative
select count(distinct patient_id) from #er_pr_her2_wide
where hr_status = 'Negative' and (her2_ihc = 'Negative' or (her2_ihc is null and her2_fish = 'Negative')); -- 115 with no date restriction


select distinct test_method_name from c3_pt360_202212_breast.patient_test where test_name_name SIMILAR TO '%(ERBB2|HER2)%'







