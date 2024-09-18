
-- Query to find the percentage of KRAS mutant patients with secondary RAS mutations

with ras_mut AS ( -- cleaning up molecular variant field and getting num of results based on variant
    SELECT
        patient_id,
        biomarker_variant_type_name,
        biomarker_name_name,
        report_date,
        CASE
            when molecular_variant ~* 'G12D|G21D|35G>A|35>A' then 'p.G12D'
            when molecular_variant ~* 'G12V|35G>T' then 'p.G12V'
            when molecular_variant ~* 'G12R' then 'p.G12R'
            when molecular_variant ~* 'c.35G>C|p.G12A' then 'p.G12A'
            when molecular_variant ~* 'c.34G>T|p.G12C' then 'p.G12C'
            when molecular_variant ~* 'Q61H|183a' then 'p.Q61H'
            when molecular_variant ~* 'Q61K' then 'p.Q61K'
            when molecular_variant ~* 'Q61R|Q16R' then 'p.Q61R'
            when molecular_variant ~* 'Q61L' then 'p.Q61L'
            when molecular_variant ~* 'G13D' then 'p.G13D'
            when molecular_variant ~* 'T58I' then 'p.T58I'
            when molecular_variant ~* 'G12L' then 'p.G12L'
            when molecular_variant ~* 'A146T' then 'p.A146T'
            when molecular_variant ~* 'exon 2|exon2' and molecular_variant !~* 'G393D|G12' then 'Exon 2'
            when molecular_variant is null and biomarker_variant_type_name ='Gene Amplification' then 'GeneAmp'
            when molecular_variant is null and biomarker_variant_type_name = 'Gene Rearrangement Abnormality' then 'GeneRearrang'
            else molecular_variant
        end as variant,
        dense_rank() over (PARTITION BY patient_id,biomarker_name_name,biomarker_variant_type_name order by variant) as rank_variant_types
    FROM biomarker_table
    WHERE biomarker_name_name ILIKE '%RAS'
      AND test_result_name IN ('Positive', 'Equivocal')
      AND biomarker_variant_type_name IN ('Gene Mutation', 'Gene Deletion', 'Deletion Mutation', 'Gene Amplification',
                                          'Gene Rearrangement Abnormality', 'Exon Deletion', 'Exon Duplication')
    and (variant_classification_name is null or variant_classification_name !~* 'benign|unknown|uncertain')

    order by 1
),
   clean_up as ( -- clean up the ranks by fixing those with null and non-nulls on same report date that were being counted separately
                SELECT *,
                       CASE
                           WHEN variant is null AND rank_variant_types = 2 THEN 0
                               THEN 1 --these have more than 2 different report dates with same result, so need to be changed to count of 1
                           WHEN variant is not null AND rank_variant_types = 1 THEN 1
                           WHEN variant is not null AND rank_variant_types = 2 THEN 1
                           END AS correct_tab
                FROM ras_mut where patient_id in (
                    select distinct patient_id from ras_mut where rank_variant_types > 1) --pulling out those that have > 1 rank based on report date
),
   distinct_var as ( --get distinct biomarker and mol var for each patient
    select distinct patient_id,biomarker_name_name, variant,
          1 as correct_tab -- some only have one record with null mol var so counting each as 1
    from ras_mut where patient_id not in (select distinct patient_id from clean_up)
    union
    select distinct patient_id,biomarker_name_name, variant, correct_tab from clean_up order by 1
)
   select ( (
   select count(*) from (
            select sum(correct_tab),patient_id from distinct_var GROUP BY 2 having sum(correct_tab) > 1) x) * 100.0  /
           (select count(distinct patient_id) from ras_mut) ); --3.47%


------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- co amplification of RAS
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--- patients with co-amplification of RAS --
-- counts of patients with specific mutations and amplified RAS

select
         CASE
            when molecular_variant ~* 'G12D|G21D|35G>A|35>A' then 'p.G12D'
            when molecular_variant ~* 'G12V|35G>T' then 'p.G12V'
            when molecular_variant ~* 'G12R' then 'p.G12R'
            when molecular_variant ~* 'c.35G>C|p.G12A' then 'p.G12A'
            when molecular_variant ~* 'c.34G>T|p.G12C' then 'p.G12C'
            when molecular_variant ~* 'Q61H|183a' then 'p.Q61H'
            when molecular_variant ~* 'Q61K' then 'p.Q61K'
            when molecular_variant ~* 'Q61R|Q16R' then 'p.Q61R'
            when molecular_variant ~* 'Q61L' then 'p.Q61L'
            when molecular_variant ~* 'G13D' then 'p.G13D'
            when molecular_variant ~* 'T58I' then 'p.T58I'
            when molecular_variant ~* 'G12L' then 'p.G12L'
            when molecular_variant ~* 'A146T' then 'p.A146T'
            when molecular_variant ~* 'exon 2|exon2' and molecular_variant !~* 'G393D|G12' then 'Exon 2'
            when molecular_variant is null and biomarker_variant_type_name ='Gene Amplification' then 'GeneAmp'
            when molecular_variant is null and biomarker_variant_type_name = 'Gene Rearrangement Abnormality' then 'GeneRearrang'
            else molecular_variant
        end as variant,
       count(distinct patient_id)
from biomarker_table
where biomarker_name_name = 'KRAS'
      and biomarker_variant_type_name = 'Gene Mutation'
      and test_result_name in ('Positive','Equivocal')
      and (variant_classification_name is null or variant_classification_name !~* 'benign|unknown|uncertain')
      and patient_id in (
                                select distinct patient_id
                                from c3_pt360_202406_pancreatic.biomarker
                                where biomarker_name_name like '%RAS'
                                  and test_result_name in ('Positive','Equivocal','Intermediate','Low')
                                  and biomarker_variant_type_name = 'Gene Amplification'
                                ) -- copy number polymorphism always negative, so did not add here
group by 1;
-- null,1
-- p.G12D,9
-- p.G12R,5
-- p.G12V,12
-- p.G13C,1
-- p.Q61H,3

-- percentage for KRAS mut pts with amplified RAS  --2.75%
select ((
select count(distinct patient_id)
from biomarker_table
where biomarker_name_name = 'KRAS'
      and biomarker_variant_type_name = 'Gene Mutation'
      and test_result_name in ('Positive','Equivocal')
      and (variant_classification_name is null or variant_classification_name !~* 'benign|unknown|uncertain')
      and patient_id in (
                                select distinct patient_id
                                from biomarker_table
                                where biomarker_name_name like '%RAS'
                                  and test_result_name in ('Positive','Equivocal','Intermediate','Low')
                                  and biomarker_variant_type_name = 'Gene Amplification'
                                ) -- copy number polymorphism always negative, so did not add here
) * 100.0 / (select count(distinct patient_id)
from biomarker_table
where biomarker_name_name = 'KRAS'
      and biomarker_variant_type_name = 'Gene Mutation'
      and test_result_name in ('Positive','Equivocal')
      and (variant_classification_name is null or variant_classification_name !~* 'benign|unknown|uncertain'))) ; --2.75%



------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- WT for amplification of KRAS --
select CASE
            when molecular_variant ~* 'G12D|G21D|35G>A|35>A' then 'p.G12D'
            when molecular_variant ~* 'G12V|35G>T' then 'p.G12V'
            when molecular_variant ~* 'G12R' then 'p.G12R'
            when molecular_variant ~* 'c.35G>C|p.G12A' then 'p.G12A'
            when molecular_variant ~* 'c.34G>T|p.G12C' then 'p.G12C'
            when molecular_variant ~* 'Q61H|183a' then 'p.Q61H'
            when molecular_variant ~* 'Q61K' then 'p.Q61K'
            when molecular_variant ~* 'Q61R|Q16R' then 'p.Q61R'
            when molecular_variant ~* 'Q61L' then 'p.Q61L'
            when molecular_variant ~* 'G13D' then 'p.G13D'
            when molecular_variant ~* 'T58I' then 'p.T58I'
            when molecular_variant ~* 'G12L' then 'p.G12L'
            when molecular_variant ~* 'A146T' then 'p.A146T'
            when molecular_variant ~* 'exon 2|exon2' and molecular_variant !~* 'G393D|G12' then 'Exon 2'
            when molecular_variant is null and biomarker_variant_type_name ='Gene Amplification' then 'GeneAmp'
            when molecular_variant is null and biomarker_variant_type_name = 'Gene Rearrangement Abnormality' then 'GeneRearrang'
            else molecular_variant
        end as variant,
       count(distinct patient_id)
from biomarker_table
where biomarker_name_name = 'KRAS'
      and biomarker_variant_type_name = 'Gene Mutation'
      and test_result_name in ('Positive','Equivocal')
      and (variant_classification_name is null or variant_classification_name !~* 'benign|unknown|uncertain')
      and patient_id in (
                                select distinct patient_id
                                from biomarker_table
                                where biomarker_name_name = 'KRAS'
                                  and test_result_name = 'Negative'
                                  and biomarker_variant_type_name in ('Gene Amplification','Copy Number Polymorphism')
                                EXCEPT
                                select distinct patient_id
                                from biomarker_table
                                where biomarker_name_name = 'KRAS'
                                  and test_result_name in ('Positive','Equivocal','Intermediate','Low')
                                  and biomarker_variant_type_name = 'Gene Amplification'
                                )
group by 1;

-- null,111
-- c.176C>A;p.A59E,1
-- c.35_36GT>AG;p.G12E,1
-- p.A146S,1
-- p.A146T,1
-- p.D33E,1
-- p.G12A,1
-- p.G12C,10
-- p.G12D,245
-- p.G12L,1
-- p.G12R,82
-- p.G12V,190
-- p.G13D,4
-- p.G13E,1
-- p.G60S,1
-- p.L19F,1
-- p.Q61H,29
-- p.Q61K,2
-- p.Q61L,2
-- p.Q61R,9
-- p.T58I,1
-- p.V14L,1

-- percentage of KRAS mut pts with no amplification (WT for amplification) -- 55.3%
select (select count(distinct patient_id)
from biomarker_table
where biomarker_name_name = 'KRAS'
      and biomarker_variant_type_name = 'Gene Mutation'
      and test_result_name in ('Positive','Equivocal')
      and (variant_classification_name is null or variant_classification_name !~* 'benign|unknown|uncertain')
      and patient_id in (
                                select distinct patient_id
                                from biomarker_table
                                where biomarker_name_name = 'KRAS'
                                  and test_result_name = 'Negative'
                                  and biomarker_variant_type_name in ('Gene Amplification','Copy Number Polymorphism')
                                EXCEPT
                                select distinct patient_id
                                from biomarker_table
                                where biomarker_name_name = 'KRAS'
                                  and test_result_name in ('Positive','Equivocal','Intermediate','Low')
                                  and biomarker_variant_type_name = 'Gene Amplification'
                                )
) * 100.0 / (select count(distinct patient_id)
from biomarker_table
where biomarker_name_name = 'KRAS'
      and biomarker_variant_type_name = 'Gene Mutation'
      and test_result_name in ('Positive','Equivocal')
      and (variant_classification_name is null or variant_classification_name !~* 'benign|unknown|uncertain')); -- 55.3%