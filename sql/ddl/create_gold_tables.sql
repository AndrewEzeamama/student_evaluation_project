------------------------------------------------------
-- GOLD LAYER: DIMENSION TABLES
------------------------------------------------------

CREATE OR REPLACE TABLE dim_student AS
SELECT
    student_id,
    student_name
FROM read_parquet('data/silver/students.parquet');

CREATE OR REPLACE TABLE dim_teacher AS
SELECT
    teacher_id,
    teacher_name
FROM read_parquet('data/silver/teachers.parquet');

CREATE OR REPLACE TABLE dim_school AS
SELECT
    school_id,
    school_name
FROM read_parquet('data/silver/schools.parquet');

CREATE OR REPLACE TABLE dim_test AS
SELECT
    test_id,
    test_name
FROM read_parquet('data/silver/tests.parquet');

------------------------------------------------------
-- OPTIONAL: GRADING GROUP DIMENSION
------------------------------------------------------

CREATE OR REPLACE TABLE dim_grading_group AS
SELECT
    CASE
        WHEN score >= 85 THEN 'A'
        WHEN score >= 70 THEN 'B'
        WHEN score >= 55 THEN 'C'
        WHEN score >= 40 THEN 'D'
        ELSE 'F'
    END AS grading_group,
    MIN(score) AS min_score,
    MAX(score) AS max_score
FROM read_parquet('data/silver/test_details.parquet')
GROUP BY grading_group;

------------------------------------------------------
-- FACT TABLE
------------------------------------------------------

CREATE OR REPLACE TABLE fact_test_results AS
SELECT
    ev.student_id,
    ev.teacher_id,
    ev.school_id,
    ev.test_id,
    td.exam_date,
    td.score,
    CASE
        WHEN td.score >= 85 THEN 'A'
        WHEN td.score >= 70 THEN 'B'
        WHEN td.score >= 55 THEN 'C'
        WHEN td.score >= 40 THEN 'D'
        ELSE 'F'
    END AS grading_group
FROM read_parquet('data/silver/student_evaluation_cleaned.parquet') ev
JOIN read_parquet('data/silver/test_details.parquet') td
  ON ev.test_id = td.test_id;
