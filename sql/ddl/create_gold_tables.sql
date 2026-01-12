-- ------------------------------------------------------
-- -- GOLD LAYER: DIMENSION TABLES
-- ------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_student (
    student_key     INTEGER PRIMARY KEY,
    student_id      VARCHAR,
    student_name    VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_teacher (
    teacher_key     INTEGER PRIMARY KEY,
    teacher_id      VARCHAR,
    teacher_name    VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_school (
    school_key      INTEGER PRIMARY KEY,
    school_id       VARCHAR,
    school_name     VARCHAR,
    municipality    VARCHAR
);


-- ------------------------------------------------------
-- -- FACT TABLE
-- ------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_test_results (
    test_result_key INTEGER PRIMARY KEY,
    student_key     INTEGER,
    teacher_key     INTEGER,
    school_key      INTEGER,
    test_key        INTEGER,
    exam_date       DATE,
    score           DOUBLE,

    FOREIGN KEY (student_key) REFERENCES dim_student(student_key),
    FOREIGN KEY (teacher_key) REFERENCES dim_teacher(teacher_key),
    FOREIGN KEY (school_key)  REFERENCES dim_school(school_key),
    FOREIGN KEY (test_key)    REFERENCES dim_test(test_key)
);


