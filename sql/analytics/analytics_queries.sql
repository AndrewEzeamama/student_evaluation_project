-- 1. Average Score by School

SELECT
    s.school_name,
    ROUND(AVG(f.score), 2) AS avg_score
FROM fact_test_results f
JOIN dim_school s
  ON f.school_id = s.school_id
GROUP BY s.school_name
ORDER BY avg_score DESC;

-- 2. Student Performance Distribution

SELECT
    grading_group,
    COUNT(*) AS student_count
FROM fact_test_results
GROUP BY grading_group
ORDER BY grading_group;

-- 3. Top 10 Performing Students

SELECT
    st.student_id,
    st.student_name,
    ROUND(AVG(f.score), 2) AS avg_score
FROM fact_test_results f
JOIN dim_student st
  ON f.student_id = st.student_id
GROUP BY st.student_id, st.student_name
ORDER BY avg_score DESC
LIMIT 10;

--  4. Teacher Effectiveness (Average Student Score)

SELECT
    t.teacher_name,
    ROUND(AVG(f.score), 2) AS avg_student_score
FROM fact_test_results f
JOIN dim_teacher t
  ON f.teacher_id = t.teacher_id
GROUP BY t.teacher_name
ORDER BY avg_student_score DESC;

--  5. School Pass Rate (Score ≥ 55)

SELECT
    s.school_name,
    ROUND(
        100.0 * SUM(CASE WHEN f.score >= 55 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS pass_rate_percent
FROM fact_test_results f
JOIN dim_school s
  ON f.school_id = s.school_id
GROUP BY s.school_name
ORDER BY pass_rate_percent DESC;

-- 6. Test Difficulty Analysis

SELECT
    t.test_name,
    ROUND(AVG(f.score), 2) AS avg_score,
    ROUND(STDDEV(f.score), 2) AS score_stddev
FROM fact_test_results f
JOIN dim_test t
  ON f.test_id = t.test_id
GROUP BY t.test_name
ORDER BY avg_score ASC;

-- 7. Trend Analysis (Scores Over Time)

SELECT
    DATE_TRUNC('month', exam_date) AS exam_month,
    ROUND(AVG(score), 2) AS avg_score
FROM fact_test_results
GROUP BY exam_month
ORDER BY exam_month;