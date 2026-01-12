CREATE TABLE IF NOT EXISTS audit_pipeline_runs (
    run_id            VARCHAR,
    stage             VARCHAR,
    status            VARCHAR,
    record_count      BIGINT,
    row_count INTEGER,
    valid_count       BIGINT,
    invalid_count     BIGINT,
    error_message     VARCHAR,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS audit_data_quality (
    run_id VARCHAR,
    dataset VARCHAR,
    check_type VARCHAR,
    status VARCHAR,
    failed_records INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS audit_dq_violations (
    run_id            VARCHAR,
    stage             VARCHAR,
    entity            VARCHAR,
    rule_type         VARCHAR,
    column_name       VARCHAR,
    violation_count   BIGINT,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


