
drop table {schema}.on_run_hook if exists;

create table {schema}.on_run_hook (
    test_state       VARCHAR(2000), -- start|end
    target_dbname    VARCHAR(2000),
    target_host      VARCHAR(2000),
    target_name      VARCHAR(2000),
    target_schema    VARCHAR(2000),
    target_type      VARCHAR(2000),
    target_user      VARCHAR(2000),
    target_pass      VARCHAR(2000),
    target_threads   INTEGER,
    run_started_at   VARCHAR(2000),
    invocation_id    VARCHAR(2000),
    thread_id        VARCHAR(2000)
);
