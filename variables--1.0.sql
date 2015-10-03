CREATE TABLE sys_variables (
  variable TEXT,
  value TEXT,
  backend_pid INT4,
  backend_start timestamptz
);
CREATE UNIQUE INDEX sys_variables_global_variable ON sys_variables (variable) WHERE backend_pid IS NULL;
CREATE UNIQUE INDEX sys_variables_session_variable ON sys_variables (variable, backend_pid, backend_start) WHERE backend_pid IS NOT NULL;
 
CREATE OR REPLACE FUNCTION get_variable( IN p_variable TEXT )
RETURNS TEXT
AS $$
SELECT sv.value FROM pg_stat_get_activity(pg_backend_pid()) as x join sys_variables sv on x.pid = sv.backend_pid AND x.backend_start = sv.backend_start WHERE backend_pid IS NOT NULL AND variable = p_variable
UNION
SELECT sv.value FROM sys_variables sv WHERE backend_pid IS NULL AND variable = p_variable
LIMIT 1;
$$
LANGUAGE sql STABLE;
 
CREATE OR REPLACE FUNCTION set_local_variable( IN p_variable TEXT, IN p_value TEXT )
RETURNS VOID
AS $$
DECLARE
  v_data record;
BEGIN
  SELECT pid, backend_start INTO v_data FROM pg_stat_get_activity(pg_backend_pid());
  WITH upsert AS (
    UPDATE sys_variables
    SET value = p_value
    WHERE variable = p_variable
      AND backend_pid IS NOT NULL
      AND backend_pid = v_data.pid
      AND backend_start = v_data.backend_start
    RETURNING variable
  )
  INSERT INTO sys_variables ( variable, value, backend_pid, backend_start )
  SELECT p_variable, p_value, v_data.pid, v_data.backend_start
  WHERE NOT EXISTS (
    SELECT * FROM upsert
  );
END;
$$
LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION set_global_variable( IN p_variable TEXT, IN p_value TEXT )
RETURNS VOID
AS $$
WITH upsert AS (
  UPDATE sys_variables
  SET value = p_value
  WHERE variable = p_variable
    AND backend_pid IS NULL
  RETURNING variable
)
INSERT INTO sys_variables ( variable, value, backend_pid, backend_start )
SELECT p_variable, p_value, NULL, NULL
WHERE NOT EXISTS (
  SELECT * FROM upsert
);
$$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION set_variable( IN p_variable TEXT, IN p_value TEXT, IN p_global BOOLEAN DEFAULT TRUE )
RETURNS VOID
AS $$
SELECT CASE WHEN p_global THEN set_global_variable( p_variable, p_value )
            ELSE               set_local_variable( p_variable, p_value )
       END;
$$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION unset_local_variable( IN p_variable TEXT )
RETURNS VOID
AS $$
DECLARE
  v_data record;
BEGIN
  SELECT pid, backend_start INTO v_data FROM pg_stat_get_activity(pg_backend_pid());
  DELETE
  FROM sys_variables
  WHERE variable = p_variable
    AND backend_pid IS NOT NULL
    AND backend_pid = v_data.pid
    AND backend_start = v_data.backend_start;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION unset_global_variable( IN p_variable TEXT )
RETURNS VOID
AS $$
DELETE
FROM sys_variables
WHERE variable = p_variable;
$$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION unset_variable( IN p_variable TEXT, IN p_global BOOLEAN DEFAULT TRUE )
RETURNS VOID
AS $$
SELECT CASE WHEN p_global THEN unset_global_variable( p_variable )
            ELSE               unset_local_variable( p_variable )
       END;
$$
LANGUAGE sql;
 
CREATE OR REPLACE FUNCTION sys_variables_cleanup( )
RETURNS VOID
AS $$
DELETE
FROM sys_variables
WHERE backend_pid IS NOT NULL
  AND (backend_pid, backend_start) NOT in (
    SELECT a.pid, a.backend_start
    FROM pg_stat_activity AS a
  );
$$
LANGUAGE sql;
