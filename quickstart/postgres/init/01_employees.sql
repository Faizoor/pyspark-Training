-- 01_employees.sql
-- Runs automatically when the postgres container starts for the first time.
-- Seeds the public.employees table with the same five employees used in the
-- Spark SQL demo JSON dataset so Section 14 (JDBC read) returns meaningful data.

CREATE TABLE IF NOT EXISTS public.employees (
    id         SERIAL PRIMARY KEY,
    name       VARCHAR(100)   NOT NULL,
    dept_name  VARCHAR(100),
    dept_floor INT,
    salary     NUMERIC(10, 2)
);

INSERT INTO public.employees (id, name, dept_name, dept_floor, salary) VALUES
  (1, 'Alice', 'Engineering', 3, 95000.00),
  (2, 'Bob',   'Marketing',   2, 72000.00),
  (3, 'Carol', 'Engineering', 3, 105000.00),
  (4, 'Dave',  'HR',          1, 65000.00),
  (5, 'Eve',   'Marketing',   2, 78000.00)
ON CONFLICT (id) DO NOTHING;
