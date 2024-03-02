TRUNCATE TABLE public.employees;
TRUNCATE TABLE public.timesheets;

-- COPY employees
-- FROM '/docker-entrypoint-initdb.d/employees.csv'
-- DELIMITER ','
-- CSV HEADER;

-- CREATE TABLE IF NOT EXISTS public.timesheets (
-- 	timesheet_id VARCHAR PRIMARY KEY,
-- 	employee_id VARCHAR NOT NULL,
-- 	date DATE NOT NULL,
-- 	checkin DATE,
-- 	checkout DATE
-- );


CREATE TABLE IF NOT EXISTS employees (
	employee_id VARCHAR PRIMARY KEY,
	branch_id VARCHAR NOT NULL,
	salary INTEGER NOT NULL,
	join_date DATE NOT NULL,
	resign_date DATE
);

