CREATE TABLE IF NOT EXISTS employees (
  	id SERIAL PRIMARY KEY,
	employee_id VARCHAR,
	branch_id VARCHAR NOT NULL,
	salary INTEGER NOT NULL,
	join_date DATE NOT NULL,
	resign_date DATE
);

COPY employees (employee_id, branch_id, salary, join_date, resign_date)
FROM '/docker-entrypoint-initdb.d/src/employees.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE IF NOT EXISTS timesheets (
	timesheet_id VARCHAR PRIMARY KEY,
	employee_id VARCHAR NOT NULL,
	date DATE NOT NULL,
	checkin TIME,
	checkout TIME
);

COPY timesheets (timesheet_id, employee_id, date, checkin, checkout)
FROM '/docker-entrypoint-initdb.d/src/timesheets.csv'
DELIMITER ','
CSV HEADER;