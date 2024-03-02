CREATE TABLE IF NOT EXISTS employees (
	employee_id VARCHAR PRIMARY KEY,
	branch_id VARCHAR NOT NULL,
	salary INTEGER NOT NULL,
	join_date DATE NOT NULL,
	resign_date DATE
);

