CREATE TABLE IF NOT EXISTS salary_per_hour (
	year INTEGER NOT NULL,
	month VARCHAR NOT NULL,
	branch_id VARCHAR NOT NULL,
	salary_per_hour FLOAT NOT NULL,
	PRIMARY KEY(year, month, branch_id)
);

TRUNCATE TABLE salary_per_hour;

INSERT INTO salary_per_hour (year, month, branch_id, salary_per_hour)
with
	eligible_timesheets as (
	/*
		- exclude missing checkin or checkout
		- remove duplication per employee_id, date, checkin, checkout
	*/
	select employee_id, date, checkin, checkout
	from timesheets
	where
		checkin is not null
		AND checkout is not null
	group by 1,2,3,4
	),
	eligible_employees as (
	/*
		- found duplicate employee data with different salary, e.g. employee_id='218078'
		- handle duplicate employee by calculating the avg salary
	*/
	with
		duplicate_employees as (
		select employee_id, count(*) count_duplication
		from employees
		group by 1
		having count(*) > 1
		),
		avg_salary_duplicated_employees as (
		select all_emp.employee_id, 
			sum(all_emp.salary)/count(dup_emp.employee_id) salary
		from employees all_emp
		inner join duplicate_employees dup_emp on all_emp.employee_id = dup_emp.employee_id
		group by 1
		)

	select emp.employee_id, emp.branch_id, emp.salary, emp.join_date, emp.resign_date 
	from employees emp
	left join duplicate_employees dup_emp on emp.employee_id = dup_emp.employee_id
	where
		dup_emp.employee_id is null
	union
	select emp.employee_id, emp.branch_id, avg_dup_emp.salary, emp.join_date, emp.resign_date
	from employees emp
	inner join avg_salary_duplicated_employees avg_dup_emp on emp.employee_id = avg_dup_emp.employee_id
	),
	calculate_time_delta AS (
	/*
		- found checkout time < checkin time. handle by use checkin as checkout, and vice versa
		- calculate `checkout - checkin`, then convert it in hour level
	*/
	select
		employee_id, date, checkin, checkout, time_delta,
		extract('hour' from time_delta) + 
			extract('minutes' from time_delta)/60 + 
			extract('second' from time_delta)/3600 
			as working_hour
	from (
		SELECT 
			* ,
			CASE
				WHEN checkout < checkin THEN checkin - checkout
				ELSE checkout - checkin
			END AS time_delta
		FROM eligible_timesheets
	)pool
	),
	total_hour_per_monthyear_branch_employee as (
	/*
		- sum working_hour per year, month, branch_id, & employee_id
	*/
	select 
		extract('year' from cal_time_delta.date)::text as year,
		extract('month' from cal_time_delta.date)::text as month,
		eli_emp.branch_id,
		cal_time_delta.employee_id,
		eli_emp.salary,
		SUM(cal_time_delta.working_hour) total_working_hour
	from calculate_time_delta cal_time_delta
	left join eligible_employees eli_emp on cal_time_delta.employee_id = eli_emp.employee_id
	group by 1,2,3,4,5
	),
	divide_total_summary_with_working_hour as (
	/*
		- convert month num to month name
		- sum salary per year, month, & branch_id
		- sum total workig hour per year, month, & branch_id
		- divide those 2 numbers and round it
	*/
	select year, initcap(trim(to_char(to_date(month, 'MM'),'month'))) as month, branch_id, 
		round(cast(sum(salary)/sum(total_working_hour) as numeric),2) salary_per_hour
	from total_hour_per_monthyear_branch_employee
	group by 1,2,3
	)

select year, month, branch_id, salary_per_hour
from divide_total_summary_with_working_hour;


---------------------------------------------------------------------------------------------------------

/*

-- bukti kalau checkout < checkin itu kebalik, bukan hari setelahnya
WITH
	eligible_timesheets AS (
		SELECT employee_id, date, checkin, checkout
		FROM timesheets
		WHERE
			checkin IS NOT NULL
			AND checkout IS NOT null
		GROUP BY 1,2,3,4
	)

select *,
	case when checkout < checkin then true
	else false
	end as is_over_day
from eligible_timesheets
where
	employee_id = '101'
	and date >= '2020-01-07' and date <= '2020-01-12'
order by 1,2;

-- employee_id ada yg duplicate
select employee_id, branch_id, count(*)
from employees
group by 1,2
having count(*) > 1;

-- timesheets is unique per employee_id & date
select employee_id, date, count(*)
from (
	select distinct employee_id, date, checkin, checkout
	from timesheets
	) sub
where
	checkin is not null
	and checkout is not null
group by 1,2
having count(*) > 1;

*/