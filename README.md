# docker-csv-to-db

## result
![ss result](https://github.com/firdausraginda/docker-csv-to-db/blob/main/ss_result.png)

## how to run

### docker command 
* use docker to ease on setup the DB & pre-loaded tables (`employees` & `timesheets`).
* run this command to initialize postgres DB with given config in `.env` and create table `employees` & `timesheets`:
```
docker-compose --env-file .env up -d
```

### DB config
* kindly find the DB confing in `.env` file, to connect in your local.

### python transform script
* script description
  * path: `transform-scripts/salary_effectivity_append.py`
  * purpose: python script to extract, transform, & load using **incremental mode** 
* execute script:
  1. create virtual environment, and install all library in `requirements.txt`
  2. execute command: `python3 transform-scripts/salary_effectivity_append.py`
* script flow
  1. create dataframe from CSV files
  2. manipulate to expected result
  3. store result to final dataframe 
  4. slice dataframe per chunk
  5. append each sliced dataframe to DB
* note
  * this script utilize python [**polars**](https://docs.pola.rs/) library to manipulate data from CSV in form of dataframe, and write it to DB. Compare to pandas, [polars is more faster](https://medium.com/cuenex/pandas-2-0-vs-polars-the-ultimate-battle-a378eb75d6d1).

### SQL transform script
* script description
  * path: `transform-scripts/salary_effectivity_truncate.sql`
  * purpose: extract data from `employees` and `timesheets` table, and load to new table with **full-snapshot mode**
* execute script
  * execute this SQL query on PGAdmin or dbeaver or any other DB tools
* script flow
  1. create destination table if not exists
  2. truncate destination table
  3. manipulate to expected result
  4. insert data to destination table

### destination table
* both SQL & python script will load to new table `salary_effectivity` with columns `year`, `month`, `branch_id`, & `salary_per_hour`.

## handle invalid data and edge cases

### duplicate employee
* found duplicate employee_id in `employees` with `employe_id = 218078`, all the columns have same value except for salary.
* we couldn't tell which salary is valid since no flagging determined such criteria. I assume new salary data should overwrite the old one, instead of create new row.
* hence, in pre-loaded table `employees`, I set new column `id` as the primary key instead of using `employe_id`.
* this duplicate employee exists in many timesheets, so instead of exclude it, I use the average salary to get the approximate number that perhaps close to the correct one.

### duplicate timesheets
* found duplicate timesheets with exact same value for all columns except the `timesheet_id`. 
* duplicate timesheets:
```
select employee_id, date, checkin, checkout, count(*)
from timesheets t 
group by 1,2,3,4
having count(*) > 1
```
* I assume this data should only appear in 1 row, since there's no way 1 employee can have 2 checkin & checkout with exact same value in the same day.
* I handle it with get the unique value per employee_id, date, checkin, & checkout.

### found NULL `checkin` or `checkout` in timesheets data
* I assume those NULL values caused by the employee forget to tap in / tap out.
* So I handle it with exclude those values from the process, since we need to have both `checkin` & `checkout` to be able calculate the working hour.

### found `checkout` < `checkin` in timesheets data
* I thought this happen because the `checkout` is on the next day. But it doesn't make sense since on the next day that employee has `checkin` with value less than `checkout`.
```
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
```
* So I assume system misplaced the `checkin` & `checkout`, therefore I switch them up.

### compare result between SQL script & python script
* I dump each result to different tables `salary_effectivity_sql` & `salary_effectivity_py`.
* then do this query to check number of different salary_per_hour per year, month, & branch_id:
```
with
	pool_union as (
	select year, month, branch_id
	from salary_effectivity_sql
	union
	select year, month, branch_id
	from salary_effectivity_py
	)

select count(*)
from pool_union a
left join salary_effectivity_sql b on
	a.year = b.year
	and a.month = b.month
	and a.branch_id = b.branch_id
left join salary_effectivity_py c on
	a.year = c.year
	and a.month = c.month
	and a.branch_id = c.branch_id
where
	b.salary_per_hour <> c.salary_per_hour 
```
* it shows 0 occurrences, meaning no discrepancy found