TRUNCATE TABLE salary_per_hour;

select 
	* ,
	case
		when checkout > checkin then checkout - checkin
		else TO_TIMESTAMP(CONCAT(date(date + interval '1 day'),' ',checkout), 'YYYY-MM-DD HH24:MI:SS') - 
			TO_TIMESTAMP(CONCAT(date,' ',checkin), 'YYYY-MM-DD HH24:MI:SS')
	end time_delta
from timesheets t
where
	checkin is not null
	and checkout is not null
order by 6;