with qualified_courses as (
select
	id,
	_synced_at,
	created_at,
	type,
	previous_id,
	user_id,
	params,
	lag(type) over(partition by user_id, json_extract_scalar(params, '$.course_key') order by synced_at) as previous_type
from {{ ref('stg_s_learntracking__user_activity') }}
),
valid_course_events as (
	select 
		id,
		_synced_at,
		created_at,
		type,
		previous_id,
		user_id,
		params,
		case when qualified_courses.Type = 'course_started' and previous_type is null then 1 
		when qualified_courses.Type = 'all_progress_reset' then 1
		when qualified_courses.Type = 'course_completed' and previous_type in ('course_started', 'all_progress_reset') then 1
		else 0 end as validity
	from qualified_courses
)
select
	id,
	_synced_at,
	created_at,
	type,
	previous_id,
	user_id,
	params
from valid_course_events 
where validity = 1
