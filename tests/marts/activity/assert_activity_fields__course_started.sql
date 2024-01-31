select *,
from {{ ref('course_activity') }}
where
	type in ('course_started')
	and json_type(params.course_key) != 'string'
