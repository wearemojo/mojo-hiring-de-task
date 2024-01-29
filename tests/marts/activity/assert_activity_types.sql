select *
from {{ ref('course_activity') }}
where type not in (
	'all_progress_reset',
	'course_completed',
	'course_started'
)
