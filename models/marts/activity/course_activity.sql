select
	id,
	_synced_at,
	created_at,
	previous_id,
	user_id,
	params,
from {{ ref('stg_s_learntracking__user_activity') }}
