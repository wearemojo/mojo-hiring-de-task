select
	_id as id,
	_fivetran_synced as _synced_at,
	created_at,
	previous_id,
	user_id,
	type,
	params,
from {{ source('mojo_s_learntracking', 'user_activity') }}
