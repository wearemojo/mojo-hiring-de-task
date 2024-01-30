{{
	config(
		materialized='incremental',
		unique_key='_id'
	)
}}
select
	_id as id,
	_fivetran_synced as _synced_at,
	created_at,
	previous_id,
	user_id,
	type,
	params,
from {{ source('mojo_s_learntracking', 'user_activity') }}

{% if is_incremental() %}

where _synced_at > coalesce(max(_synced_at), timestamp('1970-01-01 00:00:00'))

{% endif %}
