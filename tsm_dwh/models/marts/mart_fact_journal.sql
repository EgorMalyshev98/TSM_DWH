{{
    config(
        materialized='incremental',
        unique_key=['hk_dv_hub_fact_journal'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['mart'],
        indexes=[
            {
                'columns': ['hk_dv_hub_fact_journal'],
                'unique': true
            },
            {
                'columns': ['дата']
            },
            {
                'columns': ['_dbt_loaded_at']
            }
        ]
    )
}}


WITH

{% if is_incremental() %}
changed_journals AS (
    SELECT hk_dv_hub_fact_journal
    FROM {{ ref('dv_sat_fact_journal') }}
    WHERE loadts > (SELECT COALESCE(MAX(_dbt_loaded_at), '1970-01-01') FROM {{ this }})
),
{% endif %}

active_journal AS (
	SELECT
        t.hk_dv_hub_fact_journal,
        t.версия_жуфвр_name,
        t.дата,
        t.территория_name,
        t.территория_value,
        t.подразделение_name,
        t.смена_name,
        t.год,
		t.направление_деятельности_name,

        last_name || ' ' || first_name || '. ' || second_name || '.' as ответственный,

        r.name AS объект,

        t._dbt_loaded_at
	FROM
		(SELECT
			encode(hk_dv_hub_fact_journal, 'hex') as hk_dv_hub_fact_journal,

			версия_жуфвр_name,
			дата,
			территория_name,
			TRIM(территория_value::varchar) as территория_value,
			подразделение_name,
			смена_name,
			ответственный_name,
			направление_деятельности_name,
			проведен,
			пометка_удаления,

			extract(year from дата) as год,

			MAX(loadts) OVER(PARTITION BY hk_dv_hub_fact_journal) AS _dbt_loaded_at,
			row_number() OVER(PARTITION BY hk_dv_hub_fact_journal ORDER BY loadts DESC) rn,

            (string_to_array(ответственный_name, ' '))[1] AS last_name,
            substring((string_to_array(ответственный_name, ' '))[2], 1, 1) AS first_name,
            substring((string_to_array(ответственный_name, ' '))[3], 1, 1) AS second_name

		FROM {{ ref('dv_sat_fact_journal') }} j
		WHERE проведен is TRUE AND пометка_удаления is FALSE
		{% if is_incremental() %}
		  AND hk_dv_hub_fact_journal IN (SELECT hk_dv_hub_fact_journal FROM changed_journals)
		{% endif %}
		) t
    JOIN {{ source('public', 'ref_objects') }} r ON r.name_1c = t.направление_деятельности_name
	WHERE rn = 1
    and r.is_active is true
    )


SELECT * FROM active_journal
