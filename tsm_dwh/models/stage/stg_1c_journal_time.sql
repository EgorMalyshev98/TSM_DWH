
{{
    config(
        tags=['stage', 'жуфвр_1с']
    )
}}

SELECT
    recsource,
    loadts,
    hkcode,
    hk_dv_hub_fact_journal,
	bk_жуфвр_uuid,
	hdiff_dv_msat_fact_journal_time,
	период,
	id_записи,
	пользователь_value,
	пользователь_name,
	время_сек
FROM (
    SELECT
        recsource,
        loadts,
        'default',
        digest('default' || '|' || LOWER(TRIM(COALESCE("ЖУФВР_value"::varchar, '-1')::varchar)), 'sha1') as hk_dv_hub_fact_journal,
	LOWER(TRIM(COALESCE("ЖУФВР_value"::varchar, '-1')::varchar)),
	digest(TRIM(COALESCE("Период"::varchar, 'N\A')) || '|' || TRIM(COALESCE("UIDЗаписи"::varchar, 'N\A')) || '|' || TRIM(COALESCE("Пользователь_value"::varchar, 'N\A')) || '|' || TRIM(COALESCE("Пользователь_name"::varchar, 'N\A')) || '|' || TRIM(COALESCE("ДельтаВремениСек"::varchar, 'N\A')), 'sha1') as hdiff_dv_msat_fact_journal_time,
	"Период"::timestamptz,
	"UIDЗаписи"::uuid,
	"Пользователь_value"::uuid,
	"Пользователь_name"::varchar,
	"ДельтаВремениСек"::numeric
    FROM
        {{ source('public', 'src_journal_time') }}
) AS tmp (recsource, loadts, hkcode, hk_dv_hub_fact_journal,
	bk_жуфвр_uuid,
	hdiff_dv_msat_fact_journal_time,
	период,
	id_записи,
	пользователь_value,
	пользователь_name,
	время_сек)
