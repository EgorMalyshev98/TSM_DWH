
{{
    config(
        tags=['stage', 'жуфвр_1с']
    )
}}

SELECT
    recsource,
    loadts,
    hkcode,
    hk_dv_hub_fact_work,
	bk_работа_uuid,
	hk_dv_hub_tech,
	bk_ресурс_uuid,
	hk_dv_lnk_fact_work_tech,
	hdiff_dv_sat_fact_tech,
	примечание,
	госномер_техника_не_найдена,
	количество,
	часы,
	ресурс_value,
	ресурс_codе,
	ресурс_name,
	аналитика_codе,
	аналитика_name,
	контрагент_value,
	контрагент_codе,
	контрагент_name
FROM (
    SELECT
        recsource,
        loadts,
        'default',
        digest('default' || '|' || LOWER(TRIM(COALESCE(КлючСвязи::varchar, '-1')::varchar)), 'sha1') as hk_dv_hub_fact_work,
	LOWER(TRIM(COALESCE(КлючСвязи::varchar, '-1')::varchar)),
	digest('default' || '|' || LOWER(TRIM(COALESCE(Аналитика_value::varchar, '-1')::varchar)), 'sha1') as hk_dv_hub_tech,
	LOWER(TRIM(COALESCE(Аналитика_value::varchar, '-1')::varchar)),
	digest('default' || '|' || LOWER(TRIM(COALESCE(КлючСвязи::varchar, '-1')::varchar)) || '|' || LOWER(TRIM(COALESCE(Аналитика_value::varchar, '-1')::varchar)), 'sha1') as hk_dv_lnk_fact_work_tech,
	digest(TRIM(COALESCE(Примечание::varchar, 'N\A')) || '|' || TRIM(COALESCE(ГосНомерТехникаНеНайдена::varchar, 'N\A')) || '|' || TRIM(COALESCE(Количество::varchar, 'N\A')) || '|' || TRIM(COALESCE(Часы::varchar, 'N\A')) || '|' || TRIM(COALESCE(Ресурс_value::varchar, 'N\A')) || '|' || TRIM(COALESCE(Ресурс_codе::varchar, 'N\A')) || '|' || TRIM(COALESCE(Ресурс_name::varchar, 'N\A')) || '|' || TRIM(COALESCE(Аналитика_codе::varchar, 'N\A')) || '|' || TRIM(COALESCE(Аналитика_name::varchar, 'N\A')) || '|' || TRIM(COALESCE(Контрагент_value::varchar, 'N\A')) || '|' || TRIM(COALESCE(Контрагент_codе::varchar, 'N\A')) || '|' || TRIM(COALESCE(Контрагент_name::varchar, 'N\A')), 'sha1') as hdiff_dv_sat_fact_tech,
	Примечание::varchar,
	ГосНомерТехникаНеНайдена::varchar,
	Количество::int,
	Часы::numeric,
	Ресурс_value::uuid,
	Ресурс_codе::varchar,
	Ресурс_name::varchar,
	Аналитика_codе::varchar,
	Аналитика_name::varchar,
	Контрагент_value::varchar,
	Контрагент_codе::varchar,
	Контрагент_name::varchar
    FROM 
        src_tech
) AS tmp (recsource, loadts, hkcode, hk_dv_hub_fact_work,
	bk_работа_uuid,
	hk_dv_hub_tech,
	bk_ресурс_uuid,
	hk_dv_lnk_fact_work_tech,
	hdiff_dv_sat_fact_tech,
	примечание,
	госномер_техника_не_найдена,
	количество,
	часы,
	ресурс_value,
	ресурс_codе,
	ресурс_name,
	аналитика_codе,
	аналитика_name,
	контрагент_value,
	контрагент_codе,
	контрагент_name)
