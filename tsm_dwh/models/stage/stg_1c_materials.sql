SELECT
    recsource,
    loadts,
    hkcode,
    hk_dv_hub_fact_work,
	bk_работа_uuid,
	hdiff_dv_msat_fact_materials,
	номер_строки,
	примечание,
	объем_материала,
	ресурс_value,
	ресурс_codе,
	ресурс_name
FROM (
    SELECT
        recsource,
        loadts,
        'default',
        digest('default' || '|' || LOWER(TRIM(COALESCE(КлючСвязи::varchar, '-1')::varchar)), 'sha1') as hk_dv_hub_fact_work,
	LOWER(TRIM(COALESCE(КлючСвязи::varchar, '-1')::varchar)),
	digest(TRIM(COALESCE(НомерСтроки::varchar, 'N\A')) || '|' || TRIM(COALESCE(Примечание::varchar, 'N\A')) || '|' || TRIM(COALESCE(ОбъемМатериала::varchar, 'N\A')) || '|' || TRIM(COALESCE(Ресурс_value::varchar, 'N\A')) || '|' || TRIM(COALESCE(Ресурс_codе::varchar, 'N\A')) || '|' || TRIM(COALESCE(Ресурс_name::varchar, 'N\A')), 'sha1') as hdiff_dv_msat_fact_materials,
	НомерСтроки::int,
	Примечание::varchar,
	ОбъемМатериала::numeric,
	Ресурс_value::uuid,
	Ресурс_codе::varchar,
	Ресурс_name::varchar
    FROM 
        src_materials
) AS tmp (recsource, loadts, hkcode, hk_dv_hub_fact_work,
	bk_работа_uuid,
	hdiff_dv_msat_fact_materials,
	номер_строки,
	примечание,
	объем_материала,
	ресурс_value,
	ресурс_codе,
	ресурс_name)
