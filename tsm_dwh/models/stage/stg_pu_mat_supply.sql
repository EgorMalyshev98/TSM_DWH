
{{
    config(
        tags=['stage', 'ПУ_xl_поставки_материалов']
    )
}}

SELECT
    recsource,
    loadts,
    hkcode,
    hk_dv_hub_date,
	bk_факт_дата,
	hk_dv_hub_material,
	bk_материал,
	hk_dv_hub_object,
	bk_объект,
	hk_dv_lnk_object_material_date,
	hdiff_dv_sat_pu_fact_supply,
	ед_измерения,
	факт_объем,
	план_объем_суточный
FROM (
    SELECT
        recsource,
        loadts,
        'default',
        digest('default' || '|' || LOWER(TRIM(COALESCE("date"::varchar, '-1')::varchar)), 'sha1') as hk_dv_hub_date,
	LOWER(TRIM(COALESCE("date"::varchar, '-1')::varchar)),
	digest('default' || '|' || LOWER(TRIM(COALESCE("Наименование материала"::varchar, '-1')::varchar)), 'sha1') as hk_dv_hub_material,
	LOWER(TRIM(COALESCE("Наименование материала"::varchar, '-1')::varchar)),
	digest('default' || '|' || LOWER(TRIM(COALESCE("Наименование объекта"::varchar, '-1')::varchar)), 'sha1') as hk_dv_hub_object,
	LOWER(TRIM(COALESCE("Наименование объекта"::varchar, '-1')::varchar)),
	digest('default' || '|' || LOWER(TRIM(COALESCE("Наименование объекта"::varchar, '-1')::varchar)) || '|' || LOWER(TRIM(COALESCE("Наименование материала"::varchar, '-1')::varchar)) || '|' || LOWER(TRIM(COALESCE("date"::varchar, '-1')::varchar)), 'sha1') as hk_dv_lnk_object_material_date,
	digest(TRIM(COALESCE("Единица измерения"::varchar, 'N\A')) || '|' || TRIM(COALESCE("volume"::varchar, 'N\A')) || '|' || TRIM(COALESCE("План суточный"::varchar, 'N\A')), 'sha1') as hdiff_dv_sat_pu_fact_supply,
	"Единица измерения"::varchar,
	"volume"::numeric,
	"План суточный"::varchar
    FROM 
        {{ source('public', 'src_pu_mat_supply') }}
) AS tmp (recsource, loadts, hkcode, hk_dv_hub_date,
	bk_факт_дата,
	hk_dv_hub_material,
	bk_материал,
	hk_dv_hub_object,
	bk_объект,
	hk_dv_lnk_object_material_date,
	hdiff_dv_sat_pu_fact_supply,
	ед_измерения,
	факт_объем,
	план_объем_суточный)
