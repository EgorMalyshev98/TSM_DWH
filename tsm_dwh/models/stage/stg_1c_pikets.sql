
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
	hdiff_dv_msat_fact_pikets,
	ключстроки,
	пикет_с,
	пикет_по,
	смещение_с,
	смещение_по,
	объем_работы,
	вид_пикета_value,
	вид_пикета_codе,
	вид_пикета_name,
	тип_пикета_value,
	тип_пикета_codе,
	тип_пикета_name,
	группа_пикетов_spider_value,
	группа_пикетов_spider_codе,
	группа_пикетов_spider_name
FROM (
    SELECT
        recsource,
        loadts,
        'default',
        digest('default' || '|' || LOWER(TRIM(COALESCE(КлючСвязи::varchar, '-1')::varchar)), 'sha1') as hk_dv_hub_fact_work,
	LOWER(TRIM(COALESCE(КлючСвязи::varchar, '-1')::varchar)),
	digest(TRIM(COALESCE(КлючСтроки::varchar, 'N\A')) || '|' || TRIM(COALESCE(ПикетС::varchar, 'N\A')) || '|' || TRIM(COALESCE(ПикетПо::varchar, 'N\A')) || '|' || TRIM(COALESCE(СмещениеС::varchar, 'N\A')) || '|' || TRIM(COALESCE(СмещениеПо::varchar, 'N\A')) || '|' || TRIM(COALESCE(Объем::varchar, 'N\A')) || '|' || TRIM(COALESCE(ВидПикета_value::varchar, 'N\A')) || '|' || TRIM(COALESCE(ВидПикета_codе::varchar, 'N\A')) || '|' || TRIM(COALESCE(ВидПикета_name::varchar, 'N\A')) || '|' || TRIM(COALESCE(ТипПикета_value::varchar, 'N\A')) || '|' || TRIM(COALESCE(ТипПикета_codе::varchar, 'N\A')) || '|' || TRIM(COALESCE(ТипПикета_name::varchar, 'N\A')) || '|' || TRIM(COALESCE(ГруппаПикетовSpider_value::varchar, 'N\A')) || '|' || TRIM(COALESCE(ГруппаПикетовSpider_codе::varchar, 'N\A')) || '|' || TRIM(COALESCE(ГруппаПикетовSpider_name::varchar, 'N\A')), 'sha1') as hdiff_dv_msat_fact_pikets,
	КлючСтроки::uuid,
	ПикетС::numeric,
	ПикетПо::numeric,
	СмещениеС::numeric,
	СмещениеПо::numeric,
	Объем::numeric,
	ВидПикета_value::uuid,
	ВидПикета_codе::varchar,
	ВидПикета_name::varchar,
	ТипПикета_value::uuid,
	ТипПикета_codе::varchar,
	ТипПикета_name::varchar,
	ГруппаПикетовSpider_value::uuid,
	ГруппаПикетовSpider_codе::varchar,
	ГруппаПикетовSpider_name::varchar
    FROM 
        {{ source('public', 'src_pikets') }}
) AS tmp (recsource, loadts, hkcode, hk_dv_hub_fact_work,
	bk_работа_uuid,
	hdiff_dv_msat_fact_pikets,
	ключстроки,
	пикет_с,
	пикет_по,
	смещение_с,
	смещение_по,
	объем_работы,
	вид_пикета_value,
	вид_пикета_codе,
	вид_пикета_name,
	тип_пикета_value,
	тип_пикета_codе,
	тип_пикета_name,
	группа_пикетов_spider_value,
	группа_пикетов_spider_codе,
	группа_пикетов_spider_name)
