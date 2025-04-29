
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
	hk_dv_lnk_fact_journal_fact_work,
	bk_жуфвр_uuid,
	hk_dv_hub_fact_journal,
	hdiff_dv_sat_fact_work,
	идентификатор,
	объем_работы,
	примечание,
	кв,
	пометка_удаления,
	тип_spider,
	структура_работ_value,
	структура_работ_codе,
	структура_работ_name,
	видработ_value,
	видработ_codе,
	видработ_name
FROM (
    SELECT
        recsource,
        loadts,
        'default',
        digest('default' || '|' || LOWER(TRIM(COALESCE("КлючСвязи"::varchar, '-1')::varchar)), 'sha1') as hk_dv_hub_fact_work,
	LOWER(TRIM(COALESCE("КлючСвязи"::varchar, '-1')::varchar)),
	digest('default' || '|' || LOWER(TRIM(COALESCE("value_Ссылка"::varchar, '-1')::varchar)) || '|' || LOWER(TRIM(COALESCE("КлючСвязи"::varchar, '-1')::varchar)), 'sha1') as hk_dv_lnk_fact_journal_fact_work,
	LOWER(TRIM(COALESCE("value_Ссылка"::varchar, '-1')::varchar)),
	digest('default' || '|' || LOWER(TRIM(COALESCE("value_Ссылка"::varchar, '-1')::varchar)), 'sha1') as hk_dv_hub_fact_journal,
	digest(TRIM(COALESCE("СтруктураРаботИдентификатор"::varchar, 'N\A')) || '|' || TRIM(COALESCE("ОбъемРаботы"::varchar, 'N\A')) || '|' || TRIM(COALESCE("Примечание"::varchar, 'N\A')) || '|' || TRIM(COALESCE("СтруктураРаботНомерПоКонтрактнойВедомости"::varchar, 'N\A')) || '|' || TRIM(COALESCE("СтруктураРаботПометкаУдаления"::varchar, 'N\A')) || '|' || TRIM(COALESCE("ВидРаботУровеньОперации"::varchar, 'N\A')) || '|' || TRIM(COALESCE("СтруктураРабот_value"::varchar, 'N\A')) || '|' || TRIM(COALESCE("СтруктураРабот_codе"::varchar, 'N\A')) || '|' || TRIM(COALESCE("СтруктураРабот_name"::varchar, 'N\A')) || '|' || TRIM(COALESCE("ВидРабот_value"::varchar, 'N\A')) || '|' || TRIM(COALESCE("ВидРабот_codе"::varchar, 'N\A')) || '|' || TRIM(COALESCE("ВидРабот_name"::varchar, 'N\A')), 'sha1') as hdiff_dv_sat_fact_work,
	"СтруктураРаботИдентификатор"::varchar,
	"ОбъемРаботы"::numeric,
	"Примечание"::varchar,
	"СтруктураРаботНомерПоКонтрактнойВедомости"::varchar,
	"СтруктураРаботПометкаУдаления"::bool,
	"ВидРаботУровеньОперации"::varchar,
	"СтруктураРабот_value"::uuid,
	"СтруктураРабот_codе"::varchar,
	"СтруктураРабот_name"::varchar,
	"ВидРабот_value"::uuid,
	"ВидРабот_codе"::varchar,
	"ВидРабот_name"::varchar
    FROM 
        {{ source('public', 'src_works') }}
) AS tmp (recsource, loadts, hkcode, hk_dv_hub_fact_work,
	bk_работа_uuid,
	hk_dv_lnk_fact_journal_fact_work,
	bk_жуфвр_uuid,
	hk_dv_hub_fact_journal,
	hdiff_dv_sat_fact_work,
	идентификатор,
	объем_работы,
	примечание,
	кв,
	пометка_удаления,
	тип_spider,
	структура_работ_value,
	структура_работ_codе,
	структура_работ_name,
	видработ_value,
	видработ_codе,
	видработ_name)
