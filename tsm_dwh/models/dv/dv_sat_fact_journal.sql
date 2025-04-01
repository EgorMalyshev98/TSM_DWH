{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['sat', 'жуфвр_1с']
    )
}}


{% if is_incremental() %}
    WITH active_sat AS (
    SELECT DISTINCT ON (hk_dv_hub_fact_journal) hk_dv_hub_fact_journal, hdiff_dv_sat_fact_journal, loadts
    FROM dv_sat_fact_journal
    ORDER BY hk_dv_hub_fact_journal, loadts DESC)

    , max_load AS (
    SELECT max(loadts) AS max_loadts
    FROM dv_sat_fact_journal
    )
    , stage_cte AS (

        SELECT
        s.hk_dv_hub_fact_journal,
        s.hdiff_dv_sat_fact_journal,
        s.loadts,
        s.recsource,
        lag(s.hdiff_dv_sat_fact_journal) over(PARTITION BY s.hk_dv_hub_fact_journal ORDER BY s.loadts) AS lag_hdiff,
        a.loadts AS sat_loadts,
        a.hdiff_dv_sat_fact_journal AS sat_hdiff,
        rabbit_uuid,
	rabbit_дата,
	проведен,
	uuid_жуфвр,
	пометка_удаления,
	дата,
	номер,
	территория_value,
	территория_codе,
	территория_name,
	подразделение_value,
	подразделение_codе,
	подразделение_name,
	смена_value,
	смена_codе,
	смена_name,
	прораб_value,
	прораб_codе,
	прораб_name,
	комментарий,
	ответственный_value,
	ответственный_codе,
	ответственный_name,
	версия_жуфвр_value,
	версия_жуфвр_codе,
	версия_жуфвр_name,
	направление_деятельности_value,
	направление_деятельности_codе,
	направление_деятельности_name

    FROM
        (SELECT
            s.hk_dv_hub_fact_journal,
            s.hdiff_dv_sat_fact_journal,
            s.loadts,
            s.recsource,
            rabbit_uuid,
	rabbit_дата,
	проведен,
	uuid_жуфвр,
	пометка_удаления,
	дата,
	номер,
	территория_value,
	территория_codе,
	территория_name,
	подразделение_value,
	подразделение_codе,
	подразделение_name,
	смена_value,
	смена_codе,
	смена_name,
	прораб_value,
	прораб_codе,
	прораб_name,
	комментарий,
	ответственный_value,
	ответственный_codе,
	ответственный_name,
	версия_жуфвр_value,
	версия_жуфвр_codе,
	версия_жуфвр_name,
	направление_деятельности_value,
	направление_деятельности_codе,
	направление_деятельности_name
        FROM stg_1c_journal s
        CROSS JOIN max_load ml
        WHERE s.loadts > ml.max_loadts
        ) s
    LEFT JOIN active_sat a ON a.hk_dv_hub_fact_journal = s.hk_dv_hub_fact_journal)

    SELECT DISTINCT
        hk_dv_hub_fact_journal,
        recsource,
        loadts,
        hdiff_dv_sat_fact_journal,
        rabbit_uuid,
	rabbit_дата,
	проведен,
	uuid_жуфвр,
	пометка_удаления,
	дата,
	номер,
	территория_value,
	территория_codе,
	территория_name,
	подразделение_value,
	подразделение_codе,
	подразделение_name,
	смена_value,
	смена_codе,
	смена_name,
	прораб_value,
	прораб_codе,
	прораб_name,
	комментарий,
	ответственный_value,
	ответственный_codе,
	ответственный_name,
	версия_жуфвр_value,
	версия_жуфвр_codе,
	версия_жуфвр_name,
	направление_деятельности_value,
	направление_деятельности_codе,
	направление_деятельности_name
    FROM stage_cte
    WHERE 
        hdiff_dv_sat_fact_journal <> lag_hdiff 
            OR (hdiff_dv_sat_fact_journal <> sat_hdiff AND lag_hdiff IS NULL) 
            OR sat_hdiff IS NULL
    ORDER BY loadts

{% else %}
    WITH stage_cte AS (
        SELECT
        s.hk_dv_hub_fact_journal,
        s.hdiff_dv_sat_fact_journal,
        s.loadts,
        s.recsource,
        lag(s.hdiff_dv_sat_fact_journal) over(PARTITION BY s.hk_dv_hub_fact_journal ORDER BY s.loadts) AS lag_hdiff,
        rabbit_uuid,
	rabbit_дата,
	проведен,
	uuid_жуфвр,
	пометка_удаления,
	дата,
	номер,
	территория_value,
	территория_codе,
	территория_name,
	подразделение_value,
	подразделение_codе,
	подразделение_name,
	смена_value,
	смена_codе,
	смена_name,
	прораб_value,
	прораб_codе,
	прораб_name,
	комментарий,
	ответственный_value,
	ответственный_codе,
	ответственный_name,
	версия_жуфвр_value,
	версия_жуфвр_codе,
	версия_жуфвр_name,
	направление_деятельности_value,
	направление_деятельности_codе,
	направление_деятельности_name

    FROM
        (SELECT
            s.hk_dv_hub_fact_journal,
            s.hdiff_dv_sat_fact_journal,
            s.loadts,
            s.recsource,
            rabbit_uuid,
	rabbit_дата,
	проведен,
	uuid_жуфвр,
	пометка_удаления,
	дата,
	номер,
	территория_value,
	территория_codе,
	территория_name,
	подразделение_value,
	подразделение_codе,
	подразделение_name,
	смена_value,
	смена_codе,
	смена_name,
	прораб_value,
	прораб_codе,
	прораб_name,
	комментарий,
	ответственный_value,
	ответственный_codе,
	ответственный_name,
	версия_жуфвр_value,
	версия_жуфвр_codе,
	версия_жуфвр_name,
	направление_деятельности_value,
	направление_деятельности_codе,
	направление_деятельности_name
        FROM stg_1c_journal s
        ) s
    )

    SELECT DISTINCT
        hk_dv_hub_fact_journal,
        recsource,
        loadts,
        hdiff_dv_sat_fact_journal,
        rabbit_uuid,
	rabbit_дата,
	проведен,
	uuid_жуфвр,
	пометка_удаления,
	дата,
	номер,
	территория_value,
	территория_codе,
	территория_name,
	подразделение_value,
	подразделение_codе,
	подразделение_name,
	смена_value,
	смена_codе,
	смена_name,
	прораб_value,
	прораб_codе,
	прораб_name,
	комментарий,
	ответственный_value,
	ответственный_codе,
	ответственный_name,
	версия_жуфвр_value,
	версия_жуфвр_codе,
	версия_жуфвр_name,
	направление_деятельности_value,
	направление_деятельности_codе,
	направление_деятельности_name
    FROM stage_cte
    WHERE 
        hdiff_dv_sat_fact_journal <> lag_hdiff 
        OR lag_hdiff IS NULL
    ORDER BY loadts
{% endif %}
