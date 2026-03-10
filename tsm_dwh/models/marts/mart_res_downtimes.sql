{{
    config(
        materialized='incremental',
        unique_key=['направление_деятельности_name', 'ресурс_name', 'ресурс_codе', 'дата', 'смена_name'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        indexes=[
            {
                'columns': ['направление_деятельности_name', 'ресурс_name', 'ресурс_codе', 'дата', 'смена_name'],
                'unique': true
            },
            {
                'columns': ['дата']
            }
        ]
    )
}}

-- простои техники по по объектам посменно

WITH

{% if is_incremental() %}
changed_works AS (
    SELECT hk_dv_hub_fact_work
    FROM {{ ref('dv_sat_fact_work') }}
    WHERE loadts > (SELECT COALESCE(MAX(_dbt_loaded_at), '1970-01-01') FROM {{ this }})

    UNION

    SELECT lnk.hk_dv_hub_fact_work
    FROM {{ ref('dv_lnk_fact_journal_fact_work') }} lnk
    JOIN {{ ref('dv_sat_fact_journal') }} j USING(hk_dv_hub_fact_journal)
    WHERE j.loadts > (SELECT COALESCE(MAX(_dbt_loaded_at), '1970-01-01') FROM {{ this }})

    UNION

    SELECT lnk.hk_dv_hub_fact_work
    FROM {{ ref('dv_lnk_fact_work_tech') }} lnk
    JOIN {{ ref('dv_sat_fact_tech') }} t USING(hk_dv_lnk_fact_work_tech)
    WHERE t.loadts > (SELECT COALESCE(MAX(_dbt_loaded_at), '1970-01-01') FROM {{ this }})
),
{% endif %}

active_lnk_fact_work_tech AS (
    SELECT hk_dv_lnk_fact_work_tech
    FROM (
        SELECT
            hk_dv_lnk_fact_work_tech,
            end_date,
            ROW_NUMBER() OVER(PARTITION BY hk_dv_lnk_fact_work_tech ORDER BY loadts DESC) AS rn
        FROM {{ ref('dv_esat_fact_work_tech') }}
    ) t
    WHERE rn = 1 AND end_date IS NULL
),

active_tech AS MATERIALIZED (
    SELECT
        lnk.hk_dv_hub_fact_work,
        encode(lnk.hk_dv_hub_tech, 'hex') AS hk_dv_hub_tech,
        hub.bk_ресурс_uuid,
        sat.госномер_техника_не_найдена,
        sat.часы,
        sat.ресурс_codе,
        sat.ресурс_name,
        sat.аналитика_name,
        sat.max_loadts AS tech_max_loadts
    FROM (
        SELECT
            hk_dv_lnk_fact_work_tech,
            госномер_техника_не_найдена,
            часы,
            ресурс_codе,
            ресурс_name,
            аналитика_name,
            MAX(loadts) OVER(PARTITION BY hk_dv_lnk_fact_work_tech) AS max_loadts,
            ROW_NUMBER() OVER(PARTITION BY hk_dv_lnk_fact_work_tech ORDER BY loadts DESC) AS rn
        FROM {{ ref('dv_sat_fact_tech') }}
    ) sat
    JOIN active_lnk_fact_work_tech alnk USING(hk_dv_lnk_fact_work_tech)
    JOIN {{ ref('dv_lnk_fact_work_tech') }} lnk USING(hk_dv_lnk_fact_work_tech)
    JOIN {{ ref('dv_hub_tech') }} hub USING(hk_dv_hub_tech)
    WHERE rn = 1
      AND hub.bk_ресурс_uuid != '-1'
    {% if is_incremental() %}
      AND lnk.hk_dv_hub_fact_work IN (SELECT hk_dv_hub_fact_work FROM changed_works)
    {% endif %}
),

active_journal AS (
    SELECT *
    FROM (
        SELECT
            hk_dv_hub_fact_journal,
            TRIM(территория_value::varchar) AS территория_value,
            смена_name,
            направление_деятельности_name,
            дата,
            проведен,
            пометка_удаления,
            MAX(loadts) OVER(PARTITION BY hk_dv_hub_fact_journal) AS max_loadts,
            ROW_NUMBER() OVER(PARTITION BY hk_dv_hub_fact_journal ORDER BY loadts DESC) AS rn
        FROM {{ ref('dv_sat_fact_journal') }}
        WHERE проведен IS TRUE AND пометка_удаления IS FALSE
    ) t
    WHERE rn = 1
),

sat_work AS (
    SELECT *
    FROM (
        SELECT
            hk_dv_hub_fact_work,
            тип_spider,
            пометка_удаления,
            MAX(loadts) OVER(PARTITION BY hk_dv_hub_fact_work) AS max_loadts,
            ROW_NUMBER() OVER(PARTITION BY hk_dv_hub_fact_work ORDER BY loadts DESC) AS rn
        FROM {{ ref('dv_sat_fact_work') }}
        WHERE пометка_удаления IS FALSE
    ) t
    WHERE rn = 1
    {% if is_incremental() %}
      AND hk_dv_hub_fact_work IN (SELECT hk_dv_hub_fact_work FROM changed_works)
    {% endif %}
),

active_fact_works AS MATERIALIZED (
    SELECT
        w.hk_dv_hub_fact_work,
        j.смена_name,
        j.направление_деятельности_name,
        j.дата,
        GREATEST(w.max_loadts, j.max_loadts) AS max_loadts
    FROM sat_work w
    JOIN {{ ref('dv_lnk_fact_journal_fact_work') }} lnk USING(hk_dv_hub_fact_work)
    JOIN active_journal j USING(hk_dv_hub_fact_journal)
    JOIN {{ source('public', 'ref_object_names') }} r USING(территория_value)
    WHERE
        j.проведен IS TRUE
        AND j.пометка_удаления IS FALSE
        AND w.пометка_удаления IS FALSE
        AND r.is_active IS TRUE
),

dwt_calc AS (
    SELECT
        w.направление_деятельности_name,
        t.ресурс_name,
        t.ресурс_codе,
        t.аналитика_name,
        w.смена_name,
        w.дата,
        CASE
            WHEN t.bk_ресурс_uuid NOT IN (
                '5195e3a3-66ff-11ec-a16c-00224dda35d0',
                'e9ddfb4c-5be7-11ec-a16c-00224dda35d0'
            )
            THEN t.госномер_техника_не_найдена
            ELSE t.аналитика_name
        END AS имя_ресурса,
        SUM(t.часы)                                     AS часы,
        GREATEST(0, 10 - SUM(t.часы))                   AS простой,
        MAX(GREATEST(t.tech_max_loadts, w.max_loadts))  AS max_loadts
    FROM active_tech t
    JOIN active_fact_works w USING(hk_dv_hub_fact_work)
    WHERE t.bk_ресурс_uuid NOT IN (
        '5da5e5d1-6257-11ec-a16c-00224dda35d0',
        '5da5e5d2-6257-11ec-a16c-00224dda35d0'
    )
    GROUP BY
        w.направление_деятельности_name,
        t.аналитика_name,
        t.ресурс_codе,
        w.смена_name,
        t.ресурс_name,
        w.дата,
        7  -- имя_ресурса
)

SELECT
    направление_деятельности_name,
    ресурс_name,
    ресурс_codе,
    дата,
    смена_name,
    SUM(часы)       AS часы,
    SUM(простой)    AS простой,
    MAX(max_loadts) AS _dbt_loaded_at
FROM dwt_calc
GROUP BY
    направление_деятельности_name,
    ресурс_name,
    ресурс_codе,
    дата,
    смена_name