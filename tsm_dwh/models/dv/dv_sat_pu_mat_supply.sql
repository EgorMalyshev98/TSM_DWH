{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['sat', 'ПУ_xl_поставки_материалов']
    )
}}


{% if is_incremental() %}
    WITH active_sat AS (
    SELECT DISTINCT ON (hk_dv_lnk_object_material_date) hk_dv_lnk_object_material_date, hdiff_dv_sat_pu_mat_supply, loadts
    FROM {{this}}
    ORDER BY hk_dv_lnk_object_material_date, loadts DESC)

    , max_load AS (
    SELECT max(loadts) AS max_loadts
    FROM {{this}}
    )
    , stage_cte AS (

        SELECT
        s.hk_dv_lnk_object_material_date,
        s.hdiff_dv_sat_pu_mat_supply,
        s.loadts,
        s.recsource,
        lag(s.hdiff_dv_sat_pu_mat_supply) over(PARTITION BY s.hk_dv_lnk_object_material_date ORDER BY s.loadts) AS lag_hdiff,
        a.loadts AS sat_loadts,
        a.hdiff_dv_sat_pu_mat_supply AS sat_hdiff,
        ед_измерения,
	факт_объем,
	план_объем_суточный

    FROM
        (SELECT
            s.hk_dv_lnk_object_material_date,
            s.hdiff_dv_sat_pu_mat_supply,
            s.loadts,
            s.recsource,
            ед_измерения,
	факт_объем,
	план_объем_суточный
        FROM {{ ref('stg_pu_mat_supply') }} s
        CROSS JOIN max_load ml
        WHERE s.loadts > ml.max_loadts
        ) s
    LEFT JOIN active_sat a ON a.hk_dv_lnk_object_material_date = s.hk_dv_lnk_object_material_date)

    SELECT DISTINCT
        hk_dv_lnk_object_material_date,
        recsource,
        loadts,
        hdiff_dv_sat_pu_mat_supply,
        ед_измерения,
	факт_объем,
	план_объем_суточный
    FROM stage_cte
    WHERE 
        hdiff_dv_sat_pu_mat_supply <> lag_hdiff 
            OR (hdiff_dv_sat_pu_mat_supply <> sat_hdiff AND lag_hdiff IS NULL) 
            OR sat_hdiff IS NULL
    ORDER BY loadts

{% else %}
    WITH stage_cte AS (
        SELECT
        s.hk_dv_lnk_object_material_date,
        s.hdiff_dv_sat_pu_mat_supply,
        s.loadts,
        s.recsource,
        lag(s.hdiff_dv_sat_pu_mat_supply) over(PARTITION BY s.hk_dv_lnk_object_material_date ORDER BY s.loadts) AS lag_hdiff,
        ед_измерения,
	факт_объем,
	план_объем_суточный

    FROM
        (SELECT
            s.hk_dv_lnk_object_material_date,
            s.hdiff_dv_sat_pu_mat_supply,
            s.loadts,
            s.recsource,
            ед_измерения,
	факт_объем,
	план_объем_суточный
        FROM {{ ref('stg_pu_mat_supply') }} s
        ) s
    )

    SELECT DISTINCT
        hk_dv_lnk_object_material_date,
        recsource,
        loadts,
        hdiff_dv_sat_pu_mat_supply,
        ед_измерения,
	факт_объем,
	план_объем_суточный
    FROM stage_cte
    WHERE 
        hdiff_dv_sat_pu_mat_supply <> lag_hdiff 
        OR lag_hdiff IS NULL
    ORDER BY loadts
{% endif %}
