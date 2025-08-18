{{
    config(
        materialized='table'
    )
}}


WITH active_sat AS (
    SELECT hk_dv_lnk_object_material_date,
        recsource,
        loadts,
        hdiff_dv_sat_pu_mat_supply,
        ед_измерения,
        факт_объем,
        план_объем_суточный,
        ROW_NUMBER() over(
            PARTITION BY hk_dv_lnk_object_material_date
            ORDER BY loadts DESC
        ) AS rn
    FROM {{ ref('dv_sat_pu_mat_supply') }}
)

SELECT a.hk_dv_lnk_object_material_date,
    bk_факт_дата,
    bk_материал,
    bk_объект,

    a.recsource,
    a.loadts,
    a.hdiff_dv_sat_pu_mat_supply,
    a.ед_измерения,
    a.факт_объем,
    a.план_объем_суточный::numeric,

    sum(a.факт_объем) OVER(PARTITION BY bk_объект, bk_материал ORDER BY bk_факт_дата) as факт_накопительно,
    sum(a.план_объем_суточный::numeric) OVER(PARTITION BY bk_объект, bk_материал ORDER BY bk_факт_дата) as план_накопительно

    
FROM active_sat a
    JOIN {{ ref('dv_lnk_object_material_date') }}  l USING(hk_dv_lnk_object_material_date)
    JOIN {{ ref('dv_hub_material') }}  USING(hk_dv_hub_material)
    JOIN {{ ref('dv_hub_object') }}  USING(hk_dv_hub_object)
    JOIN {{ ref('dv_hub_date') }}  USING(hk_dv_hub_date)
WHERE rn = 1