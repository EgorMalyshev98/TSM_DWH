
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['msat', 'жуфвр_1с'],
        indexes=[
            {
                'columns': ['loadts']
            }
        ]
    )
}}

{% if is_incremental() %}
    WITH active_sat AS (
        SELECT *
        FROM (
            SELECT
                hk_dv_hub_fact_work,
                hdiff_dv_msat_fact_materials,
                loadts,
                sub_seq,
                rank() OVER (
                    PARTITION BY hk_dv_hub_fact_work
                    ORDER BY loadts DESC
                ) AS rnk
            FROM {{ this }}
            ) a (
                sat_id,
                sat_hdiff,
                sat_loadts,
                sat_sub_seq,
                sat_rnk
            )
        WHERE sat_rnk = 1
    ),
    -- Хеш активного блока: GROUP BY — string_agg с ORDER BY работает только в агрегатах
    active_blocks AS (
        SELECT
            sat_id,
            COUNT(*)                                                        AS sat_count,
            string_agg(sat_hdiff::varchar, ',' ORDER BY sat_hdiff)          AS sat_block_hash
        FROM active_sat
        GROUP BY sat_id
    ),
    stage_raw AS (
        SELECT
            s.hk_dv_hub_fact_work,
            s.hdiff_dv_msat_fact_materials,
            s.loadts,
            s.recsource,
            номер_строки,
	примечание,
	объем_материала,
	ед_изм,
	ресурс_value,
	ресурс_codе,
	ресурс_name
        FROM {{ ref('stg_1c_materials') }} s
        CROSS JOIN (
            SELECT max(loadts) AS max_loadts
            FROM {{ this }}
        ) ml
        WHERE s.loadts > ml.max_loadts
    ),
    -- Хеш блока stage: GROUP BY
    stage_block_hashes AS (
        SELECT
            hk_dv_hub_fact_work,
            loadts,
            COUNT(*)                                                                AS stg_count,
            string_agg(hdiff_dv_msat_fact_materials::varchar, ',' ORDER BY hdiff_dv_msat_fact_materials)            AS stg_block_hash
        FROM stage_raw
        GROUP BY hk_dv_hub_fact_work, loadts
    ),
    stage_with_seq AS (
        SELECT
            hk_dv_hub_fact_work  AS stg_id,
            hdiff_dv_msat_fact_materials      AS stg_hdiff,
            loadts            AS stg_loadts,
            recsource         AS stg_recsource,
            -- ORDER BY hdiff делает sub_seq детерминированным
            row_number() OVER (
                PARTITION BY hk_dv_hub_fact_work, loadts
                ORDER BY hdiff_dv_msat_fact_materials
            ) AS sub_seq,
            stg_count,
            stg_block_hash,
            номер_строки,
	примечание,
	объем_материала,
	ед_изм,
	ресурс_value,
	ресурс_codе,
	ресурс_name
        FROM (
            SELECT r.*, h.stg_count, h.stg_block_hash
            FROM stage_raw r
            JOIN stage_block_hashes h
                ON h.hk_dv_hub_fact_work = r.hk_dv_hub_fact_work
                AND h.loadts = r.loadts
        ) t
    ),
    -- Сравнение: текущий батч в stg / предыдущий батч в stg / активный sat
    block_cte AS (
        SELECT
            h.hk_dv_hub_fact_work  AS stg_id,
            h.loadts            AS stg_loadts,
            h.stg_block_hash,
            lag(h.stg_block_hash) OVER (
                PARTITION BY h.hk_dv_hub_fact_work
                ORDER BY h.loadts
            ) AS lag_block_hash,
            a.sat_block_hash
        FROM stage_block_hashes h
        LEFT JOIN active_blocks a ON a.sat_id = h.hk_dv_hub_fact_work
    ),
    stage_for_insert AS (
        SELECT stg_id, stg_loadts
        FROM block_cte
        WHERE
            -- блок изменился относительно предыдущего батча в stage
            stg_block_hash <> lag_block_hash
            -- первый батч для ключа в stage: сравнение с активным блоком в sat
            -- вставка только при отличии или если ключа в sat ещё нет
            OR (
                lag_block_hash IS NULL
                AND (sat_block_hash IS NULL OR stg_block_hash <> sat_block_hash)
            )
    )

    SELECT DISTINCT
        stg_id        AS hk_dv_hub_fact_work,
        stg_recsource AS recsource,
        stg_loadts    AS loadts,
        stg_hdiff     AS hdiff_dv_msat_fact_materials,
        sub_seq,
        номер_строки,
	примечание,
	объем_материала,
	ед_изм,
	ресурс_value,
	ресурс_codе,
	ресурс_name
    FROM stage_with_seq s
    WHERE EXISTS (
        SELECT 1
        FROM stage_for_insert si
        WHERE si.stg_id = s.stg_id
            AND si.stg_loadts = s.stg_loadts
    )
    ORDER BY stg_loadts, stg_id, sub_seq

{% else %}
    WITH stage_raw AS (
        SELECT
            s.hk_dv_hub_fact_work,
            s.hdiff_dv_msat_fact_materials,
            s.loadts,
            s.recsource,
            номер_строки,
	примечание,
	объем_материала,
	ед_изм,
	ресурс_value,
	ресурс_codе,
	ресурс_name
        FROM {{ ref('stg_1c_materials') }} s
    ),
    stage_block_hashes AS (
        SELECT
            hk_dv_hub_fact_work,
            loadts,
            COUNT(*)                                                                AS stg_count,
            string_agg(hdiff_dv_msat_fact_materials::varchar, ',' ORDER BY hdiff_dv_msat_fact_materials)            AS stg_block_hash
        FROM stage_raw
        GROUP BY hk_dv_hub_fact_work, loadts
    ),
    stage_with_seq AS (
        SELECT
            hk_dv_hub_fact_work  AS stg_id,
            hdiff_dv_msat_fact_materials      AS stg_hdiff,
            loadts            AS stg_loadts,
            recsource         AS stg_recsource,
            row_number() OVER (
                PARTITION BY hk_dv_hub_fact_work, loadts
                ORDER BY hdiff_dv_msat_fact_materials
            ) AS sub_seq,
            stg_count,
            stg_block_hash,
            номер_строки,
	примечание,
	объем_материала,
	ед_изм,
	ресурс_value,
	ресурс_codе,
	ресурс_name
        FROM (
            SELECT r.*, h.stg_count, h.stg_block_hash
            FROM stage_raw r
            JOIN stage_block_hashes h
                ON h.hk_dv_hub_fact_work = r.hk_dv_hub_fact_work
                AND h.loadts = r.loadts
        ) t
    ),
    block_cte AS (
        SELECT
            hk_dv_hub_fact_work    AS stg_id,
            loadts              AS stg_loadts,
            stg_block_hash,
            lag(stg_block_hash) OVER (
                PARTITION BY hk_dv_hub_fact_work
                ORDER BY loadts
            ) AS lag_block_hash
        FROM stage_block_hashes
    ),
    stage_for_insert AS (
        SELECT stg_id, stg_loadts
        FROM block_cte
        WHERE stg_block_hash <> lag_block_hash
            OR lag_block_hash IS NULL
    )

    SELECT DISTINCT
        stg_id        AS hk_dv_hub_fact_work,
        stg_recsource AS recsource,
        stg_loadts    AS loadts,
        stg_hdiff     AS hdiff_dv_msat_fact_materials,
        sub_seq,
        номер_строки,
	примечание,
	объем_материала,
	ед_изм,
	ресурс_value,
	ресурс_codе,
	ресурс_name
    FROM stage_with_seq s
    WHERE EXISTS (
        SELECT 1
        FROM stage_for_insert si
        WHERE si.stg_id = s.stg_id
            AND si.stg_loadts = s.stg_loadts
    )
    ORDER BY stg_loadts, stg_id, sub_seq
{% endif %}
