{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['msat', 'жуфвр_1с']
    )
}}

{% if is_incremental() %}
    WITH active_sat AS (
    SELECT *
    FROM (
        SELECT 
            hk_dv_hub_fact_work,
            hdiff_dv_msat_fact_pikets,
            loadts,
            sub_seq,
            COUNT(*) OVER (PARTITION BY hk_dv_hub_fact_work, loadts) AS cnt,
            rank() OVER (
                PARTITION BY hk_dv_hub_fact_work
                ORDER BY loadts DESC
            ) AS rnk
        FROM dv_msat_fact_pikets
        ) a (
            sat_id,
            sat_hdiff,
            sat_loadts,
            sat_sub_seq,
            sat_count,
            sat_rnk
        )
    WHERE sat_rnk = 1
    ),
    stage_cte AS (
        SELECT 
            stg_id,
            stg_hdiff,
            stg_loadts,
            stg_recsource,
            stg_sub_seq,
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
	группа_пикетов_spider_name,
            COUNT(*) OVER (PARTITION BY stg_id, stg_loadts) AS stg_count,
            lag(stg_hdiff) over(
                PARTITION BY stg_id, stg_sub_seq
                ORDER BY stg_loadts
            ) AS 
                stg_lag_hdiff,
                sat_hdiff,
                sat_count,
                sat_sub_seq,
                sat_rnk
        FROM (
                SELECT 
                    s.hk_dv_hub_fact_work,
                    s.hdiff_dv_msat_fact_pikets,
                    s.loadts,
                    s.recsource,
                    row_number() OVER(PARTITION BY s.hk_dv_hub_fact_work, s.loadts) AS sub_seq,
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
                FROM stg_1c_pikets s
                    CROSS JOIN (
                        SELECT max(loadts) AS max_loadts
                        FROM dv_msat_fact_pikets
                    ) ml
                WHERE s.loadts > ml.max_loadts
            ) s (stg_id, stg_hdiff, stg_loadts, stg_recsource, stg_sub_seq)
        LEFT JOIN active_sat a ON sat_id = stg_id
        AND sat_sub_seq = stg_sub_seq
    ),
    stage_for_insert AS (
        SELECT DISTINCT 
            stg_id,
            stg_loadts
        FROM (
                SELECT stg_id,
                    stg_loadts,
                    stg_hdiff,
                    stg_lag_hdiff,
                    stg_count,
                    lag(stg_count) over(
                        PARTITION BY stg_id,
                        stg_sub_seq
                        ORDER BY stg_loadts
                    ) AS stg_lag_count,
                    sat_hdiff,
                    sat_count,
                    sat_sub_seq,
                    sat_rnk
                FROM stage_cte
            ) t
        WHERE (
                stg_hdiff <> stg_lag_hdiff
                OR stg_count <> stg_lag_count
            )
            OR (
                (
                    stg_hdiff <> sat_hdiff
                    AND stg_lag_hdiff IS NULL
                )
                OR (
                    stg_count <> sat_count
                    AND stg_lag_count IS NULL
                )
            )
    )

    SELECT DISTINCT 
        stg_id,
        stg_recsource,
        stg_loadts,
        stg_hdiff,
        stg_sub_seq,
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
    FROM stage_cte s
    WHERE EXISTS(
            SELECT 1
            FROM stage_for_insert si
            WHERE si.stg_id = s.stg_id
                AND si.stg_loadts = s.stg_loadts
        )
    ORDER BY stg_loadts,
        stg_id,
        stg_sub_seq

{% else %}
    WITH stage_cte AS (
        SELECT 
            stg_id,
            stg_hdiff,
            stg_loadts,
            stg_recsource,
            stg_sub_seq,
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
	группа_пикетов_spider_name,
            COUNT(*) OVER (PARTITION BY stg_id, stg_loadts) AS stg_count,
            lag(stg_hdiff) over(
                PARTITION BY stg_id, stg_sub_seq
                ORDER BY stg_loadts
            ) AS stg_lag_hdiff
        FROM (
                SELECT 
                    s.hk_dv_hub_fact_work,
                    s.hdiff_dv_msat_fact_pikets,
                    s.loadts,
                    s.recsource,
                    row_number() OVER(PARTITION BY s.hk_dv_hub_fact_work, s.loadts) AS sub_seq,
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
                FROM stg_1c_pikets s
            ) s (stg_id, stg_hdiff, stg_loadts, stg_recsource, stg_sub_seq)
    ),
    stage_for_insert AS (
        SELECT DISTINCT 
            stg_id,
            stg_loadts
        FROM (
                SELECT stg_id,
                    stg_loadts,
                    stg_hdiff,
                    stg_lag_hdiff,
                    stg_count,
                    lag(stg_count) over(
                        PARTITION BY stg_id,
                        stg_sub_seq
                        ORDER BY stg_loadts
                    ) AS stg_lag_count
                FROM stage_cte
            ) t
        WHERE (
                stg_hdiff <> stg_lag_hdiff
                OR stg_count <> stg_lag_count
            )
            
            OR stg_lag_hdiff IS null

    )

    SELECT DISTINCT 
        stg_id,
        stg_recsource,
        stg_loadts,
        stg_hdiff,
        stg_sub_seq,
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
    FROM stage_cte s
    WHERE EXISTS(
            SELECT 1
            FROM stage_for_insert si
            WHERE si.stg_id = s.stg_id
                AND si.stg_loadts = s.stg_loadts
        )
    ORDER BY stg_loadts,
        stg_id,
        stg_sub_seq
{% endif %}
