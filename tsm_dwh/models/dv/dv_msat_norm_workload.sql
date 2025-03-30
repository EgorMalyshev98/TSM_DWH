WITH active_sat AS (
SELECT *
FROM (
        SELECT 
            hk_dv_hub_fact_work,
            hdiff_dv_msat_norm_workload,
            loadts,
            sub_seq,
            COUNT(*) OVER (PARTITION BY hk_dv_hub_fact_work, loadts) AS cnt,
            rank() OVER (
                PARTITION BY hk_dv_hub_fact_work
                ORDER BY loadts DESC
            ) AS rnk
        FROM dv_msat_norm_workload
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
        период,
	рассчетный_объем,
	трудоемкость_нормативная,
	трудоемкость_фактическая,
	наемный_ресурс,
	ключевой_ресурс_value,
	ключевой_ресурс_code,
	ключевой_ресурс_name,
	несколько_ключевых_ресурсов,
	жуфвр,
	ключ_документа_ресурс,
	структура_работ_value,
	структура_работ_codе,
	структура_работ_name,
	ресурс_spider_value,
	ресурс_spider_codе,
	ресурс_spider_name,
	аналитика_value,
	аналитика_codе,
	аналитика_name,
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
                s.hdiff_dv_msat_norm_workload,
                s.loadts,
                s.recsource,
                row_number() OVER(PARTITION BY s.hk_dv_hub_fact_work, s.loadts) AS sub_seq,
                период,
	рассчетный_объем,
	трудоемкость_нормативная,
	трудоемкость_фактическая,
	наемный_ресурс,
	ключевой_ресурс_value,
	ключевой_ресурс_code,
	ключевой_ресурс_name,
	несколько_ключевых_ресурсов,
	жуфвр,
	ключ_документа_ресурс,
	структура_работ_value,
	структура_работ_codе,
	структура_работ_name,
	ресурс_spider_value,
	ресурс_spider_codе,
	ресурс_spider_name,
	аналитика_value,
	аналитика_codе,
	аналитика_name
            FROM stg_1c_norm_workload s
                CROSS JOIN (
                    SELECT max(loadts) AS max_loadts
                    FROM dv_msat_norm_workload
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


INSERT INTO dv_msat_norm_workload (hk_dv_hub_fact_work, recsource, loadts, hdiff_dv_msat_norm_workload, sub_seq, период,
	рассчетный_объем,
	трудоемкость_нормативная,
	трудоемкость_фактическая,
	наемный_ресурс,
	ключевой_ресурс_value,
	ключевой_ресурс_code,
	ключевой_ресурс_name,
	несколько_ключевых_ресурсов,
	жуфвр,
	ключ_документа_ресурс,
	структура_работ_value,
	структура_работ_codе,
	структура_работ_name,
	ресурс_spider_value,
	ресурс_spider_codе,
	ресурс_spider_name,
	аналитика_value,
	аналитика_codе,
	аналитика_name)
SELECT DISTINCT 
    stg_id,
    stg_recsource,
    stg_loadts,
    stg_hdiff,
    stg_sub_seq,
    период,
	рассчетный_объем,
	трудоемкость_нормативная,
	трудоемкость_фактическая,
	наемный_ресурс,
	ключевой_ресурс_value,
	ключевой_ресурс_code,
	ключевой_ресурс_name,
	несколько_ключевых_ресурсов,
	жуфвр,
	ключ_документа_ресурс,
	структура_работ_value,
	структура_работ_codе,
	структура_работ_name,
	ресурс_spider_value,
	ресурс_spider_codе,
	ресурс_spider_name,
	аналитика_value,
	аналитика_codе,
	аналитика_name
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
ON CONFLICT DO NOTHING;
