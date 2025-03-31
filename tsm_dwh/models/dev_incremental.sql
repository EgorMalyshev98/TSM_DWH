{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['dev']
  )
}}

SELECT distinct on (id_journal, id_work)
    id_journal,
    id_work,
    min(loadts) AS loadts
FROM
    "postgres"."public"."dev_stg" stg

{% if is_incremental() %}
  where not exists (
    select 1
    from {{this}} target
    where 
        target.id_journal = stg.id_journal 
            and
        target.id_work = stg.id_work
  )
{% endif %}

GROUP BY id_journal, id_work
