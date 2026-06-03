{{ config(materialized='table') }}

SELECT DISTINCT
    CAST(year AS VARCHAR) || '_' || CAST(round AS VARCHAR)  AS race_surrogate_key,
    year                AS race_year,
    round               AS race_round,
    event_name          AS race_name,
    current_timestamp   AS dbt_loaded_at
FROM {{ source('silver', 'results') }}
WHERE year IS NOT NULL
  AND round IS NOT NULL