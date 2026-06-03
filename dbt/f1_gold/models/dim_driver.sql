{{ config(materialized='table') }}

WITH source AS (
    SELECT DISTINCT
        "Abbreviation"    AS driver_code,
        "FullName"        AS driver_name,
        "TeamName"        AS team_name,
        year,
        _ingested_at
    FROM {{ source('silver', 'results') }}
    WHERE "Abbreviation" IS NOT NULL
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY driver_code || '_' || team_name || '_' || CAST(year AS VARCHAR)
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM source
)

SELECT
    driver_code || '_' || team_name || '_' || CAST(year AS VARCHAR) AS driver_surrogate_key,
    driver_code,
    driver_name,
    team_name,
    year                AS effective_year,
    year                AS valid_from,
    9999                AS valid_to,
    TRUE                AS is_current_record,
    _ingested_at        AS dbt_loaded_at
FROM deduped
WHERE rn = 1