{{ config(materialized='table') }}

WITH results AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY year, round, "Abbreviation"
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM {{ source('silver', 'results') }}
),

results_deduped AS (
    SELECT * FROM results WHERE rn = 1
),

laps_agg AS (
    SELECT
        year,
        round,
        "Driver"                                            AS driver_code,
        COUNT(*)                                            AS total_laps,
        AVG(TRY_CAST("LapTime" AS DOUBLE))                 AS avg_lap_time_seconds,
        MIN(TRY_CAST("LapTime" AS DOUBLE))                 AS fastest_lap_seconds,
        SUM(CASE WHEN "IsPersonalBest" THEN 1 ELSE 0 END)  AS personal_best_count,
        AVG("SpeedI1")                                      AS avg_speed_sector1,
        AVG("SpeedFL")                                      AS avg_speed_finish_line,
        MAX("SpeedST")                                      AS top_speed
    FROM {{ source('silver', 'laps') }}
    GROUP BY year, round, "Driver"
)

SELECT
    CAST(r.year AS VARCHAR) || '_' ||
    CAST(r.round AS VARCHAR) || '_' ||
    r."Abbreviation"                        AS result_surrogate_key,
    r.year                                  AS race_year,
    r.round                                 AS race_round,
    r.event_name                            AS race_name,
    r."Abbreviation"                        AS driver_code,
    r."FullName"                            AS driver_name,
    r."TeamName"                            AS team_name,
    r."Position"                            AS finish_position,
    r."GridPosition"                        AS grid_position,
    r."Points"                              AS points_scored,
    r."Status"                              AS race_status,
    r."ClassifiedPosition"                  AS classified_position,
    CAST(r."GridPosition" AS DOUBLE) -
        CAST(r."Position" AS DOUBLE)        AS positions_gained,
    r."Points" > 0                          AS is_points_finish,
    l.total_laps,
    l.avg_lap_time_seconds,
    l.fastest_lap_seconds,
    l.personal_best_count,
    l.avg_speed_sector1,
    l.avg_speed_finish_line,
    l.top_speed,
    current_timestamp                       AS dbt_loaded_at
FROM results_deduped r
LEFT JOIN laps_agg l
    ON  r.year        = l.year
    AND r.round       = l.round
    AND r."Abbreviation" = l.driver_code