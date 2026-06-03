{{ config(materialized='table') }}

SELECT
    driver_code,
    driver_name,
    race_year,
    team_name,
    COUNT(*)                                                AS races_entered,
    SUM(points_scored)                                      AS total_points,
    COUNT(CASE WHEN finish_position = 1   THEN 1 END)       AS wins,
    COUNT(CASE WHEN finish_position <= 3  THEN 1 END)       AS podiums,
    COUNT(CASE WHEN finish_position <= 10 THEN 1 END)       AS points_finishes,
    ROUND(AVG(CAST(finish_position AS DOUBLE)), 2)          AS avg_finish_position,
    ROUND(AVG(avg_lap_time_seconds), 3)                     AS avg_lap_time_seconds,
    ROUND(MIN(fastest_lap_seconds), 3)                      AS best_lap_ever_seconds,
    SUM(positions_gained)                                   AS total_positions_gained,
    MAX(top_speed)                                          AS max_top_speed
FROM {{ ref('fact_race_results') }}
GROUP BY driver_code, driver_name, race_year, team_name
ORDER BY race_year DESC, total_points DESC