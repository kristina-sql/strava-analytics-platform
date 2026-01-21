{{ 
    config(
        materialized='view',
        schema = 'raw'
    ) 
}}
with source as (
    select
        activity_id,
        extracted_at_utc,
        payload
    from {{source('neondb.raw','strava_activities')}}
),

final as (
    select
        activity_id,

        -- timestamps
        (payload->>'start_date')::timestamptz as start_date_utc,
        extracted_at_utc,

        -- identifiers / text
        payload->>'name' as activity_name,
        payload->>'type' as activity_type,
        payload->>'sport_type' as sport_type,

        -- numerics (Strava returns meters, seconds, m/s)
        (payload->>'distance')::numeric as distance_m,
        (payload->>'moving_time')::integer as moving_time_s,
        (payload->>'elapsed_time')::integer as elapsed_time_s,
        (payload->>'total_elevation_gain')::numeric as elevation_m,

        (payload->>'average_speed')::numeric as avg_speed_mps,
        (payload->>'max_speed')::numeric as max_speed_mps,

        -- optional fields (may be null)
        (payload->>'average_watts')::numeric as avg_power_w,
        (payload->>'max_watts')::numeric as max_power_w,
        (payload->>'kilojoules')::numeric as kilojoules,

        (payload->>'average_heartrate')::numeric as avg_hr,
        (payload->>'max_heartrate')::numeric as max_hr,

        -- conversions 
        ((payload->>'distance')::numeric / 1000) as distance_km,
        ((payload->>'moving_time')::numeric / 60) as moving_time_min,
        ((payload->>'average_speed')::numeric * 3.6) as avg_speed_kmh

    from source
)

select * 
from final
