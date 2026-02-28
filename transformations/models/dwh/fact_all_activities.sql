{{ config(
    materialized='incremental',
    unique_key='activity_id',
) }}

with source as (
    select * 
    from {{ ref('stg_strava_activities') }}

),

activities_enriched as (
    select
        activity_id,
        s.athlete_id,
        start_date_utc::date as activity_date,
        
        -- join to get valid ftp for this activity date
        ftp.ftp_id,
        
        -- distance & time
        distance_m,
        distance_km,
        round(moving_time_s / 60.0, 2) as duration_minutes,
        
        -- speed
        avg_speed_kmh,
        max_speed_mps * 3.6 as max_speed_kmh, 
        
        -- power
        avg_power_w,
        max_power_w,
        weighted_average_watts as normalized_power, 
        
        -- heart rate
        avg_hr,
        max_hr,
        
        -- elevation
        elevation_m as elevation_gain,
        
        -- calculated metrics
        round(weighted_average_watts::numeric / nullif(ftp.ftp_watts, 0), 3) as intensity_factor,
        round(
            (moving_time_s * weighted_average_watts * weighted_average_watts::numeric) 
            / (nullif(ftp.ftp_watts, 0) * nullif(ftp.ftp_watts, 0) * 3600.0) * 100, 
            2
        ) as tss,
        nullif(ftp.ftp_watts, 0) as ftp_watts_at_activity,
        
        -- cadence
        average_cadence as avg_cadence,
        
        -- metadata
        activity_name,
        activity_type,
        now() as created_at,
        now() as updated_at
        
    from source s
    left join {{ ref('dim_ftp') }} as ftp
        on s.start_date_utc::date >= ftp.valid_from
        and s.athlete_id = ftp.athlete_id
        and ftp.valid_to is null
)

select * 
from activities_enriched