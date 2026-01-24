{{ config(
    materialized='incremental',
    unique_key='activity_id'
) }}

with source as (
    select * 
    from {{ ref('stg_strava_activities') }}

    --strava IDs are sequential
    {% if is_incremental() %}
    where activity_id > (select max(activity_id) from {{ this }})
    {% endif %}
),

activities_enriched as (
    select
        activity_id,
        start_date_utc::date as activity_date,
        12345678::bigint as athlete_id,  -- your athlete id
        
        -- join to get valid ftp for this activity date
        ftp.ftp_id,
        
        -- distance & time
        distance_m,
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
        round(weighted_average_watts::numeric / ftp.ftp_watts, 3) as intensity_factor,
        round(
            (round(moving_time_s / 60.0, 2) * weighted_average_watts * (weighted_average_watts::numeric / ftp.ftp_watts)) 
            / (ftp.ftp_watts * 3600.0) * 100, 
            2
        ) as tss,
        ftp.ftp_watts as ftp_watts_at_activity,
        
        -- cadence
        average_cadence as avg_cadence,
        
        -- metadata
        activity_name,
        activity_type,
        now() as created_at,
        now() as updated_at
        
    from source 
    left join {{ ref('dim_ftp') }} as ftp
        on start_date_utc::date >= ftp.valid_from
        and ftp.valid_to is null
        and ftp.athlete_id = athlete_id
)

select * 
from activities_enriched