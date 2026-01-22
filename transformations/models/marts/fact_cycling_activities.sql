{{ config(
    materialized='incremental',
    unique_key='activity_id'
) }}

with source as (
    select * from {{ source('neondb.raw', 'stg_strava_activities') }}
    {% if is_incremental() %}
    where start_date > (select max(created_at) from {{ this }})
    {% endif %}
),

activities_enriched as (
    select
        id as activity_id,
        start_date::date as date_id,
        12345678::bigint as athlete_id,  -- your athlete id
        
        -- join to get valid ftp for this activity date
        ftp.ftp_id,
        
        -- distance & time
        distance as distance_m,
        moving_time as duration_seconds,
        
        -- speed
        average_speed * 3.6 as avg_speed_kmh,  -- m/s to km/h
        max_speed * 3.6 as max_speed_kmh,
        
        -- power
        average_watts as avg_power,
        max_watts as max_power,
        weighted_average_watts as normalized_power,
        
        -- heart rate
        average_heartrate as avg_hr,
        max_heartrate as max_hr,
        
        -- elevation
        total_elevation_gain as elevation_gain,
        
        -- calculated metrics
        round(weighted_average_watts::numeric / ftp.ftp_watts, 3) as intensity_factor,
        round(
            (moving_time * weighted_average_watts * (weighted_average_watts::numeric / ftp.ftp_watts)) 
            / (ftp.ftp_watts * 3600.0) * 100, 
            2
        ) as tss,
        ftp.ftp_watts as ftp_watts_at_activity,
        
        -- cadence
        average_cadence as avg_cadence,
        
        -- metadata
        name as activity_name,
        type as activity_type,
        now() as created_at,
        now() as updated_at
        
    from source
    left join {{ ref('dim_ftp') }} as ftp
        on start_date::date >= ftp.valid_from
        and (start_date::date <= ftp.valid_to or ftp.valid_to is null)
        and ftp.athlete_id = 12345678
)

select * from activities_enriched