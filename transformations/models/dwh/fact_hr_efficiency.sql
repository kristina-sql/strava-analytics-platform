{{ config(
    materialized='table'
) }}

with activities as (
  select
    activity_id,
    activity_date,                    
    avg_power_w,
    normalized_power,
    avg_hr
  from {{ ref('fact_all_activities') }}
),

ftp as (
  select
    ftp_id,
    valid_from,
    valid_to,
    ftp_watts
  from {{ ref('dim_ftp') }}
),

activity_with_ftp as (
  -- Pick the FTP that was valid at the time of the activity
  select
    a.activity_id,
    f.ftp_id,
    f.ftp_watts,
    a.avg_power_w,
    a.normalized_power,
    a.avg_hr
  from activities a
  join ftp f
    on a.activity_date >= f.valid_from

    --if this FTP row doesn’t have an end date yet, treat it as still valid,
    --otherwise model reads null as unknown but we need to see true or fale, unknown will be dropped
   and a.activity_date <  coalesce(f.valid_to, '2999-12-31'::date) 
),

hr_zones as (
  select
    hr_zone_id,
    bpm_low,
    bpm_high
  from {{ ref('dim_hr_zones') }}
),

final as (
  select
    awf.activity_id,
    awf.ftp_id,

    -- Optional: map avg_hr into a zone
    hz.hr_zone_id,

    awf.avg_power_w,
    awf.normalized_power,
    awf.avg_hr,

    -- watts_per_bpm = avg_power_w / avg_hr
    case
      when awf.avg_hr > 0 then round(awf.avg_power_w / awf.avg_hr, 2)
      else null
    end as watts_per_bpm,

    -- efficiency_factor (common cycling metric) often = normalized_power / avg_hr
    case
      when awf.avg_hr > 0 and awf.normalized_power is not null
        then round(awf.normalized_power / awf.avg_hr, 3)
      else null
    end as efficiency_factor

  from activity_with_ftp awf
  left join hr_zones hz
    on awf.avg_hr >= hz.bpm_low
   and awf.avg_hr <  hz.bpm_high
)

select * 
from final

