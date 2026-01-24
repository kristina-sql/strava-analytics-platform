{{ config(
    materialized='table',
    unique_key='activity_id'
) }}

--body doesn’t respond linearly to effort, so np might make more sense to rely on, need to explore more
--also if there will be problem with np being null, can use fallback logic to avg power with coalesce()
with activities as (
    select
        activity_id,
        ftp_id,
        normalized_power
    from {{ ref('fact_all_activities') }}
    where normalized_power is not null
),

matched_zone as (
    select
        a.activity_id,
        a.ftp_id,
        'normalized_power'::varchar(20) as classification_basis,
        a.normalized_power::decimal(6,2) as power_watts
    from activities a
    join {{ ref('dim_power_zones') }} dz
      on dz.ftp_id = a.ftp_id
     and a.normalized_power >= dz.watts_low
     and (dz.watts_high is null or a.normalized_power < dz.watts_high)
)

select * 
from matched_zone