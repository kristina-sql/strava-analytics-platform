{{ config(
    materialized = 'table'
) }}

with athlete as (
    select athlete_id
    from {{ ref('dim_athlete') }}
),

-- 1) Grab only VirtualRide activities 
rides as (
    select
        activity_id,
        athlete_id,
        activity_date,
        distance_m,
        duration_minutes,
        avg_speed_kmh,
        max_speed_kmh,
        avg_power_w,
        normalized_power,
        avg_hr,
        max_hr,
        intensity_factor,
        tss,
        ftp_watts_at_activity
    from {{ ref('fact_all_activities') }}
    where activity_type = 'VirtualRide'
),

-- 2) Attach efficiency metrics already calculated
rides_with_eff as (
    select
        r.*,
        he.watts_per_bpm,
        he.efficiency_factor,
        he.hr_zone_id
    from rides r
    left join {{ ref('fact_hr_efficiency') }} he
        on r.activity_id = he.activity_id
),

-- 3)  attach power zone classification (based on normalized power) ??
--  current fact_activity_power_zones returns 1 row per activity with matched zone logic.
rides_with_zones as (
    select
        rwe.*,

        paz.classification_basis,
        paz.power_watts as classified_power_watts,

        -- power zone info
        dpz.zone_number as power_zone_number,
        dpz.zone_label  as power_zone_label,
        dpz.zone_name   as power_zone_name

    from rides_with_eff rwe
    left join {{ ref('fact_activity_power_zone') }} paz
        on rwe.activity_id = paz.activity_id

    left join {{ ref('dim_power_zones') }} dpz
        on dpz.ftp_id = paz.ftp_id
       and paz.power_watts >= dpz.watts_low
       and (
            dpz.watts_high is null
            or paz.power_watts < dpz.watts_high
       )
),

-- 4) Daily aggregation: one row per athlete_id per date
daily as (
    select
        athlete_id,
        activity_date,

        count(*) as ride_count,

        -- volume
        round(sum(distance_m) / 1000.0, 2) as distance_km,
        round(sum(duration_minutes), 2) as duration_minutes,

        -- performance (weighted by duration so short rides don’t dominate)
        case when sum(duration_minutes) > 0
            then round(sum(avg_speed_kmh * duration_minutes) / sum(duration_minutes), 2)
        end as avg_speed_kmh_wt,

        case when sum(duration_minutes) > 0
            then round(sum(avg_power_w * duration_minutes) / sum(duration_minutes), 0)
        end as avg_power_w_wt,

        case when sum(duration_minutes) > 0 and sum(normalized_power * duration_minutes) is not null
            then round(sum(normalized_power * duration_minutes) / sum(duration_minutes), 0)
        end as normalized_power_wt,

        -- physiological cost (duration-weighted)
        case when sum(duration_minutes) > 0
            then round(sum(avg_hr * duration_minutes) / sum(duration_minutes), 0)
        end as avg_hr_bpm_wt,

        -- “same cost, better output” indicators
        -- watts_per_bpm already expresses this idea directly
        case when sum(duration_minutes) > 0 and sum(watts_per_bpm * duration_minutes) is not null
            then round(sum(watts_per_bpm * duration_minutes) / sum(duration_minutes), 2)
        end as watts_per_bpm_wt,

        case when sum(duration_minutes) > 0 and sum(efficiency_factor * duration_minutes) is not null
            then round(sum(efficiency_factor * duration_minutes) / sum(duration_minutes), 3)
        end as efficiency_factor_wt,

        -- load
        round(sum(tss), 2) as tss_total,
        round(avg(intensity_factor), 3) as intensity_factor_avg,

        -- simple zone signal (based on AVG HR zone per ride
        sum(case when hr_zone_id is not null then 1 else 0 end) as rides_with_hr_zone,
        sum(case when hr_zone_id is not null and hr_zone_id like '%' then 0 else 0 end) as _placeholder,

        -- high HR zone : classified into highest power zone (my Z4 threshold+)
        sum(case
              when hr_zone_id is not null
                   and hr_zone_id in (
                     select hr_zone_id from {{ ref('dim_hr_zones') }} where zone_number = 4
                   )
              then 1 else 0
            end
        ) as high_hr_zone_rides,

        -- rides classified into highest power zone (my Z4 threshold+)
        sum(case when power_zone_number = 4 then 1 else 0 end) as high_power_zone_rides

    from rides_with_zones
    group by 1, 2
),

-- 5) days with 0 rides
date_spine as (
    select
        d.date_id as activity_date
    from {{ ref('dim_date') }} d
    where d.date_id between
        (select min(activity_date) from rides)
        and
        (select max(activity_date) from rides)
),

-- 6) Build final daily table with gaps + rolling fatigue signals
final as (
    select
        a.athlete_id,
        ds.activity_date,

        coalesce(d.ride_count, 0) as ride_count,
        coalesce(d.distance_km, 0) as distance_km,
        coalesce(d.duration_minutes, 0) as duration_minutes,

        d.avg_speed_kmh_wt,
        d.avg_power_w_wt,
        d.normalized_power_wt,
        d.avg_hr_bpm_wt,

        d.watts_per_bpm_wt,
        d.efficiency_factor_wt,

        coalesce(d.tss_total, 0) as tss_total,
        d.intensity_factor_avg,

        coalesce(d.high_hr_zone_rides, 0) as high_hr_zone_rides,
        coalesce(d.high_power_zone_rides, 0) as high_power_zone_rides,

        -- Rolling load / fatigue signals (very practical for “am I overdoing it?”)
        sum(coalesce(d.tss_total, 0)) over (
            partition by a.athlete_id
            order by ds.activity_date
            rows between 6 preceding and current row
        ) as tss_7d,

        sum(coalesce(d.tss_total, 0)) over (
            partition by a.athlete_id
            order by ds.activity_date
            rows between 27 preceding and current row
        ) as tss_28d,

        -- Days since last VirtualRide
        ds.activity_date
          - max(case when coalesce(d.ride_count, 0) > 0 then ds.activity_date end) over (
                partition by a.athlete_id
                order by ds.activity_date
                rows between unbounded preceding and current row
            ) as days_since_last_ride

    from athlete a
    cross join date_spine ds
    left join daily d
        on d.athlete_id = a.athlete_id
       and d.activity_date = ds.activity_date
)

select *
from final
order by athlete_id, activity_date
