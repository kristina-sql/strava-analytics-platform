{{ config(
    materialized='table'
) }}

{% set zones = [
  {'zone_number': 1, 'zone_name': 'Recovery',   'zone_label': 'Z1', 'low': 0.50, 'high': 0.60, 'fatigue_risk': 'Low',    'training_focus': 'Easy recovery, circulation'},
  {'zone_number': 2, 'zone_name': 'Endurance',  'zone_label': 'Z2', 'low': 0.60, 'high': 0.70, 'fatigue_risk': 'Low',    'training_focus': 'Aerobic base, endurance'},
  {'zone_number': 3, 'zone_name': 'Tempo',      'zone_label': 'Z3', 'low': 0.70, 'high': 0.80, 'fatigue_risk': 'Medium', 'training_focus': 'Sustainable hard, muscular endurance'},
  {'zone_number': 4, 'zone_name': 'Threshold',  'zone_label': 'Z4', 'low': 0.80, 'high': 1.00, 'fatigue_risk': 'High',   'training_focus': 'Raise threshold, controlled discomfort'},
] %}

with hr_reference as (
  select
    athlete_id,
    added_at,
    max_hr as reference_bpm
  from {{ ref('dim_athlete') }}
),

hr_ref_scd as (
  select
    athlete_id,
    reference_bpm,
    cast(added_at as date) as valid_from,
    cast(
      lead(added_at) over (partition by athlete_id order by added_at)
      as date
    ) as valid_to
  from hr_reference
),

zone_defs as (
  {% for z in zones %}
  select
    {{ z.zone_number }}::int as zone_number,
    '{{ z.zone_name }}'::varchar(50) as zone_name,
    '{{ z.zone_label }}'::varchar(20) as zone_label,
    {{ z.low }}::decimal(5,2) as hr_pct_low,
    {{ z.high }}::decimal(5,2) as hr_pct_high,
    '{{ z.fatigue_risk }}'::varchar(20) as fatigue_risk,
    '{{ z.training_focus }}'::text as training_focus
  {% if not loop.last %} union all {% endif %}
  {% endfor %}
),

final as (
  select
    {{ dbt_utils.generate_surrogate_key(['r.athlete_id','r.valid_from','zd.zone_number']) }} as hr_zone_id,

    r.athlete_id,
    zd.zone_number,
    zd.zone_name,
    zd.zone_label,
    zd.hr_pct_low,
    zd.hr_pct_high,
    -- floor opposit round up
    floor(r.reference_bpm * zd.hr_pct_low)::int as bpm_low,
    floor(r.reference_bpm * zd.hr_pct_high)::int as bpm_high,

    zd.fatigue_risk,
    zd.training_focus,

    r.valid_from,
    r.valid_to,
    (r.valid_to is null) as is_current

  from hr_ref_scd r
  cross join zone_defs zd
)

select * 
from final