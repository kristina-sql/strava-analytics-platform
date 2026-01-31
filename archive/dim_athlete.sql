{{ config(
    materialized='table'
) }}

select
    12345678::bigint as athlete_id,  -- need to update it later, for now it is not that important
    'Kristina' as first_name,
    'Artemenkova' as last_name,
    53.0 as weight_kg,  
    195 as max_hr,      
    65 as resting_hr,   
    current_date as added_at,
    now() as updated_at