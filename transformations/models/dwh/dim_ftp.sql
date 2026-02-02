{{ config(
    materialized='table'
) }}

--this model needs to be migrated to sources and deleted from here, becasue it will be maintained manually on neon db
--due to scaling to more athletes

with ftp_tests as (
    select * from (values
        (1, 82489018, 143, 2.69, '2025-01-01'::date, 'Standart 20 min', true),
        (2, 46046096, 185, 2.68, '2026-01-01'::date, 'Unknown', true)
    ) as t(ftp_id, athlete_id, ftp_watts, ftp_wkg, test_date, test_type, is_current)
)

select
    ftp_id,
    athlete_id,
    ftp_watts,
    ftp_wkg,
    test_date,
    test_date as valid_from,
    lead(test_date) over (partition by athlete_id order by test_date) - interval '1 day' as valid_to,
    test_date + interval '6 weeks' as next_retest_due,
    test_type,
    null::text as test_notes, --need to write on 24th Jan overall feelings during test and food
    is_current,
    now() as created_at
from ftp_tests