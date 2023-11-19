{{
  config(
    materialized = 'view',
    enabled = true,
    )
}}

with datebase as (

    {{ dbt_date.get_date_dimension('1970-01-01', '2050-01-01') }} -- noqa

),

renamed as (

    select

        {{ dbt_utils.surrogate_key(['date_day']) }} as datekey,

        date_day as dateday,
        prior_date_day as priordateday,
        next_date_day as nextdateday,
        prior_year_date_day as prioryeardateday,
        prior_year_over_year_date_day as prioryearoveryeardateday,
        day_of_week as dayofweek, -- noqa
        day_of_week_iso as dayofweekiso,
        day_of_week_name as dayofweekname,
        day_of_week_name_short as dayofweeknameshort,
        day_of_month as dayofmonth, -- noqa
        day_of_year as dayofyear, -- noqa
        week_of_year as weekofyear,
        iso_week_start_date as isoweekstartdate,
        iso_week_end_date as isoweekenddate,
        prior_year_iso_week_start_date as prioryearisoweekstartdate,
        prior_year_iso_week_end_date as prioryearisoweekenddate,
        iso_week_of_year as isoweekofyear,
        prior_year_week_of_year as prioryearweekofyear,
        prior_year_iso_week_of_year as prioryearisoweekofyear,
        month_of_year as monthofyear,
        month_name as monthname, -- noqa
        month_name_short as monthnameshort,
        month_start_date as monthstartdate,
        month_end_date as monthenddate,
        prior_year_month_start_date as prioryearmonthstartdate,
        prior_year_month_end_date as prioryearmonthenddate,
        quarter_of_year as quarterofyear,
        quarter_start_date as quarterstartdate,
        quarter_end_date as quarterenddate,
        year_number as yearnumber,
        year_start_date as yearstartdate,
        year_end_date as yearenddate

    from datebase

),

newfields as (

    select

        *,

        'Q' || quarter(current_date)::varchar as quartername,

        -- current date parts
        (dateday = current_date::date)::boolean as iscurrentdate,
        (date_part('year', dateday) = date_part('year', current_date))::boolean as iscurrentyear,
        (date_part('month', dateday) = date_part('month', current_date))::boolean as iscurrentmonth,
        (date_part('week', dateday) = date_part('week', current_date))::boolean as iscurrentweek,
        (dayofweek(current_date) < 6)::boolean as isweekday,

        -- MTD flag for date
        (dateday <= dateadd('day', -1, current_date) and dateday >= date_trunc('month', dateadd('day', -1, current_date)))::boolean as ismtd,

        -- prior MTD calcs
        (dateday <= add_months(dateadd('day', -1, current_date), -1)
        and dateday >= date_trunc('month', add_months(dateadd('day', -1, current_date), -1)))::boolean as lastmonthmtd,
        (dateday <= add_months(dateadd('day', -1, current_date), -2)
        and dateday >= date_trunc('month', add_months(dateadd('day', -1, current_date), -2)))::boolean as last2monthmtd,
        (dateday <= add_months(dateadd('day', -1, current_date), -3)
        and dateday >= date_trunc('month', add_months(dateadd('day', -1, current_date), -3)))::boolean as last3monthmtd,

        {# actual mtd calcs as of run time #}
        -- MTD flag for date
        (dateday <= current_date and dateday >= date_trunc('month', current_date))::boolean as ismtdasoftoday,

        -- prior MTD calcs
        (dateday <= add_months(current_date, -1)
        and dateday >= date_trunc('month', add_months(current_date, -1)))::boolean as lastmonthmtdismtdasoftoday,
        (dateday <= add_months(current_date, -2)
        and dateday >= date_trunc('month', add_months(current_date, -2)))::boolean as last2monthmtdismtdasoftoday,
        (dateday <= add_months(current_date, -3)
        and dateday >= date_trunc('month', add_months(current_date, -3)))::boolean as last3monthmtdismtdasoftoday,

        -- index calculations
        datediff('day', current_date, dateday)::integer as dayindexdiff,
        datediff('week', current_date, dateday)::integer as weekindexdiff,
        datediff('month', current_date, dateday)::integer as monthindexdiff,
        datediff('quarter', current_date, dateday)::integer as quarterindexdiff,
        datediff('year', current_date, dateday)::integer as yearindexdiff

    from renamed

)

select * from newfields