{{
    config(
        materialized='table'
    )
}}

with date_spine as (
    select
        date_add(
            (select min(metric_date) from {{ ref('stg_page_insights') }}),
            interval day_offset day
        ) as date_day
    from unnest(generate_array(0, 730)) as day_offset  -- ~2 years of dates
),

filtered_dates as (
    select date_day
    from date_spine
    where date_day <= current_date()
)

select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day_of_month,
    extract(dayofweek from date_day) as day_of_week,
    extract(week from date_day) as week_of_year,
    extract(quarter from date_day) as quarter,
    
    -- Day name
    format_date('%A', date_day) as day_name,
    format_date('%B', date_day) as month_name,
    format_date('%Y-%m', date_day) as year_month,
    format_date('%Y-Q%Q', date_day) as year_quarter,
    
    -- Flags
    case when extract(dayofweek from date_day) in (1, 7) then true else false end as is_weekend,
    case when date_day = current_date() then true else false end as is_today

from filtered_dates
order by date_day
