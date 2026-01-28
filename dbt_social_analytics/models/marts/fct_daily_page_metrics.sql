{{
    config(
        materialized='table',
        partition_by={
            "field": "metric_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

with page_insights as (
    select * from {{ ref('stg_page_insights') }}
),

pivoted as (
    select
        metric_date,
        max(case when metric_name = 'page_post_engagements' then metric_value end) as page_post_engagements,
        max(case when metric_name = 'page_views_total' then metric_value end) as page_views_total,
        max(case when metric_name = 'page_video_views' then metric_value end) as page_video_views,
        max(case when metric_name = 'page_actions_post_reactions_like_total' then metric_value end) as page_reactions_like,
        max(ingestion_time) as last_updated
    from page_insights
    group by metric_date
)

select
    metric_date,
    coalesce(page_post_engagements, 0) as page_post_engagements,
    coalesce(page_views_total, 0) as page_views_total,
    coalesce(page_video_views, 0) as page_video_views,
    coalesce(page_reactions_like, 0) as page_reactions_like,
    -- Calculated metrics
    coalesce(page_post_engagements, 0) + coalesce(page_reactions_like, 0) as total_engagement,
    last_updated
from pivoted
order by metric_date desc
