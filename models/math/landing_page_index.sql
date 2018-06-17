SELECT 
yyyymm, 
site,
platform,
channel, 
url,
cost,
impressions,
clicks,
conversions,
sessions,
pct_sessions,
goal_completions,
pct_goal_completions,
case when pct_sessions > 0 then pct_goal_completions / pct_sessions else null end as conversion_index
FROM (
    SELECT
    yyyymm, 
    site,
    platform,
    channel, 
    url,
    cost,
    impressions,
    clicks,
    conversions,
    sessions,
    case when total_sessions > 0 then sessions / total_sessions else null end as pct_sessions,
    goal_completions,
    case when total_goal_completions > 0 then goal_completions / total_goal_completions else null end as pct_goal_completions
    FROM {{ ref('landing_page_by_month')}}
)