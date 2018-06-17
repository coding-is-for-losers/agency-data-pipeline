with ga as (

	select 
		date, account, site, channel, platform, url,
		sessions, goal_completions,
		null as cost, null as impressions, null as clicks, null as conversions
	from {{ref('ga_stats')}}

),

platforms as (

	select 
		date, account, site, channel, platform, url,
		null as sessions, null as goal_completions,
		cost, impressions, clicks, conversions
	from {{ref('agg_platforms_site')}}

)


SELECT
    date, 
    site,
    platform,
    channel, 
    url,
    sum(cost) cost,
	sum(impressions) impressions,
	sum(clicks) clicks,
	sum(conversions) conversions,
	sum(sessions) sessions,
	sum(goal_completions) goal_completions
FROM
(
    SELECT *
    FROM 
      ga
    UNION ALL
    SELECT *
    FROM 
      platforms 
) 
GROUP BY
    date, site, platform, channel, url