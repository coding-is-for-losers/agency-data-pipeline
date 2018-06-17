SELECT 
cast(day as date) date, 
account, 
platform,
channel,
url,
campaign,
sum(cost) cost,
sum(impressions) impressions,
sum(clicks) clicks,
sum(conversions) conversions
FROM {{ref('adwords_join')}}
GROUP BY date, account, platform, channel, url, campaign