SELECT
day,
a.campaign,
a.campaignid, 
account, 
channel,
platform,
b.url,
cost,
impressions,
clicks,
conversions
FROM {{ref('adwords_campaigns')}} a
LEFT JOIN {{ref('adwords_urls')}} b
ON a.campaignid = b.campaignid
