SELECT 
date, 
a.account account, 
b.site site,
a.platform platform,
a.channel channel,
url,
cost,
impressions,
clicks,
conversions
FROM 
  {{ref('agg_platforms_union')}} a
LEFT JOIN 
  {{ref('accounts_proc')}} b
ON ( 
  a.account = b.account AND
  a.platform = b.platform
)