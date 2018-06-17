SELECT
site,
account,
bigquery_name,
source,
medium,
max(platform_n) platform,
max(channel_n) channel,
time_of_entry
FROM  ( 

	SELECT  
	site,
	account,
	bigquery_name,
	source,
	medium,
	platform as platform_n,
	channel as channel_n,
	time_of_entry,
	first_value(time_of_entry) OVER (PARTITION BY site ORDER BY time_of_entry DESC) lv
	FROM `{{ target.project }}.agency_data_pipeline.mappings_ga` 

) 

WHERE lv = time_of_entry
group by site, account, bigquery_name, source, medium, platform_n, channel_n, time_of_entry