SELECT 
site,
bigquery_name,
platform,
goal_name,
max(goal_type) goal_type,
account,
max(time_of_entry) time_of_entry
FROM  ( 

	SELECT  
	site,
	bigquery_name,
	platform,
	trim(replace(replace(lower(goal_name),',',''),' ','')) goal_name,
	goal_type,
	account,
	time_of_entry,
	first_value(time_of_entry) OVER (PARTITION BY platform ORDER BY time_of_entry DESC) lv
	FROM `{{ target.project }}.agency_data_pipeline.conversion_goals` 

) 

WHERE lv = time_of_entry
group by site, bigquery_name, platform, account, goal_name