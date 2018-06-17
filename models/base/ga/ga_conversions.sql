SELECT 
site,
bigquery_name,
platform,
goal_name,
goal_type,
account,
time_of_entry
FROM {{ ref('conversion_goals_proc') }}
WHERE platform = 'Google Analytics'