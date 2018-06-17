-- depends_on: {{ ref('ga_conversions') }}
{% set accounts = get_column_values(table=ref('accounts_proc'), column='bigquery_name', max_records=50, filter_column='platform', filter_value='Google Analytics') %}

{% if accounts != '' %}

with ga_report as (

	    {% for account in accounts %}

	    	{% set goals = get_column_values(table=ref('ga_conversions'), column='goal_name', max_records=50, filter_column='goal_type', filter_value='Signup', filter_column_2='bigquery_name', filter_value_2=account ) %}
	    	
		   	SELECT
		   	'{{account}}' as bigquery_name,
		   	'Google Analytics' as lookup_platform,
			lower(trim(regexp_replace(replace(replace(replace(replace(CONCAT(hostname,landingpagepath),'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),'/')) as url,
			date,
			lower(source) source,
			lower(medium) medium,
			sessions,
			## goal completion columns
			{% if goals != [] %}
				{% for goal in goals %}
					cast(goal{{goal}}completions as int64) 
					{% if not loop.last %} + {% endif %} 
					{% if loop.last %} as goal_completions, {% endif %} 
				{% endfor %}
			{% else %}				
				goalcompletionsall as goal_completions,		
			{% endif %}
			_sdc_sequence,
			first_value(_sdc_sequence) OVER (PARTITION BY hostname, landingpagepath, date, source, medium ORDER BY _sdc_sequence DESC) lv
			FROM `{{ target.project }}.ga_{{account}}.report` 

		    {% if not loop.last %} UNION ALL {% endif %}
	   {% endfor %}

)


SELECT 
bigquery_name,
lookup_platform,
date,
url,
source,
medium,
sum(sessions) sessions,
sum(goal_completions) goal_completions
FROM ga_report
where lv = _sdc_sequence
group by bigquery_name, lookup_platform, date, url, source, medium

{% endif %}
