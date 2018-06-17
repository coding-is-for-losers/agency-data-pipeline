 -- depends_on: {{ ref('fb_conversions') }}

{% set accounts = get_column_values(table=ref('accounts_proc'), column='bigquery_name', max_records=50, filter_column='platform', filter_value='FB Ads') %}

{% if accounts != [] %}

with fb_ads_insights_conversions as (

	    {% for account in accounts %}

	    	{% set goals = get_column_values(table=ref('fb_conversions'), column='goal_name', max_records=50, filter_column='goal_type', filter_value='Signup', filter_column_2='bigquery_name', filter_value_2=account ) %}

		   	SELECT 
			date_start,
			campaign_id,
			campaign_name,
			ad_id,
			account_name,
			_sdc_sequence,
			action_type,
			_28d_click conversions,
			null as revenue
			FROM `{{ target.project }}.fb_ads_{{account}}.ads_insights`
			cross join unnest(actions)

			## goal completion columns
			{% if goals != [] %}
				where action_type in (
				{% for goal in goals %}
					'{{goal}}'
					{% if not loop.last %} , {% endif %} 
				{% endfor %}
				)
			{% endif %}
			
			UNION ALL

		   	SELECT 
			date_start,
			campaign_id,
			campaign_name,
			ad_id,
			account_name,
			_sdc_sequence,
			action_type,
			null as conversions,
			_28d_click as revenue
			FROM `{{ target.project }}.fb_ads_{{account}}.ads_insights`
			cross join unnest(action_values)
			
			## goal completion columns
			{% if goals != [] %}
				where action_type in (
				{% for goal in goals %}
					'{{goal}}'
					{% if not loop.last %} , {% endif %} 
				{% endfor %}
				)
			{% endif %}

		    {% if not loop.last %} UNION ALL {% endif %}
	   {% endfor %}

)

SELECT 
date,
campaign_id,
campaign,
ad_id,
account,
sum(conversions) conversions,
sum(revenue) revenue
FROM 
		(
	    SELECT
	    date_start date,
	    campaign_id,
	    campaign_name campaign,
	    ad_id,
	    account_name account,
	    action_type,
	    conversions,
	    revenue,
	    first_value(_sdc_sequence) OVER (PARTITION BY ad_id, date_start ORDER BY _sdc_sequence DESC) lv,
	    _sdc_sequence
	    FROM fb_ads_insights_conversions
	   	)
where lv = _sdc_sequence
group by date, campaign_id, campaign, ad_id, account


{% endif %}
