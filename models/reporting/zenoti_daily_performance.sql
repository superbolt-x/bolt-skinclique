{{ config (
    alias = target.database + '_zenoti_daily_performance'
)}}
    
{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}

WITH initial_data as
  (SELECT *, invoice_item_created_date_and_time::date as date, {{ get_date_parts('date') }} 
  FROM {{ source('azure_raw_superbolt','sales_fact_consolidated') }} 
  WHERE _fivetran_deleted IS false),

  final_data as
  ({%- for date_granularity in date_granularity_list %}    
  SELECT 
      '{{date_granularity}}' as date_granularity,
      {{date_granularity}} as date,
      service_name, service_category, product_name, product_category,
      COUNT(DISTINCT appointment_id) as appointments_requested,
      COUNT(DISTINCT CASE WHEN appointment_status IN ('Confirmed','Checkin','Open','Closed') THEN appointment_id END) as all_appointments_booked,
      COUNT(DISTINCT CASE WHEN appointment_status IN ('Open') THEN appointment_id END) as future_appointments_booked,
      COUNT(DISTINCT CASE WHEN appointment_status IN ('Closed') AND item_name !~* 'book' THEN appointment_id END) as visits,
      COALESCE(SUM(total_adjusted_revenue),0) as total_adjusted_revenue,
      COALESCE(SUM(total_revenue),0) as total_revenue
      FROM initial_data
      WHERE center_name !~* 'training' AND service_category !~* 'perks'
      GROUP BY 1,2,3,4,5,6
      {% if not loop.last %}UNION ALL
      {% endif %}
  {% endfor %})

SELECT 
  date_granularity,
  date,
  service_name, 
  service_category, 
  product_name, 
  product_category,
  appointments_requested,
  all_appointments_booked,
  future_appointments_booked,
  visits,
  total_adjusted_revenue,
  total_revenue
FROM final_data
