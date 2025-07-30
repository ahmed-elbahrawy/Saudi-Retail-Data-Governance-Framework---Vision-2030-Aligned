-- models/marts/customer_loyalty_kpis.sql
-- Customer Loyalty KPIs aligned with Saudi Vision 2030 retail transformation goals

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['customer_id', 'calculation_date'], 'unique': true},
        {'columns': ['loyalty_tier']},
        {'columns': ['region']}
    ],
    tags=['customer_analytics', 'loyalty', 'vision_2030'],
    meta={
        'owner': 'data_analytics_team',
        'description': 'Customer loyalty metrics supporting Vision 2030 retail sector transformation',
        'business_rules': [
            'VIP customers: >50K SAR annual spend',
            'Gold customers: 20K-50K SAR annual spend', 
            'Silver customers: 5K-20K SAR annual spend',
            'Bronze customers: <5K SAR annual spend'
        ]
    }
) }}

WITH customer_transactions AS (
    SELECT 
        customer_id,
        transaction_date,
        total_amount,
        loyalty_points_earned,
        store_id,
        region,
        payment_method
    FROM {{ ref('fct_transactions') }}
    WHERE transaction_date >= DATEADD('year', -2, CURRENT_DATE())
),

customer_purchase_behavior AS (
    SELECT 
        customer_id,
        region,
        COUNT(DISTINCT transaction_date) as transaction_frequency,
        COUNT(DISTINCT DATE_TRUNC('month', transaction_date)) as active_months,
        SUM(total_amount) as total_spent_2yr,
        SUM(CASE WHEN transaction_date >= DATEADD('year', -1, CURRENT_DATE()) 
                 THEN total_amount ELSE 0 END) as total_spent_1yr,
        SUM(loyalty_points_earned) as total_loyalty_points,
        AVG(total_amount) as avg_transaction_value,
        MAX(transaction_date) as last_purchase_date,
        MIN(transaction_date) as first_purchase_date,
        COUNT(DISTINCT store_id) as stores_visited,
        COUNT(DISTINCT payment_method) as payment_methods_used,
        -- Vision 2030 specific metrics
        SUM(CASE WHEN payment_method IN ('MADA', 'DIGITAL_WALLET', 'CONTACTLESS') 
                 THEN total_amount ELSE 0 END) as digital_payment_amount,
        COUNT(CASE WHEN payment_method IN ('MADA', 'DIGITAL_WALLET', 'CONTACTLESS') 
                   THEN 1 END) as digital_payment_transactions
    FROM customer_transactions
    GROUP BY customer_id, region
),

customer_demographics AS (
    SELECT 
        customer_id,
        age_group,
        gender,
        nationality,
        city,
        registration_date,
        preferred_language
    FROM {{ ref('dim_customers') }}
),

loyalty_calculations AS (
    SELECT 
        cpb.*,
        cd.age_group,
        cd.gender,
        cd.nationality,
        cd.city,
        cd.registration_date,
        cd.preferred_language,
        
        -- Recency (days since last purchase)
        DATEDIFF('day', cpb.last_purchase_date, CURRENT_DATE()) as recency_days,
        
        -- Frequency score (normalized)
        CASE 
            WHEN cpb.transaction_frequency >= 100 THEN 5
            WHEN cpb.transaction_frequency >= 50 THEN 4
            WHEN cpb.transaction_frequency >= 20 THEN 3
            WHEN cpb.transaction_frequency >= 10 THEN 2
            ELSE 1
        END as frequency_score,
        
        -- Monetary score (based on 1-year spending)
        CASE 
            WHEN cpb.total_spent_1yr >= 50000 THEN 5
            WHEN cpb.total_spent_1yr >= 20000 THEN 4
            WHEN cpb.total_spent_1yr >= 5000 THEN 3
            WHEN cpb.total_spent_1yr >= 1000 THEN 2
            ELSE 1
        END as monetary_score,
        
        -- Customer lifetime value estimation
        (cpb.total_spent_2yr / NULLIF(DATEDIFF('month', cpb.first_purchase_date, CURRENT_DATE()), 0)) * 12 as estimated_annual_clv,
        
        -- Loyalty tier based on annual spending
        CASE 
            WHEN cpb.total_spent_1yr >= 50000 THEN 'VIP'
            WHEN cpb.total_spent_1yr >= 20000 THEN 'GOLD'
            WHEN cpb.total_spent_1yr >= 5000 THEN 'SILVER'
            WHEN cpb.total_spent_1yr >= 1000 THEN 'BRONZE'
            ELSE 'BASIC'
        END as loyalty_tier,
        
        -- Churn risk assessment
        CASE 
            WHEN DATEDIFF('day', cpb.last_purchase_date, CURRENT_DATE()) > 180 THEN 'HIGH_RISK'
            WHEN DATEDIFF('day', cpb.last_purchase_date, CURRENT_DATE()) > 90 THEN 'MEDIUM_RISK'
            WHEN DATEDIFF('day', cpb.last_purchase_date, CURRENT_DATE()) > 30 THEN 'LOW_RISK'
            ELSE 'ACTIVE'
        END as churn_risk_category,
        
        -- Digital adoption score (Vision 2030 KPI)
        ROUND(
            (cpb.digital_payment_amount / NULLIF(cpb.total_spent_1yr, 0)) * 100, 2
        ) as digital_adoption_percentage,
        
        -- Multi-channel engagement
        CASE 
            WHEN cpb.stores_visited >= 5 THEN 'OMNICHANNEL'
            WHEN cpb.stores_visited >= 3 THEN 'MULTI_STORE'
            WHEN cpb.stores_visited = 2 THEN 'TWO_STORES'
            ELSE 'SINGLE_STORE'
        END as channel_engagement_level
        
    FROM customer_purchase_behavior cpb
    LEFT JOIN customer_demographics cd ON cpb.customer_id = cd.customer_id
),

final_kpis AS (
    SELECT 
        customer_id,
        region,
        city,
        age_group,
        gender,
        nationality,
        preferred_language,
        loyalty_tier,
        churn_risk_category,
        channel_engagement_level,
        
        -- Core loyalty metrics
        transaction_frequency,
        active_months,
        total_spent_1yr,
        total_spent_2yr,
        avg_transaction_value,
        total_loyalty_points,
        estimated_annual_clv,
        recency_days,
        frequency_score,
        monetary_score,
        
        -- Composite loyalty score (RFM)
        (frequency_score + monetary_score + 
         CASE 
             WHEN recency_days <= 30 THEN 5
             WHEN recency_days <= 60 THEN 4
             WHEN recency_days <= 90 THEN 3
             WHEN recency_days <= 180 THEN 2
             ELSE 1
         END
        ) as loyalty_score,
        
        -- Vision 2030 specific KPIs
        digital_adoption_percentage,
        stores_visited,
        payment_methods_used,
        
        -- Customer segmentation
        CASE 
            WHEN loyalty_tier IN ('VIP', 'GOLD') AND churn_risk_category = 'ACTIVE' THEN 'CHAMPIONS'
            WHEN loyalty_tier IN ('VIP', 'GOLD') AND churn_risk_category IN ('LOW_RISK', 'MEDIUM_RISK') THEN 'LOYAL_CUSTOMERS'
            WHEN monetary_score >= 4 AND frequency_score <= 2 THEN 'BIG_SPENDERS'
            WHEN frequency_score >= 4 AND monetary_score <= 2 THEN 'FREQUENT_BUYERS'
            WHEN recency_days <= 30 AND monetary_score >= 3 THEN 'NEW_CUSTOMERS'
            WHEN churn_risk_category = 'HIGH_RISK' AND loyalty_tier IN ('GOLD', 'SILVER') THEN 'AT_RISK'
            WHEN churn_risk_category = 'HIGH_RISK' THEN 'CANT_LOSE_THEM'
            ELSE 'OTHERS'
        END as customer_segment,
        
        -- Data governance fields
        CURRENT_DATE() as calculation_date,
        '{{ var("dbt_version") }}' as dbt_model_version,
        '{{ run_started_at }}' as pipeline_execution_timestamp
        
    FROM loyalty_calculations
)

SELECT * FROM final_kpis

-- Data quality tests
{{ dbt_utils.test_not_null(['customer_id', 'loyalty_tier', 'customer_segment']) }}
{{ dbt_utils.test_unique_combination_of_columns(['customer_id', 'calculation_date']) }}
{{ dbt_utils.test_accepted_values('loyalty_tier', ['VIP', 'GOLD', 'SILVER', 'BRONZE', 'BASIC']) }}
{{ dbt_utils.test_accepted_values('churn_risk_category', ['ACTIVE', 'LOW_RISK', 'MEDIUM_RISK', 'HIGH_RISK']) }}