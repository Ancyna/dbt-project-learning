WITH stg_jaffle_shop__orders AS (
  SELECT
    *
  FROM {{ ref('dbt_fundamental_analytics', 'stg_jaffle_shop__orders') }}
), stg_stripe__payments AS (
  SELECT
    *
  FROM {{ ref('dbt_fundamental_analytics', 'stg_stripe__payments') }}
), rename_1 AS (
  SELECT
    *
    RENAME (STATUS AS ORDER_STATUS)
  FROM stg_jaffle_shop__orders
), rename_2 AS (
  SELECT
    *
    RENAME (STATUS AS PAYMENT_STATUS, AMOUNT AS PAYMENT_AMOUNT)
  FROM stg_stripe__payments
), join_1 AS (
  SELECT
    *
  FROM rename_2
  JOIN rename_1
    USING (ORDER_ID)
), aggregate_1 AS (
  SELECT
    PAYMENT_METHOD,
    PAYMENT_STATUS,
    SUM(PAYMENT_AMOUNT) AS SUM_PAYMENT_AMOUNT_IN_DOLLARS
  FROM join_1
  GROUP BY
    PAYMENT_METHOD,
    PAYMENT_STATUS
), rename_3 AS (
  SELECT
    PAYMENT_METHOD,
    PAYMENT_STATUS,
    SUM_PAYMENT_AMOUNT_IN_DOLLARS
  FROM aggregate_1
), filter_1 AS (
  SELECT
    *
  FROM rename_3
  WHERE
    PAYMENT_METHOD = 'credit_card'
), formula_1 AS (
  SELECT
    *,
    CASE
      WHEN PAYMENT_STATUS = 'success'
      THEN SUM_PAYMENT_AMOUNT_IN_DOLLARS * 0.01
      WHEN PAYMENT_STATUS = 'fail'
      THEN SUM_PAYMENT_AMOUNT_IN_DOLLARS * 0.05
      ELSE 0
    END AS FEE_AMOUNT,
    SUM_PAYMENT_AMOUNT_IN_DOLLARS - FEE_AMOUNT AS net_profit
  FROM filter_1
), credit_card_fees_sql AS (
  SELECT
    PAYMENT_METHOD,
    PAYMENT_STATUS,
    SUM_PAYMENT_AMOUNT_IN_DOLLARS,
    FEE_AMOUNT
  FROM formula_1
)
SELECT
  *
FROM credit_card_fees_sql