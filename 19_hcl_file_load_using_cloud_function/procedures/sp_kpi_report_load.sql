CREATE OR REPLACE PROCEDURE
`rameshgcplearning.reporting.sp_kpi_report_load`()

BEGIN

-- =========================================================
-- 1. TOP 5 CUSTOMERS BY TRANSACTION VOLUME
-- =========================================================

CREATE OR REPLACE TABLE
`rameshgcplearning.reporting.top_five_customer_trans`
AS

SELECT
    c.customer_id,
    c.customer_name,
    SUM(t.amount) AS total_volume

FROM `rameshgcplearning.curated.transactions` t

JOIN `rameshgcplearning.curated.customers` c
ON t.account_id = c.account_id

WHERE UPPER(t.status) = 'SUCCESS'

GROUP BY
    c.customer_id,
    c.customer_name

ORDER BY total_volume DESC

LIMIT 5;

-- =========================================================
-- 2. MONTHLY TRANSACTION TRENDS PER BRANCH
-- =========================================================

CREATE OR REPLACE TABLE
`rameshgcplearning.reporting.monthly_trans_trend_per_branch`
AS

SELECT

    branch_id,

    FORMAT_DATE(
        '%Y-%m',
        transaction_date
    ) AS year_month,

    COUNT(*) AS total_transactions,

    SUM(amount) AS total_amount,

    UPPER(status) AS transaction_status

FROM `rameshgcplearning.curated.transactions`

GROUP BY
    branch_id,
    year_month,
    transaction_status

ORDER BY
    branch_id,
    year_month;

-- =========================================================
-- 3. PRODUCT-WISE REVENUE CONTRIBUTION
-- =========================================================

CREATE OR REPLACE TABLE
`rameshgcplearning.reporting.prod_wise_revenue`
AS

SELECT

    p.product_id,

    p.product_name,

    SUM(t.amount) AS total_revenue,

    ROUND(
        SUM(t.amount) * 100 /
        SUM(SUM(t.amount)) OVER (),
        2
    ) AS revenue_percentage

FROM `rameshgcplearning.curated.transactions` t

JOIN `rameshgcplearning.curated.products` p
ON t.product_id = p.product_id

WHERE UPPER(t.status) = 'SUCCESS'

GROUP BY
    p.product_id,
    p.product_name

ORDER BY total_revenue DESC;

-- =========================================================
-- 4. INACTIVE ACCOUNTS
-- NO TRANSACTIONS IN LAST 3 MONTHS
-- =========================================================

CREATE OR REPLACE TABLE
`rameshgcplearning.reporting.last_three_month_inact_acct`
AS

SELECT

    a.account_id,
    a.account_name

FROM `rameshgcplearning.curated.accounts` a

LEFT JOIN `rameshgcplearning.curated.transactions` t
ON a.account_id = t.account_id
AND t.transaction_date >= DATE_SUB(
                                CURRENT_DATE(),
                                INTERVAL 3 MONTH
                            )

WHERE t.account_id IS NULL;

-- =========================================================
-- 5. SUSPICIOUS TRANSACTIONS
-- AMOUNT > 100000
-- =========================================================

CREATE OR REPLACE TABLE
`rameshgcplearning.reporting.suspicious_trans`
AS

SELECT

    t.transaction_id,

    c.customer_id,

    c.customer_name,

    t.amount,

    t.transaction_date,

    t.status

FROM `rameshgcplearning.curated.transactions` t

JOIN `rameshgcplearning.curated.customers` c
ON t.account_id = c.account_id

WHERE t.amount > 100000

ORDER BY t.amount DESC;

END;