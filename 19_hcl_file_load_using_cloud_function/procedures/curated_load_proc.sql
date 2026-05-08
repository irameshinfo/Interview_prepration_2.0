CREATE OR REPLACE PROCEDURE
`rameshgcplearning.curated.sp_load_all_tables`()
BEGIN

-- =========================================================
-- CUSTOMERS
-- =========================================================

CREATE OR REPLACE TABLE `rameshgcplearning.curated.customers`
AS

SELECT * EXCEPT(rnm)

FROM
(
    SELECT

        TRIM(customer_id) AS customer_id,

        UPPER(TRIM(customer_name)) AS customer_name,

        LOWER(TRIM(email)) AS email,

        TRIM(phone) AS phone,

        TRIM(account_id) AS account_id,

        UPPER(TRIM(gender)) AS gender,

        SAFE_CAST(dob AS DATE) AS dob,

        TRIM(address) AS address,

        UPPER(TRIM(kyc_status)) AS kyc_status,

        SAFE_CAST(registration_date AS DATE) AS registration_date,

        TRIM(transaction_id) AS transaction_id,

        -- EMAIL VALIDATION
        CASE
            WHEN REGEXP_CONTAINS(
                    LOWER(TRIM(email)),
                    r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
                 )
            THEN 'VALID'
            ELSE 'INVALID'
        END AS email_status,

        -- MOBILE VALIDATION
        CASE
            WHEN REGEXP_CONTAINS(
                    TRIM(phone),
                    r'^(\+91[- ]?)?[6-9][0-9]{9}$'
                 )
            THEN 'VALID'
            ELSE 'INVALID'
        END AS mobile_status,

        CURRENT_TIMESTAMP() AS load_date,

        ROW_NUMBER() OVER
        (
            PARTITION BY customer_id
            ORDER BY registration_date DESC
        ) AS rnm

    FROM `rameshgcplearning.staging.customers_stg`

    WHERE customer_id IS NOT NULL
)

WHERE rnm = 1;

-- =========================================================
-- ACCOUNTS
-- =========================================================

CREATE OR REPLACE TABLE `rameshgcplearning.curated.accounts`
AS

SELECT * EXCEPT(rnm)

FROM
(
    SELECT

        TRIM(account_id) AS account_id,

        UPPER(TRIM(account_name)) AS account_name,

        LOWER(TRIM(email)) AS email,

        TRIM(phone_number) AS phone_number,

        TRIM(address) AS address,

        SAFE_CAST(created_date AS DATE) AS created_date,

        -- EMAIL VALIDATION
        CASE
            WHEN REGEXP_CONTAINS(
                    LOWER(TRIM(email)),
                    r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
                 )
            THEN 'VALID'
            ELSE 'INVALID'
        END AS email_status,

        -- MOBILE VALIDATION
        CASE
            WHEN REGEXP_CONTAINS(
                    TRIM(phone_number),
                    r'^(\+91[- ]?)?[6-9][0-9]{9}$'
                 )
            THEN 'VALID'
            ELSE 'INVALID'
        END AS phonenumber_status,

        CURRENT_TIMESTAMP() AS load_date,

        ROW_NUMBER() OVER
        (
            PARTITION BY account_id
            ORDER BY created_date DESC
        ) AS rnm

    FROM `rameshgcplearning.staging.accounts_stg`

    WHERE account_id IS NOT NULL
)

WHERE rnm = 1;

-- =========================================================
-- BRANCHES
-- =========================================================

CREATE OR REPLACE TABLE `rameshgcplearning.curated.branches`
AS

SELECT * EXCEPT(rnm)

FROM
(
    SELECT

        TRIM(branch_id) AS branch_id,

        UPPER(TRIM(branch_name)) AS branch_name,

        UPPER(TRIM(location)) AS location,

        UPPER(TRIM(manager_name)) AS manager_name,

        SAFE_CAST(opened_date AS DATE) AS opened_date,

        UPPER(TRIM(region)) AS region,

        UPPER(TRIM(branch_type)) AS branch_type,

        TRIM(contact_number) AS contact_number,

        -- CONTACT VALIDATION
        CASE
            WHEN REGEXP_CONTAINS(
                    TRIM(contact_number),
                    r'^(\+91[- ]?)?[6-9][0-9]{9}$'
                 )
            THEN 'VALID'
            ELSE 'INVALID'
        END AS contact_status,

        CURRENT_TIMESTAMP() AS load_date,

        ROW_NUMBER() OVER
        (
            PARTITION BY branch_id
            ORDER BY opened_date DESC
        ) AS rnm

    FROM `rameshgcplearning.staging.branches_stg`

    WHERE branch_id IS NOT NULL
)

WHERE rnm = 1;

-- =========================================================
-- PRODUCTS
-- =========================================================

CREATE OR REPLACE TABLE `rameshgcplearning.curated.products`
AS

SELECT * EXCEPT(rnm)

FROM
(
    SELECT

        TRIM(product_id) AS product_id,

        UPPER(TRIM(product_name)) AS product_name,

        UPPER(TRIM(product_type)) AS product_type,

        SAFE_CAST(price AS FLOAT64) AS price,

        SAFE_CAST(launch_date AS DATE) AS launch_date,

        is_active AS is_active,

        UPPER(TRIM(category)) AS category,

        UPPER(TRIM(vendor_name)) AS vendor_name,

        CURRENT_TIMESTAMP() AS load_date,

        ROW_NUMBER() OVER
        (
            PARTITION BY product_id
            ORDER BY launch_date DESC
        ) AS rnm

    FROM `rameshgcplearning.staging.products_stg`

    WHERE product_id IS NOT NULL
)

WHERE rnm = 1;

-- =========================================================
-- TRANSACTIONS
-- =========================================================

CREATE OR REPLACE TABLE `rameshgcplearning.curated.transactions`
AS

SELECT * EXCEPT(rnm)

FROM
(
    SELECT

        TRIM(transaction_id) AS transaction_id,

        TRIM(account_id) AS account_id,

        TRIM(product_id) AS product_id,

        TRIM(branch_id) AS branch_id,

        SAFE_CAST(date AS DATE) AS transaction_date,

        SAFE_CAST(timestamp AS TIMESTAMP) AS transaction_timestamp,

        SAFE_CAST(amount AS FLOAT64) AS amount,

        UPPER(TRIM(payment_method)) AS payment_method,

        UPPER(TRIM(status)) AS status,

        UPPER(TRIM(currency)) AS currency,

        TRIM(remarks) AS remarks,

        is_refund AS is_refund,

        CURRENT_TIMESTAMP() AS load_date,

        ROW_NUMBER() OVER
        (
            PARTITION BY transaction_id
            ORDER BY SAFE_CAST(timestamp AS TIMESTAMP) DESC
        ) AS rnm


    FROM `rameshgcplearning.staging.transactions_stg`

    WHERE transaction_id IS NOT NULL
)

WHERE rnm = 1;

END;