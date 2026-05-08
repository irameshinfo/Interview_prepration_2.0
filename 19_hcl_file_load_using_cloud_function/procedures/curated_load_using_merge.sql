CREATE OR REPLACE PROCEDURE
`rameshgcplearning.curated.sp_load_all_tables_merge`()
BEGIN

-- =========================================================
-- CUSTOMERS
-- =========================================================

MERGE `rameshgcplearning.curated.customers` T
USING
(
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

            CASE
                WHEN REGEXP_CONTAINS(
                        LOWER(TRIM(email)),
                        r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
                     )
                THEN 'VALID'
                ELSE 'INVALID'
            END AS email_status,

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
                ORDER BY SAFE_CAST(registration_date AS DATE) DESC
            ) AS rnm

        FROM `rameshgcplearning.staging.customers_stg`

        WHERE customer_id IS NOT NULL
    )
    WHERE rnm = 1
) S

ON T.customer_id = S.customer_id

WHEN MATCHED THEN
UPDATE SET

    T.customer_name = S.customer_name,
    T.email = S.email,
    T.phone = S.phone,
    T.account_id = S.account_id,
    T.gender = S.gender,
    T.dob = S.dob,
    T.address = S.address,
    T.kyc_status = S.kyc_status,
    T.registration_date = S.registration_date,
    T.transaction_id = S.transaction_id,
    T.email_status = S.email_status,
    T.mobile_status = S.mobile_status,
    T.load_date = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT
(
    customer_id,
    customer_name,
    email,
    phone,
    account_id,
    gender,
    dob,
    address,
    kyc_status,
    registration_date,
    transaction_id,
    email_status,
    mobile_status,
    load_date
)
VALUES
(
    S.customer_id,
    S.customer_name,
    S.email,
    S.phone,
    S.account_id,
    S.gender,
    S.dob,
    S.address,
    S.kyc_status,
    S.registration_date,
    S.transaction_id,
    S.email_status,
    S.mobile_status,
    CURRENT_TIMESTAMP()
);

-- =========================================================
-- ACCOUNTS
-- =========================================================

MERGE `rameshgcplearning.curated.accounts` T
USING
(
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

            CASE
                WHEN REGEXP_CONTAINS(
                        LOWER(TRIM(email)),
                        r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
                     )
                THEN 'VALID'
                ELSE 'INVALID'
            END AS email_status,

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
                ORDER BY SAFE_CAST(created_date AS DATE) DESC
            ) AS rnm

        FROM `rameshgcplearning.staging.accounts_stg`

        WHERE account_id IS NOT NULL
    )
    WHERE rnm = 1
) S

ON T.account_id = S.account_id

WHEN MATCHED THEN
UPDATE SET

    T.account_name = S.account_name,
    T.email = S.email,
    T.phone_number = S.phone_number,
    T.address = S.address,
    T.created_date = S.created_date,
    T.email_status = S.email_status,
    T.phonenumber_status = S.phonenumber_status,
    T.load_date = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT
(
    account_id,
    account_name,
    email,
    phone_number,
    address,
    created_date,
    email_status,
    phonenumber_status,
    load_date
)
VALUES
(
    S.account_id,
    S.account_name,
    S.email,
    S.phone_number,
    S.address,
    S.created_date,
    S.email_status,
    S.phonenumber_status,
    CURRENT_TIMESTAMP()
);

-- =========================================================
-- BRANCHES
-- =========================================================

MERGE `rameshgcplearning.curated.branches` T
USING
(
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
                ORDER BY SAFE_CAST(opened_date AS DATE) DESC
            ) AS rnm

        FROM `rameshgcplearning.staging.branches_stg`

        WHERE branch_id IS NOT NULL
    )
    WHERE rnm = 1
) S

ON T.branch_id = S.branch_id

WHEN MATCHED THEN
UPDATE SET

    T.branch_name = S.branch_name,
    T.location = S.location,
    T.manager_name = S.manager_name,
    T.opened_date = S.opened_date,
    T.region = S.region,
    T.branch_type = S.branch_type,
    T.contact_number = S.contact_number,
    T.contact_status = S.contact_status,
    T.load_date = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT
(
    branch_id,
    branch_name,
    location,
    manager_name,
    opened_date,
    region,
    branch_type,
    contact_number,
    contact_status,
    load_date
)
VALUES
(
    S.branch_id,
    S.branch_name,
    S.location,
    S.manager_name,
    S.opened_date,
    S.region,
    S.branch_type,
    S.contact_number,
    S.contact_status,
    CURRENT_TIMESTAMP()
);

-- =========================================================
-- PRODUCTS
-- =========================================================

MERGE `rameshgcplearning.curated.products` T
USING
(
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
                ORDER BY SAFE_CAST(launch_date AS DATE) DESC
            ) AS rnm

        FROM `rameshgcplearning.staging.products_stg`

        WHERE product_id IS NOT NULL
    )
    WHERE rnm = 1
) S

ON T.product_id = S.product_id

WHEN MATCHED THEN
UPDATE SET

    T.product_name = S.product_name,
    T.product_type = S.product_type,
    T.price = S.price,
    T.launch_date = S.launch_date,
    T.is_active = S.is_active,
    T.category = S.category,
    T.vendor_name = S.vendor_name,
    T.load_date = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT
(
    product_id,
    product_name,
    product_type,
    price,
    launch_date,
    is_active,
    category,
    vendor_name,
    load_date
)
VALUES
(
    S.product_id,
    S.product_name,
    S.product_type,
    S.price,
    S.launch_date,
    S.is_active,
    S.category,
    S.vendor_name,
    CURRENT_TIMESTAMP()
);

-- =========================================================
-- TRANSACTIONS
-- =========================================================

MERGE `rameshgcplearning.curated.transactions` T
USING
(
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
    WHERE rnm = 1
) S

ON T.transaction_id = S.transaction_id

WHEN MATCHED THEN
UPDATE SET

    T.account_id = S.account_id,
    T.product_id = S.product_id,
    T.branch_id = S.branch_id,
    T.transaction_date = S.transaction_date,
    T.transaction_timestamp = S.transaction_timestamp,
    T.amount = S.amount,
    T.payment_method = S.payment_method,
    T.status = S.status,
    T.currency = S.currency,
    T.remarks = S.remarks,
    T.is_refund = S.is_refund,
    T.load_date = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT
(
    transaction_id,
    account_id,
    product_id,
    branch_id,
    transaction_date,
    transaction_timestamp,
    amount,
    payment_method,
    status,
    currency,
    remarks,
    is_refund,
    load_date
)
VALUES
(
    S.transaction_id,
    S.account_id,
    S.product_id,
    S.branch_id,
    S.transaction_date,
    S.transaction_timestamp,
    S.amount,
    S.payment_method,
    S.status,
    S.currency,
    S.remarks,
    S.is_refund,
    CURRENT_TIMESTAMP()
);

END;