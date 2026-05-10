CREATE OR REPLACE PROCEDURE
`rameshgcplearning.curated.sp_load_all_tables_merge_valid_invalid`()
BEGIN

-- =========================================================
-- INVALID CUSTOMERS
-- =========================================================

INSERT INTO `rameshgcplearning.curated.invalid_customers`

SELECT
    TRIM(customer_id),
    UPPER(TRIM(customer_name)),
    LOWER(TRIM(email)),
    TRIM(phone),
    TRIM(account_id),
    UPPER(TRIM(gender)),
    SAFE_CAST(dob AS DATE),
    TRIM(address),
    UPPER(TRIM(kyc_status)),
    SAFE_CAST(registration_date AS DATE),
    TRIM(transaction_id),

    CASE
        WHEN customer_id IS NULL
        THEN 'CUSTOMER_ID IS NULL'

        WHEN NOT REGEXP_CONTAINS(
                LOWER(TRIM(email)),
                r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
             )
             AND NOT REGEXP_CONTAINS(
                TRIM(phone),
                r'^(\+91[- ]?)?[6-9][0-9]{9}$'
             )
        THEN 'INVALID EMAIL AND PHONE'

        WHEN NOT REGEXP_CONTAINS(
                LOWER(TRIM(email)),
                r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
             )
        THEN 'INVALID EMAIL'

        WHEN NOT REGEXP_CONTAINS(
                TRIM(phone),
                r'^(\+91[- ]?)?[6-9][0-9]{9}$'
             )
        THEN 'INVALID PHONE'
    END,

    CURRENT_TIMESTAMP()

FROM `rameshgcplearning.staging.customers_stg`

WHERE
      customer_id IS NULL

   OR NOT REGEXP_CONTAINS(
            LOWER(TRIM(email)),
            r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
        )

   OR NOT REGEXP_CONTAINS(
            TRIM(phone),
            r'^(\+91[- ]?)?[6-9][0-9]{9}$'
        );


-- =========================================================
-- VALID CUSTOMERS
-- =========================================================

MERGE `rameshgcplearning.curated.customers`
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

            CURRENT_TIMESTAMP() AS load_date,

            ROW_NUMBER() OVER
            (
                PARTITION BY customer_id
                ORDER BY SAFE_CAST(registration_date AS DATE) DESC
            ) AS rnm

        FROM `rameshgcplearning.staging.customers_stg`

        WHERE customer_id IS NOT NULL

        AND REGEXP_CONTAINS(
                LOWER(TRIM(email)),
                r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            )

        AND REGEXP_CONTAINS(
                TRIM(phone),
                r'^(\+91[- ]?)?[6-9][0-9]{9}$'
            )
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
    CURRENT_TIMESTAMP()
);


-- =========================================================
-- INVALID ACCOUNTS
-- =========================================================

INSERT INTO `rameshgcplearning.curated.invalid_accounts`

SELECT
    TRIM(account_id),
    UPPER(TRIM(account_name)),
    LOWER(TRIM(email)),
    TRIM(phone_number),
    TRIM(address),
    SAFE_CAST(created_date AS DATE),

    CASE
        WHEN account_id IS NULL
        THEN 'ACCOUNT_ID IS NULL'

        WHEN NOT REGEXP_CONTAINS(
                LOWER(TRIM(email)),
                r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
             )
             AND NOT REGEXP_CONTAINS(
                TRIM(phone_number),
                r'^(\+91[- ]?)?[6-9][0-9]{9}$'
             )
        THEN 'INVALID EMAIL AND PHONE'

        WHEN NOT REGEXP_CONTAINS(
                LOWER(TRIM(email)),
                r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
             )
        THEN 'INVALID EMAIL'

        WHEN NOT REGEXP_CONTAINS(
                TRIM(phone_number),
                r'^(\+91[- ]?)?[6-9][0-9]{9}$'
             )
        THEN 'INVALID PHONE'
    END,

    CURRENT_TIMESTAMP()

FROM `rameshgcplearning.staging.accounts_stg`

WHERE
      account_id IS NULL

   OR NOT REGEXP_CONTAINS(
            LOWER(TRIM(email)),
            r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
        )

   OR NOT REGEXP_CONTAINS(
            TRIM(phone_number),
            r'^(\+91[- ]?)?[6-9][0-9]{9}$'
        );


-- =========================================================
-- VALID ACCOUNTS
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

            CURRENT_TIMESTAMP() AS load_date,

            ROW_NUMBER() OVER
            (
                PARTITION BY account_id
                ORDER BY SAFE_CAST(created_date AS DATE) DESC
            ) AS rnm

        FROM `rameshgcplearning.staging.accounts_stg`

        WHERE account_id IS NOT NULL

        AND REGEXP_CONTAINS(
                LOWER(TRIM(email)),
                r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            )

        AND REGEXP_CONTAINS(
                TRIM(phone_number),
                r'^(\+91[- ]?)?[6-9][0-9]{9}$'
            )
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
    CURRENT_TIMESTAMP()
);


-- =========================================================
-- INVALID BRANCHES
-- =========================================================

INSERT INTO `rameshgcplearning.curated.invalid_branches`

SELECT
    TRIM(branch_id),
    UPPER(TRIM(branch_name)),
    UPPER(TRIM(location)),
    UPPER(TRIM(manager_name)),
    SAFE_CAST(opened_date AS DATE),
    UPPER(TRIM(region)),
    UPPER(TRIM(branch_type)),
    TRIM(contact_number),

    CASE
        WHEN branch_id IS NULL
        THEN 'BRANCH_ID IS NULL'

        WHEN NOT REGEXP_CONTAINS(
                TRIM(contact_number),
                r'^(\+91[- ]?)?[6-9][0-9]{9}$'
             )
        THEN 'INVALID CONTACT NUMBER'
    END,

    CURRENT_TIMESTAMP()

FROM `rameshgcplearning.staging.branches_stg`

WHERE
      branch_id IS NULL

   OR NOT REGEXP_CONTAINS(
            TRIM(contact_number),
            r'^(\+91[- ]?)?[6-9][0-9]{9}$'
        );


-- =========================================================
-- VALID BRANCHES
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

            CURRENT_TIMESTAMP() AS load_date,

            ROW_NUMBER() OVER
            (
                PARTITION BY branch_id
                ORDER BY SAFE_CAST(opened_date AS DATE) DESC
            ) AS rnm

        FROM `rameshgcplearning.staging.branches_stg`

        WHERE branch_id IS NOT NULL

        AND REGEXP_CONTAINS(
                TRIM(contact_number),
                r'^(\+91[- ]?)?[6-9][0-9]{9}$'
            )
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
    CURRENT_TIMESTAMP()
);

END;