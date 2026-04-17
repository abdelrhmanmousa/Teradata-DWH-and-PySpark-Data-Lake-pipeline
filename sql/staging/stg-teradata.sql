CREATE MULTISET TABLE STG_ACCOUNTS (
    acc_no      INTEGER,
    eff_date    DATE,
    acc_status  VARCHAR(20)
) PRIMARY INDEX (acc_no);

CREATE MULTISET TABLE STG_ACCOUNT_DETAILS (
    acc_no      INTEGER,
    eff_date    DATE,
    acc_type    VARCHAR(10)
) PRIMARY INDEX (acc_no);

CREATE MULTISET TABLE STG_PERSON (
    acc_no      INTEGER,
    person_id   CHAR(1)
) PRIMARY INDEX (acc_no);

CREATE MULTISET TABLE STG_PERSON_PROFILE (
    person_id   CHAR(1),
    person_name VARCHAR(100),
    eff_date    DATE
) PRIMARY INDEX (person_id);

CREATE MULTISET TABLE STG_PERSON_IDEN (
    person_id   CHAR(1),
    id_value    VARCHAR(50),
    eff_date    DATE
) PRIMARY INDEX (person_id);
