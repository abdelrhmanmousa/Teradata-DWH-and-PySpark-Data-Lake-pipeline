CREATE TABLE Person (
    person_id VARCHAR(10) PRIMARY KEY
);

CREATE TABLE Account (
    account_id INT PRIMARY KEY
);


CREATE TABLE Person_Account (
    person_id VARCHAR(10),
    account_id INT,

    PRIMARY KEY (person_id, account_id),

    FOREIGN KEY (person_id) REFERENCES Person(person_id),
    FOREIGN KEY (account_id) REFERENCES Account(account_id)
);


CREATE TABLE Person_Profile (
    profile_id INT IDENTITY(1,1) PRIMARY KEY,
    person_id VARCHAR(10),
    name VARCHAR(100),
    start_date DATE,
    end_date DATE NULL,

    FOREIGN KEY (person_id) REFERENCES Person(person_id)
);

CREATE TABLE Person_Iden (
    iden_id INT IDENTITY(1,1) PRIMARY KEY,
    person_id VARCHAR(10),
    id_value VARCHAR(50),
    id_type VARCHAR(20),
    start_date DATE,

    FOREIGN KEY (person_id) REFERENCES Person(person_id)
);

CREATE TABLE Account_Details (
    details_id INT IDENTITY(1,1) PRIMARY KEY,
    account_id INT,
    record_date DATE,
    account_type VARCHAR(20),
    status VARCHAR(20),

    FOREIGN KEY (account_id) REFERENCES Account(account_id)
);

CREATE INDEX idx_person_account_person
ON Person_Account(person_id);

CREATE INDEX idx_person_profile_person
ON Person_Profile(person_id);

CREATE INDEX idx_account_details_account
ON Account_Details(account_id);

ALTER TABLE Person_Iden
ADD CONSTRAINT uq_person_id UNIQUE (person_id, id_value);

ALTER TABLE Person_Profile
ADD is_current BIT DEFAULT 1;