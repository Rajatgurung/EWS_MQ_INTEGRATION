
CREATE TABLE IF NOT EXISTS ews_test (
    "time" TIMESTAMP WITH TIME ZONE NOT NULL,
    "unit" VARCHAR(255) NOT NULL,
    "minid" INTEGER NOT NULL,
    "fieldName" VARCHAR(255) NOT NULL,
    "fieldValue" DECIMAL(18, 6) NOT NULL,
    "messageId" VARCHAR(255) NOT NULL
);

SELECT create_hypertable('conditions', 'time');