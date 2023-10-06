-- this SQL copied from the Quartz library at org/quartz/impl/jdbcjobstore/tables_postgres.sql
-- and then modified by David An to:
--     * split the DROP and CREATE statements into two separate files
--     * create tables within the sys_wds schema instead of the public schema
--     * separate out the CREATE and DROP statements to allow Liquibase rollback



-- Thanks to Patrick Lightbody for submitting this...
--
-- In your Quartz properties file, you'll need to set
-- org.quartz.jobStore.driverDelegateClass = org.quartz.impl.jdbcjobstore.PostgreSQLDelegate

DROP TABLE IF EXISTS sys_wds.QRTZ_FIRED_TRIGGERS;
DROP TABLE IF EXISTS sys_wds.QRTZ_PAUSED_TRIGGER_GRPS;
DROP TABLE IF EXISTS sys_wds.QRTZ_SCHEDULER_STATE;
DROP TABLE IF EXISTS sys_wds.QRTZ_LOCKS;
DROP TABLE IF EXISTS sys_wds.QRTZ_SIMPLE_TRIGGERS;
DROP TABLE IF EXISTS sys_wds.QRTZ_CRON_TRIGGERS;
DROP TABLE IF EXISTS sys_wds.QRTZ_SIMPROP_TRIGGERS;
DROP TABLE IF EXISTS sys_wds.QRTZ_BLOB_TRIGGERS;
DROP TABLE IF EXISTS sys_wds.QRTZ_TRIGGERS;
DROP TABLE IF EXISTS sys_wds.QRTZ_JOB_DETAILS;
DROP TABLE IF EXISTS sys_wds.QRTZ_CALENDARS;

-- CREATE TABLE statements exist here in the original Quartz file; they
-- exist in 20231003_quartz_tables.sql.

COMMIT;
