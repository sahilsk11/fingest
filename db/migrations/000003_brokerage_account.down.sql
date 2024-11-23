drop view current_brokerage_account_open_lot;

drop table brokerage_account_open_lot_version;
drop table brokerage_account_open_lot;
-- Drop the tables created in the up migration
DROP TABLE IF EXISTS brokerage_account_transaction_import_run;
DROP TABLE IF EXISTS brokerage_account_transaction;
DROP TABLE IF EXISTS brokerage_account;

-- Drop the enum types created in the up migration
DROP TYPE IF EXISTS side;
-- DROP TYPE IF EXISTS brokerage_account_type; -- Uncomment if you want to drop this type as well
