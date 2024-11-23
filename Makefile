migrate:
	env/bin/python db/migration_manager.py up postgres

rollback:
	env/bin/python db/migration_manager.py down postgres

migration:
	migrate create -ext sql -dir db/migrations/ -seq $(name)
