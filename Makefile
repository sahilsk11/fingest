db-models:
	jet -dsn=postgresql://postgres:postgres@localhost:5441/postgres?sslmode=disable -path=./app/db_models
	# tools/env/bin/python tools/db_model_helper.py

migration:
	migrate create -ext sql -dir db/migrations/ -seq $(name)

migrate:
	env/bin/python db/migration_manager.py up postgres
	make db-models;


rollback:
	env/bin/python db/migration_manager.py down postgres