package main

import (
	"log"

	"github.com/google/uuid"
	"github.com/sahilsk11/fingest/app/repository"
	"github.com/sahilsk11/fingest/app/service"
	"github.com/sahilsk11/fingest/app/util"

	_ "github.com/lib/pq"
)

func main() {
	secrets, err := util.GetSecrets()
	if err != nil {
		log.Fatal(err)
	}

	db, err := util.NewConnection(secrets.Database)
	if err != nil {
		log.Fatal(err)
	}

	snowflakeRepository, err := repository.NewSnowflakeRepository(repository.SnowflakeCredentials{
		User:     secrets.Snowflake.User,
		Password: secrets.Snowflake.Password,
		Account:  secrets.Snowflake.Account,
		Schema:   secrets.Snowflake.Schema,
		Database: secrets.Snowflake.Database,
	})
	if err != nil {
		log.Fatal(err)
	}

	importRunStateRepository := repository.NewImportRunStateRepository(db)

	service := service.NewImportRunService(snowflakeRepository, importRunStateRepository)
	err = service.ImportFromSnowflake(uuid.MustParse("e0e7f86d-0bfb-43dd-b90d-7687222afcef"))
	if err != nil {
		log.Fatal(err)
	}
}
