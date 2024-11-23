package util

import (
	"encoding/json"
	"fmt"
	"os"
)

type Secrets struct {
	Snowflake SnowflakeCredentials `json:"snowflake"`
	Database  DatabaseCredentials  `json:"database"`
}

type DatabaseCredentials struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	Database string `json:"database"`
}

type SnowflakeCredentials struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Account  string `json:"account"`
	Schema   string `json:"schema"`
	Database string `json:"database"`
}

func GetSecrets() (*Secrets, error) {
	secrets := Secrets{}
	bytes, err := os.ReadFile("app/secrets.json")
	if err != nil {
		return nil, fmt.Errorf("failed to read secrets.json: %w", err)
	}
	err = json.Unmarshal(bytes, &secrets)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal secrets.json: %w", err)
	}
	return &secrets, nil
}
