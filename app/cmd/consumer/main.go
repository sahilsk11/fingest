package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/sahilsk11/fingest/app/cmd"
	"github.com/sahilsk11/fingest/app/consumer"
	"github.com/sahilsk11/fingest/app/util"

	_ "github.com/lib/pq"
)

const (
	exitCodeErr       = 1
	exitCodeInterrupt = 2
)

func main() {
	secrets, err := util.GetSecrets()
	if err != nil {
		log.Fatal(err)
	}
	dependencies, err := cmd.InitializeDependencies()
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := consumer.NewConsumer(secrets.Kafka.Host, secrets.Kafka.Port, dependencies)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	defer func() {
		signal.Stop(signalChan)
		cancel()
	}()

	go func() {
		select {
		case <-signalChan: // first signal, cancel context
			cancel()
		case <-ctx.Done():
		}
		<-signalChan // second signal, hard exit
		os.Exit(exitCodeInterrupt)
	}()

	err = consumer.Start(ctx)
	if err != nil {
		log.Fatal(err)
	}
}
