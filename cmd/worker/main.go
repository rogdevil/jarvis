package main

import (
	"log"

	jarvis "github.com/symbolichealth/jarvis/backend"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	c, err := client.Dial(client.Options{})
	jarvisClient := jarvis.Start()
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	w := worker.New(c, "jarvis-message-queue", worker.Options{})

	w.RegisterWorkflow(jarvis.JarvisWorkflow)
	w.RegisterActivity(jarvisClient.Chat)

	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}

}
