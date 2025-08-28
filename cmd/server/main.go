package main

import (
	"log"

	jarvis "github.com/symbolichealth/jarvis/backend"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func initTemporalWorker() {
	c, err := client.Dial(client.Options{
		HostPort: "localhost:6233",
	})
	jarvisClient := jarvis.Start()
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	w := worker.New(c, "jarvis-message-queue", worker.Options{})

	w.RegisterWorkflow(jarvis.JarvisWorkflow)
	w.RegisterWorkflow(jarvis.ProcessChatMessageWorkflow)
	w.RegisterActivity(jarvisClient.Chat)
	w.RegisterActivity(jarvisClient.GetChatLengthActivity)

	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}

}

func main() {
	log.Println("Starting Jarvis web server...")

	// go routine for temporal worker server
	go initTemporalWorker()

	server := jarvis.NewServer()
	if err := server.StartServer("7070"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
