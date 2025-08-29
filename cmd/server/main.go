package main

import (
	"log"

	jarvis "github.com/symbolichealth/jarvis/backend"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func initTemporalWorker() {
	c, err := client.Dial(client.Options{
		HostPort: "localhost:6233",
	})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	jarvisClient := jarvis.Start()

	// First worker for jarvis workflow
	w := worker.New(c, "jarvis-message-queue", worker.Options{})
	w.RegisterWorkflow(jarvis.JarvisWorkflow)
	w.RegisterActivity(jarvisClient.Chat)

	// Second worker for process workflow
	processWorker := worker.New(c, "process-message-queue", worker.Options{})
	registerProcessWorkflowOption := workflow.RegisterOptions{
		Name: "ProcessChatMessageWorkflow",
	}
	processWorker.RegisterWorkflowWithOptions(jarvis.ProcessChatMessageWorkflow, registerProcessWorkflowOption)
	processWorker.RegisterActivityWithOptions(jarvisClient.GetChatLengthActivity, activity.RegisterOptions{
		Name: "GetChatLengthActivity",
	})

	// Start both workers concurrently
	go func() {
		err := w.Run(worker.InterruptCh())
		if err != nil {
			log.Fatalln("Unable to start jarvis worker", err)
		}
	}()

	go func() {
		err := processWorker.Run(worker.InterruptCh())
		if err != nil {
			log.Fatalln("Unable to start process worker", err)
		}
	}()

	// Keep the function running
	select {}
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
