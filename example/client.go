package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"gitlab.wildberries.ru/portals/portal-core/tools/election.git/pkg/clients/election"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	// Define a group name for the election
	groupName := "example-service"

	// Define member name (optional)
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("Failed to get hostname: %v", err)
		return
	}

	// Create leader election callbacks
	callbacks := election.LeaderCallbacks{
		OnImLeader: func(event election.LeaderEvent) {
			log.Printf("🎉 I am now the leader of group '%s'", event.Group)
		},
		OnImNotLeader: func(event election.LeaderEvent) {
			log.Printf("👋 I am no longer the leader of group '%s'", event.Group)
		},
		OnLeaderChanged: func(event election.LeaderEvent) {
			log.Printf("ℹ️ Leader changed for group '%s': new leader is '%s'",
				event.Group, event.Leader)
		},
	}

	// Create a new client with various configuration options
	client, err := election.NewClient(
		groupName,
		election.WithMember(hostname),
		election.WithRetryInterval(10*time.Second),
		election.WithElectionTimeout(5*time.Second),
		election.WithCallbacks(callbacks),
	)
	if err != nil {
		log.Printf("Failed to create election client: %v", err)
		return
	}

	// Start the client
	if err := client.Start(); err != nil {
		log.Printf("Failed to start election client: %v", err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				state, err := client.GetGroupState(groupName)
				if err != nil {
					log.Printf("Failed to get group state: %v", err)
					continue
				}

				fmt.Println("\n--- Group State ---")
				fmt.Printf("Leader: %s\n", state.Leader)
				fmt.Printf("Members: %v\n", state.Members)
				fmt.Println("------------------")

			case <-ctx.Done():
				return
			}
		}
	}()

	<-signalCh
	log.Println("Received termination signal, shutting down...")

	// Properly close the client
	if err := client.Close(); err != nil {
		log.Printf("Error during client shutdown: %v", err)
	} else {
		log.Println("Client shutdown successfully")
	}

	cancel()
	wg.Wait()
}
