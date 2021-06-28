package main

import (
	"log"
	"math/rand"
	"redis-streams-implementation/client"

	"github.com/go-redis/redis/v8"
)

func main() {
	log.Println("Start the publisher")
	log.Println(" ")

	rdb, err := client.NewRedisClient()
	if err != nil {
		log.Fatalln("Failed initiate redis client", err)
	}

	for i := 0; i < 10; i++ {
		err = publishEvent(rdb)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func publishEvent(client *redis.Client) error {
	log.Println("Publishing event to redis")

	err := client.XAdd(client.Context(), &redis.XAddArgs{
		Stream:       "messages",
		MaxLen:       0,
		MaxLenApprox: 0,
		ID:           "",
		Values: map[string]interface{}{
			"intent":      string("new message"),
			"messageID":   int(rand.Intn(9999999)),
			"messageText": string("message text"),
		},
	}).Err()

	return err
}
