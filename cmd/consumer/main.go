package main

import (
	"fmt"
	"log"
	"redis-streams-implementation/client"

	"github.com/go-redis/redis/v8"
	"github.com/rs/xid"
)

func main() {
	log.Println("Start the consumer")
	log.Println(" ")

	rdb, err := client.NewRedisClient()
	if err != nil {
		log.Fatalln("Failed initiate redis client", err)
	}

	stream := "messages"
	consumerGroup := "messages-consumer-group"

	err = rdb.XGroupCreate(rdb.Context(), stream, consumerGroup, "0").Err()
	if err != nil {
		log.Println(err)
	}

	uniqueID := xid.New().String()

	for {
		entries, err := rdb.XReadGroup(rdb.Context(), &redis.XReadGroupArgs{
			Group:    consumerGroup,
			Consumer: uniqueID,
			Streams:  []string{stream, ">"},
			Count:    2,
			Block:    0,
			NoAck:    false,
		}).Result()

		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i < len(entries[0].Messages); i++ {
			eventID := entries[0].Messages[i].ID
			eventValues := entries[0].Messages[i].Values

			eventValueIntent := fmt.Sprintf("%v", eventValues["intent"])

			messageID := fmt.Sprintf("%v", eventValues["messageID"])
			messageText := fmt.Sprintf("%v", eventValues["messageText"])

			if eventValueIntent == "new message" {
				err := handleNewMessage(messageID, messageText)
				if err != nil {
					log.Fatal(err)
				}

				rdb.XAck(rdb.Context(), stream, consumerGroup, eventID)
			}
		}
	}

}

func handleNewMessage(messageID string, messageText string) error {
	log.Printf("Handling new message. ID: %s - Text: %s\n", messageID, messageText)
	return nil
}
