package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	//"errors"
	"log"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/dt665m/lucidpubsub"
	"github.com/jackc/pgx"
)

const (
	EventCount           = 1000
	SumSeries            = ((EventCount - 1) * (EventCount)) / 2
	ProjectID            = "testing"
	TopicID              = "test"
	SubscriptionID       = "sub1"
	BackupSubscriptionID = "backup1"

	HOST     = "127.0.0.1"
	PORT     = 5432
	USER     = "postgres"
	PASSWORD = "123456"
	DB       = "testdata"
	RowID    = 37337
)

type EventPayload struct {
	Sequence int64
	Bytes    []byte
}

var Events = []*EventPayload{}
var SimulateProccessFail = true

func init() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, ProjectID)
	if err != nil {
		log.Panicf("Failed to create Connect to Google Pubsub: %v", err)
	}

	// create topic
	topic, _ := client.CreateTopic(ctx, TopicID)
	ok, err := topic.Exists(ctx)
	if !ok || err != nil {
		log.Panicf("Failed to Get Topic: %v", err)
	}

	//backup subscription
	sub, err := client.CreateSubscription(ctx, BackupSubscriptionID, pubsub.SubscriptionConfig{
		Topic: topic,
		//#hack for pubsub emulator in latest HEAD version of golang PUBSUB
		//#ref https://github.com/GoogleCloudPlatform/google-cloud-go/issues/847
		RetentionDuration: time.Duration(5 * 24 * time.Hour),
	})
	ok, err = sub.Exists(ctx)
	if !ok || err != nil {
		log.Panicf("Failed to Get Subscription: %v", err)
	}

	//postgres subscription
	sub, err = client.CreateSubscription(ctx, SubscriptionID, pubsub.SubscriptionConfig{
		Topic: topic,
		//#HACK for pubsub emulator in latest HEAD version of golang PUBSUB
		RetentionDuration: time.Duration(5 * 24 * time.Hour),
	})
	ok, err = sub.Exists(ctx)
	if !ok || err != nil {
		log.Panicf("Failed to Get Subscription: %v", err)
	}

	//prime the postgres database with intial value
	db, err := pgx.Connect(pgx.ConnConfig{
		Host:     HOST,
		Port:     PORT,
		User:     USER,
		Password: PASSWORD,
		Database: DB,
	})
	if err != nil {
		log.Fatalf("Failed to create db connection: %v", err)
	}
	_, err = db.Exec("INSERT INTO finaldata(id,total) VALUES ($1,$2)", RowID, 0)
	if err != nil {
		log.Fatalf("Failed to insert DB data seed: %v", err)
	}
	_, err = db.Exec("INSERT INTO checkpoint(id,checkpoint) VALUES ($1,$2)", RowID, -1)
	if err != nil {
		log.Fatalf("Failed to insert DB checkpoint seed: %v", err)
	}
	db.Close()

	for i := int64(0); i < EventCount; i++ {
		bytes := make([]byte, 8)
		binary.PutVarint(bytes, i)
		payload := &EventPayload{i, bytes}
		Events = append(Events, payload)
	}
}

func main() {
	log.Println("Starting PubSub Example with Postgres")

	wg := sync.WaitGroup{}
	wg.Add(2)
	go publish(&wg)
	go backup(&wg)

	//process until done
	checkpoint := int64(0)
	lastAck := int64(-1)
	for checkpoint < EventCount-1 {
		checkpoint, lastAck = process(lastAck)
	}
	log.Println("Processing Finished!")
	wg.Wait()

	checkData()
	log.Println("Done.")
}

func publish(wg *sync.WaitGroup) {
	defer wg.Done()

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, ProjectID)
	if err != nil {
		log.Fatal(err)
	}

	// Get Topic
	topic := client.Topic(TopicID)
	ok, err := topic.Exists(ctx)
	if err != nil || !ok {
		log.Fatal(err, ok)
	}

	results := []*pubsub.PublishResult{}
	for _, payload := range Events {
		msg := &pubsub.Message{Data: payload.Bytes, Attributes: make(map[string]string)}
		msg.Attributes["sequence"] = strconv.FormatInt(payload.Sequence, 10)

		res := topic.Publish(ctx, msg)
		results = append(results, res)
		time.Sleep(10 * time.Millisecond)
	}

	for _, result := range results {
		if _, err := result.Get(ctx); err != nil {
			log.Println("Failed to Send Message: ", err)
		}
	}
	log.Println("Publish Finished!")
}

func backup(wg *sync.WaitGroup) {
	defer wg.Done()

	db, err := pgx.Connect(pgx.ConnConfig{
		Host:     HOST,
		Port:     PORT,
		User:     USER,
		Password: PASSWORD,
		Database: DB,
	})
	if err != nil {
		log.Fatalf("Failed to create db connection: %v", err)
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, ProjectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Get Topic
	topic := client.Topic(TopicID)
	ok, err := topic.Exists(ctx)
	if !ok || err != nil {
		log.Fatalf("Topic retrieval fail: %v", err)
	}

	// Get Backup Subscription
	sub := client.Subscription(BackupSubscriptionID)
	ok, err = sub.Exists(ctx)
	if !ok || err != nil {
		log.Fatalf("Subscription Not Found: %v", TopicID)
	}

	count := 0
	mu := sync.Mutex{}
	cctx, cancel := context.WithCancel(ctx)
	err = sub.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
		//get sequence
		seq, err := strconv.ParseInt(m.Attributes["sequence"], 10, 64)
		if err != nil {
			m.Nack()
			cancel()
		}

		//try to store it, Nack() if we can't store it
		mu.Lock()
		_, dbErr := db.Exec("INSERT INTO events(sequence,data) VALUES($1,$2)", seq, m.Data)
		count++
		mu.Unlock()
		if dbErr != nil {
			log.Printf("DB Event Storage Failed: %v\n", dbErr)
			m.Nack()
			cancel()
		}
		m.Ack()

		//we've finished archiving
		if count == len(Events) {
			cancel()
		}
	})
	if err != nil {
		log.Printf("Backup Subscription Failed: %v", err)
	}
	cancel()

	//Query and check event backup to see if we got every packet and backed it up
	rows, err := db.Query("SELECT * FROM events ORDER BY sequence ASC")
	if err != nil {
		log.Fatalf("Event Rows Not Found: %v", err)
	}
	index := 0
	for rows.Next() {
		var sequence int64
		var data []byte
		if err := rows.Scan(&sequence, &data); err != nil {
			log.Fatalf("Event Query Failed: %v", err)
		}
		event := Events[index]
		if event.Sequence != sequence {
			log.Printf("Sequence Incorrect.  Expected: %v, Got: %v", event.Sequence, sequence)
		}
		if !bytes.Equal(event.Bytes, data) {
			log.Printf("Data Incorrect.  Expected: %v, Got: %v", event.Bytes, data)
		}
		index++
	}
	rows.Close()
	db.Close()

	log.Println("Backup Finished!")
}

func process(lastAck int64) (int64, int64) {
	// Random Listen Duration range to Simulate Stop/Crash
	rand.Seed(time.Now().Unix())
	max := EventCount - 1
	min := int(lastAck) + 1
	simulateFailAt := int64(rand.Intn(max-min) + min) //int64(996) //

	db, err := pgx.Connect(pgx.ConnConfig{
		Host:     HOST,
		Port:     PORT,
		User:     USER,
		Password: PASSWORD,
		Database: DB,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Create Event Handler
	count := int64(0)
	eventHandler := func(tx *pgx.Tx, data []byte) error {
		var checkpoint int64
		if err := tx.QueryRow("SELECT checkpoint FROM checkpoint WHERE id=$1", RowID).Scan(&checkpoint); err != nil {
			return fmt.Errorf("Checkpoint Query Error: %v, Rollback: %v", err, tx.Rollback())
		}

		//check if we are on the correct sequence, otherwise fail
		integer, _ := binary.Varint(data)
		if integer != checkpoint+1 {
			return fmt.Errorf("Sequence Error. expected %v, got %v; Rollback: %v", checkpoint+1, integer, tx.Rollback())
		}

		//simulate fail if we are supposed to
		if SimulateProccessFail && simulateFailAt == integer {
			return fmt.Errorf("Simulated Transaction Failure at: %d", integer)
		}

		_, err = tx.Exec("UPDATE checkpoint SET checkpoint=$1 WHERE id=$2", integer, RowID)
		if err != nil {
			return fmt.Errorf("Checkpoint Update Error: %v, Rollback: %v", err, tx.Rollback())
		}

		_, err = tx.Exec("UPDATE finaldata SET total=total+$1 WHERE id=$2", integer, RowID)
		if err != nil {
			return fmt.Errorf("Total Update Error: %v, Rollback: %v", err, tx.Rollback())
		}

		log.Println("Processed: ", integer)
		count = integer
		return nil
	}

	//find our checkpoint
	checkpoint := int64(0)
	err = db.QueryRow("SELECT checkpoint FROM checkpoint").Scan(&checkpoint)
	if err != nil {
		log.Fatalf("Checkpoint Row Not Found: %v", err)
	}

	// check if backup has everything we need (events > checkpoint and <= last ack to pubsub)
	missingEvents := lastAck - checkpoint
	log.Println("Last CheckPoint: ", checkpoint)
	log.Println("Last Ack: ", lastAck)
	if missingEvents > 0 {
		for {
			backupCount := int64(0)
			err := db.QueryRow("SELECT count(*) from events WHERE sequence>$1 AND sequence<=$2", checkpoint, lastAck).Scan(&backupCount)
			if err != nil {
				log.Fatal("backup state query failed: ", err)
			}
			if backupCount >= missingEvents {
				break
			}
			log.Printf("Backup Not Yet Caught Up: expecting %d, current %d", missingEvents, backupCount)
			time.Sleep(500 * time.Millisecond)
		}

		//catchup events from backup
		rows, err := db.Query("SELECT * FROM events WHERE sequence>$1 AND sequence<=$2 ORDER BY sequence ASC", checkpoint, lastAck)
		if err != nil {
			log.Fatalf("Event Rows Not Found: %v", err)
		}
		catchupEvents := []*EventPayload{}
		for rows.Next() {
			var sequence int64
			var data []byte
			if err := rows.Scan(&sequence, &data); err != nil {
				log.Fatalf("Event Query Failed: %v", err)
			}
			catchupEvents = append(catchupEvents, &EventPayload{sequence, data})
		}
		rows.Close()

		SimulateProccessFail = false
		tx, err := db.Begin()
		if err != nil {
			log.Fatal("Transaction Start Failed: ", err)
		}
		for _, event := range catchupEvents {
			if err := eventHandler(tx, event.Bytes); err != nil {
				log.Fatalf("Dispatch Error in Catchup: %v, Rollback: %v", err, tx.Rollback())
			}
		}
		if err := tx.Commit(); err != nil {
			log.Fatal("Transaction End Failed: ", err)
		}
		log.Println("Events Handled From Backup:", len(catchupEvents))

		//quit early if we already finished after catchup
		if count == EventCount-1 {
			return count, count
		}
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, ProjectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Get Topic
	topic := client.Topic(TopicID)
	ok, err := topic.Exists(ctx)
	if !ok || err != nil {
		log.Fatalf("Topic retrieval fail: %v", err)
	}

	// Get Postgres Subscription
	sub := client.Subscription(SubscriptionID)
	ok, err = sub.Exists(ctx)
	if !ok || err != nil {
		log.Fatalf("Subscription Not Found: %v", TopicID)
	}

	//queue and order events
	log.Println("Processing From: ", lastAck+1)
	memQueue := lucidpubsub.NewMemoryQueue(lastAck + 1)
ListenLoop:
	for {
		cctx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		err = sub.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
			seq, err := strconv.ParseInt(m.Attributes["sequence"], 10, 64)
			if err != nil {
				m.Nack()
				cancel()
				log.Printf("Sequence Parsing Error: %v", err)
			}

			if err := memQueue.Enqueue(seq, m.Data); err != nil {
				log.Print(err)
				m.Nack()
				cancel()
			}

			m.Ack()
		})
		cancel()

		if err != nil {
			log.Printf("Handler Error: %v", err)
		}

		//handle queued up events
		tx, err := db.Begin()
		if err != nil {
			log.Fatal("Transaction Start Failed: ", err)
		}
		it := memQueue.NewIterator()
		for it.HasNext() {
			if err := eventHandler(tx, it.Next()); err != nil {
				//if we run into a transaction error we might break the whole thing
				log.Printf("Event Handling Failed: %v, Rollback: %v", err, tx.Rollback())
				break ListenLoop
			}
		}
		if err := tx.Commit(); err != nil {
			log.Fatal("Transaction End Failed: ", err)
		}

		cErr := ctx.Err()
		if cErr != nil && cErr != context.DeadlineExceeded || memQueue.LatestAck() == EventCount-1 {
			log.Println("Context: ", cErr)
			break
		}
	}

	db.Close()
	return count, memQueue.LatestAck()
}

func checkData() {
	db, err := pgx.Connect(pgx.ConnConfig{
		Host:     HOST,
		Port:     PORT,
		User:     USER,
		Password: PASSWORD,
		Database: DB,
	})
	if err != nil {
		log.Fatal(err)
	}

	//Query Aggregate and check if we are consistent
	total := int64(0)
	err = db.QueryRow("SELECT total FROM finaldata").Scan(&total)
	if err != nil {
		log.Fatalf("Total Row Not Found: %v", err)
	}
	log.Printf("Expected Data Series Sum: %d Got: %d", SumSeries, total)

	//Query Checkpoint and check if we are consistent
	checkpoint := int64(0)
	err = db.QueryRow("SELECT checkpoint FROM checkpoint").Scan(&checkpoint)
	if err != nil {
		log.Fatalf("Checkpoint Row Not Found: %v", err)
	}
	log.Printf("Expected Checkpoint: %d Got: %d", len(Events)-1, checkpoint)

	db.Close()
}

// 	log.Println("Handler Last ACK: ", handler.LatestAck())
// 	if err != nil || cctx.Err() == context.DeadlineExceeded {
// 		log.Fatal("Subscription Receive Stopped with Error: ", err, cctx.Err())
// 	}

// 	//reset the handler without crashing
// 	dispatcher2 := func(seq int64, data []byte) error {
// 		tx, err := db.Begin()
// 		if err != nil {
// 			return err
// 		}

// 		// _, dbErr := tx.Exec("UPDATE checkpoint SET checkpoint=$1 WHERE id=$2", seq, rowID)
// 		// if err != nil {
// 		// 	return dbErr
// 		// }

// 		// integer, _ := binary.Varint(data)
// 		// _, dbErr = tx.Exec("UPDATE finaldata SET total=total+$1 WHERE id=$2", integer, rowID)
// 		// if err != nil {
// 		// 	return dbErr
// 		// }
// 		// count++
// 		// return dbErr

// 		return nil
// 	}

// 	// after crash, find our checkpoint
// 	lastMemQueueAck := handler.LatestAck() //We get this value from crash logs
// 	checkpoint := int64(0)
// 	err = db.QueryRow("SELECT checkpoint FROM checkpoint").Scan(&checkpoint)
// 	if err != nil {
// 		log.Fatalf("Checkpoint Row Not Found: %v", err)
// 	}
// 	if checkpoint != sequenceToCrash-1 {
// 		log.Fatalf("Incorrect Checkpoint: expected %d, got %d", sequenceToCrash-1, checkpoint)
// 	}
// 	log.Println("Checkpoint:", checkpoint)

// 	// check if backup has everything we need (events > checkpoint and <= last ack to pubsub)
// 	for {
// 		backupCount := int64(0)
// 		err := db.QueryRow("SELECT count(*) from events WHERE sequence>$1 AND sequence<=$2", checkpoint, lastMemQueueAck).Scan(&backupCount)
// 		if err != nil {
// 			log.Fatal("backup state query failed: ", err)
// 		}
// 		if backupCount >= (lastMemQueueAck - checkpoint) {
// 			break
// 		}
// 		log.Printf("Backup Not Yet Caught Up: expecting %d, current %d", lastMemQueueAck-checkpoint, backupCount)
// 		time.Sleep(500 * time.Millisecond)
// 	}

// 	//catchup events from backup
// 	rows, err := db.Query("SELECT * FROM events WHERE sequence>$1 AND sequence<=$2 ORDER BY sequence ASC", checkpoint, lastMemQueueAck)
// 	if err != nil {
// 		log.Fatalf("Event Rows Not Found: %v", err)
// 	}
// 	catchupEvents := []*EventPayload{}
// 	for rows.Next() {
// 		var sequence int64
// 		var data []byte
// 		if err := rows.Scan(&sequence, &data); err != nil {
// 			log.Fatalf("Event Query Failed: %v", err)
// 		}
// 		catchupEvents = append(catchupEvents, &EventPayload{data, sequence})
// 	}
// 	rows.Close()
// 	log.Println("Catchup Count: ", len(catchupEvents))
// 	for _, event := range catchupEvents {
// 		if err := dispatcher2(event.Sequence, event.Bytes); err != nil {
// 			log.Fatalf("Dispatch Error in Catchup: %v", err)
// 		}
// 	}

// 	// find our checkpoint again
// 	checkpoint = int64(0)
// 	err = db.QueryRow("SELECT checkpoint FROM checkpoint").Scan(&checkpoint)
// 	if err != nil {
// 		log.Fatalf("Checkpoint Row Not Found: %v", err)
// 	}
// 	log.Println("Last Checkpoint: ", checkpoint)

// 	if int(checkpoint) < len(Events)-1 {
// 		// begin handling again from checkpoint
// 		log.Printf("Catchup unfinished, Checkpoint: %d, Leftover: %d ", checkpoint, len(Events)-1-int(checkpoint))
// 		handler = NewOrderedHandler(dispatcher2, checkpoint+1)
// 		cctx, cancel := context.WithTimeout(context.Background(), time.Duration(30*time.Second))
// 		err := sub.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
// 			if hErr := handler.Handle(ctx, m); hErr != nil {
// 				cancel()
// 			}

// 			if count == len(Events) {
// 				cancel()
// 			}
// 		})
// 		cancel()

// 		if err != nil || cctx.Err() == context.DeadlineExceeded {
// 			log.Printf("Subscription Receive Stopped with Error: %v, %v", err, cctx.Err())
// 		}
// 	}

// 	// check to see we got it all
// 	if count != len(Events) {
// 		log.Printf("Incorrect Number of Events Received: expected %d, got %d", len(Events), count)
// 	}

// 	//Query Aggregate and check if we are consistent
// 	total := int64(0)
// 	err = db.QueryRow("SELECT total FROM finaldata").Scan(&total)
// 	if err != nil {
// 		log.Fatalf("Total Row Not Found: %v", err)
// 	}
// 	if int(total) != len(Events) {
// 		log.Fatalf("Total Incorrect, Expected: %d, Got: %d", len(Events), total)
// 	}

// 	//Query Checkpoint and check if we are consistent
// 	checkpoint = int64(0)
// 	err = db.QueryRow("SELECT checkpoint FROM checkpoint").Scan(&checkpoint)
// 	if err != nil {
// 		log.Fatalf("Checkpoint Row Not Found: %v", err)
// 	}
// 	if int(checkpoint) != len(Events)-1 {
// 		log.Fatalf("Checkpoint Incorrect, Expected: %d, Got: %d", len(Events), checkpoint)
// 	}
// 	db.Close()
// }

// func TestLongtermStore {
// 	db, err := pgx.Connect(pgx.ConnConfig{
// 		Host:     "127.0.0.1",
// 		Port:     PORT,
// 		User:     USER,
// 		Password: PASSWORD,
// 		Database: DB,
// 	})
// 	if err != nil {
// 		log.Fatalf("Failed to create db connection: %v", err)
// 	}

// 	ctx := context.Background()
// 	client, err := pubsub.NewClient(ctx, ProjectID)
// 	if err != nil {
// 		log.Fatalf("Failed to create client: %v", err)
// 	}

// 	// Get Topic
// 	topic := client.Topic(TopicID)
// 	ok, err := topic.Exists(ctx)
// 	if !ok || err != nil {
// 		log.Fatalf("Topic retrieval fail: %v", err)
// 	}

// 	// Get Backup Subscription
// 	sub := client.Subscription(BackupSubscriptionID)
// 	ok, err = sub.Exists(ctx)
// 	if !ok || err != nil {
// 		log.Fatalf("Subscription Not Found: %v", TopicID)
// 	}

// 	count := 0
// 	mu := sync.Mutex{}
// 	cctx, cancel := context.WithCancel(ctx)
// 	time.AfterFunc(time.Duration(20*time.Second), cancel)
// 	err = sub.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
// 		//get sequence
// 		seq, err := strconv.ParseInt(m.Attributes["sequence"], 10, 64)
// 		if err != nil {
// 			m.Nack()
// 			cancel()
// 		}

// 		//try to store it, Nack() if we can't store it
// 		mu.Lock()
// 		_, dbErr := db.Exec("INSERT INTO events(sequence,data) VALUES($1,$2)", seq, m.Data)
// 		count++
// 		mu.Unlock()
// 		if dbErr != nil {
// 			log.Printf("DB Event Storage Failed: %v", dbErr)
// 			m.Nack()
// 			cancel()
// 		}
// 		m.Ack()

// 		//we've finished archiving
// 		if count == len(Events) {
// 			cancel()
// 		}
// 	})
// 	if err != nil {
// 		log.Printf("Backup Subscription Failed: %v", err)
// 	}
// 	cancel()

// 	//Query and check event backup to see if we got every packet and backed it up
// 	rows, err := db.Query("SELECT * FROM events ORDER BY sequence ASC")
// 	if err != nil {
// 		log.Fatalf("Event Rows Not Found: %v", err)
// 	}
// 	index := 0
// 	for rows.Next() {
// 		var sequence int64
// 		var data []byte
// 		if err := rows.Scan(&sequence, &data); err != nil {
// 			log.Fatalf("Event Query Failed: %v", err)
// 		}
// 		event := Events[index]
// 		if event.Sequence != sequence {
// 			log.Printf("Sequence Incorrect.  Expected: %v, Got: %v", event.Sequence, sequence)
// 		}
// 		if !bytes.Equal(event.Bytes, data) {
// 			log.Printf("Data Incorrect.  Expected: %v, Got: %v", event.Bytes, data)
// 		}
// 		index++
// 	}
// 	rows.Close()
// 	db.Close()
// }

// func PostgresInMemCombination() {
// 	db, err := pgx.Connect(pgx.ConnConfig{
// 		Host:     "127.0.0.1",
// 		Port:     PORT,
// 		User:     USER,
// 		Password: PASSWORD,
// 		Database: DB,
// 	})
// 	if err != nil {
// 		log.Fatalf("Failed to create db connection: %v", err)
// 	}

// 	//insert initial value and create checkpoint
// 	const rowID = 37337
// 	_, err = db.Exec("INSERT INTO finaldata(id,total) VALUES ($1,$2)", rowID, 0)
// 	if err != nil {
// 		log.Fatalf("Failed to insert DB data seed: %v", err)
// 	}
// 	_, err = db.Exec("INSERT INTO checkpoint(id,checkpoint) VALUES ($1,$2)", rowID, 0)
// 	if err != nil {
// 		log.Fatalf("Failed to insert DB data seed: %v", err)
// 	}

// 	ctx := context.Background()
// 	client, err := pubsub.NewClient(ctx, ProjectID)
// 	if err != nil {
// 		log.Fatalf("Failed to create client: %v", err)
// 	}

// 	// Get Topic
// 	topic := client.Topic(TopicID)
// 	ok, err := topic.Exists(ctx)
// 	if !ok || err != nil {
// 		log.Fatalf("Topic retrieval fail: %v", err)
// 	}

// 	// Get Postgres Subscription
// 	sub := client.Subscription(PostgresSubcriptionID)
// 	ok, err = sub.Exists(ctx)
// 	if !ok || err != nil {
// 		log.Fatalf("Subscription Not Found: %v", TopicID)
// 	}

// 	// Generate a Random Sequence to simulate Crashing
// 	rand.Seed(time.Now().Unix())
// 	max := len(Events) - 50 //guarantee a crash
// 	min := 10
// 	sequenceToCrash := int64(rand.Intn(max-min) + min)

// 	// Create Event Handler
// 	count := 0
// 	dispatcher := func(seq int64, data []byte) error {
// 		if seq == sequenceToCrash {
// 			return errors.New("Crash Simulation")
// 		}

// 		tx, err := db.Begin()
// 		if err != nil {
// 			return err
// 		}

// 		_, dbErr := tx.Exec("UPDATE checkpoint SET checkpoint=$1 WHERE id=$2", seq, rowID)
// 		if err != nil {
// 			return dbErr
// 		}

// 		integer, _ := binary.Varint(data)
// 		_, dbErr = tx.Exec("UPDATE finaldata SET total=total+$1 WHERE id=$2", integer, rowID)
// 		if err != nil {
// 			return dbErr
// 		}
// 		count++
// 		return dbErr
// 	}

// 	// handle events
// 	handler := NewOrderedHandler(dispatcher, 0)
// 	cctx, cancel := context.WithTimeout(ctx, time.Duration(30*time.Second))
// 	err = sub.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
// 		if hErr := handler.Handle(ctx, m); hErr != nil {
// 			cancel()
// 		}
// 	})
// 	cancel()

// 	log.Println("Handler Last ACK: ", handler.LatestAck())
// 	if err != nil || cctx.Err() == context.DeadlineExceeded {
// 		log.Fatal("Subscription Receive Stopped with Error: ", err, cctx.Err())
// 	}

// 	//reset the handler without crashing
// 	dispatcher2 := func(seq int64, data []byte) error {
// 		tx, err := db.Begin()
// 		if err != nil {
// 			return err
// 		}

// 		_, dbErr := tx.Exec("UPDATE checkpoint SET checkpoint=$1 WHERE id=$2", seq, rowID)
// 		if err != nil {
// 			return dbErr
// 		}

// 		integer, _ := binary.Varint(data)
// 		_, dbErr = tx.Exec("UPDATE finaldata SET total=total+$1 WHERE id=$2", integer, rowID)
// 		if err != nil {
// 			return dbErr
// 		}
// 		count++
// 		return dbErr
// 	}

// 	// after crash, find our checkpoint
// 	lastMemQueueAck := handler.LatestAck() //We get this value from crash logs
// 	checkpoint := int64(0)
// 	err = db.QueryRow("SELECT checkpoint FROM checkpoint").Scan(&checkpoint)
// 	if err != nil {
// 		log.Fatalf("Checkpoint Row Not Found: %v", err)
// 	}
// 	if checkpoint != sequenceToCrash-1 {
// 		log.Fatalf("Incorrect Checkpoint: expected %d, got %d", sequenceToCrash-1, checkpoint)
// 	}
// 	log.Println("Checkpoint:", checkpoint)

// 	// check if backup has everything we need (events > checkpoint and <= last ack to pubsub)
// 	for {
// 		backupCount := int64(0)
// 		err := db.QueryRow("SELECT count(*) from events WHERE sequence>$1 AND sequence<=$2", checkpoint, lastMemQueueAck).Scan(&backupCount)
// 		if err != nil {
// 			log.Fatal("backup state query failed: ", err)
// 		}
// 		if backupCount >= (lastMemQueueAck - checkpoint) {
// 			break
// 		}
// 		log.Printlnf("Backup Not Yet Caught Up: expecting %d, current %d", lastMemQueueAck-checkpoint, backupCount)
// 		time.Sleep(500 * time.Millisecond)
// 	}

// 	//catchup events from backup
// 	rows, err := db.Query("SELECT * FROM events WHERE sequence>$1 AND sequence<=$2 ORDER BY sequence ASC", checkpoint, lastMemQueueAck)
// 	if err != nil {
// 		log.Fatalf("Event Rows Not Found: %v", err)
// 	}
// 	catchupEvents := []*EventPayload{}
// 	for rows.Next() {
// 		var sequence int64
// 		var data []byte
// 		if err := rows.Scan(&sequence, &data); err != nil {
// 			log.Fatalf("Event Query Failed: %v", err)
// 		}
// 		catchupEvents = append(catchupEvents, &EventPayload{data, sequence})
// 	}
// 	rows.Close()
// 	log.Println("Catchup Count: ", len(catchupEvents))
// 	for _, event := range catchupEvents {
// 		if err := dispatcher2(event.Sequence, event.Bytes); err != nil {
// 			log.Fatalf("Dispatch Error in Catchup: %v", err)
// 		}
// 	}

// 	// find our checkpoint again
// 	checkpoint = int64(0)
// 	err = db.QueryRow("SELECT checkpoint FROM checkpoint").Scan(&checkpoint)
// 	if err != nil {
// 		log.Fatalf("Checkpoint Row Not Found: %v", err)
// 	}
// 	log.Println("Last Checkpoint: ", checkpoint)

// 	if int(checkpoint) < len(Events)-1 {
// 		// begin handling again from checkpoint
// 		log.Printlnf("Catchup unfinished, Checkpoint: %d, Leftover: %d ", checkpoint, len(Events)-1-int(checkpoint))
// 		handler = NewOrderedHandler(dispatcher2, checkpoint+1)
// 		cctx, cancel := context.WithTimeout(context.Background(), time.Duration(30*time.Second))
// 		err := sub.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
// 			if hErr := handler.Handle(ctx, m); hErr != nil {
// 				cancel()
// 			}

// 			if count == len(Events) {
// 				cancel()
// 			}
// 		})
// 		cancel()

// 		if err != nil || cctx.Err() == context.DeadlineExceeded {
// 			log.Printf("Subscription Receive Stopped with Error: %v, %v", err, cctx.Err())
// 		}
// 	}

// 	// check to see we got it all
// 	if count != len(Events) {
// 		log.Printf("Incorrect Number of Events Received: expected %d, got %d", len(Events), count)
// 	}

// 	//Query Aggregate and check if we are consistent
// 	total := int64(0)
// 	err = db.QueryRow("SELECT total FROM finaldata").Scan(&total)
// 	if err != nil {
// 		log.Fatalf("Total Row Not Found: %v", err)
// 	}
// 	if int(total) != len(Events) {
// 		log.Fatalf("Total Incorrect, Expected: %d, Got: %d", len(Events), total)
// 	}

// 	//Query Checkpoint and check if we are consistent
// 	checkpoint = int64(0)
// 	err = db.QueryRow("SELECT checkpoint FROM checkpoint").Scan(&checkpoint)
// 	if err != nil {
// 		log.Fatalf("Checkpoint Row Not Found: %v", err)
// 	}
// 	if int(checkpoint) != len(Events)-1 {
// 		log.Fatalf("Checkpoint Incorrect, Expected: %d, Got: %d", len(Events), checkpoint)
// 	}
// 	db.Close()
// }
