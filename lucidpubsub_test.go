package lucidpubsub

import (
	"bytes"
	"context"
	"encoding/binary"
	"log"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/dgraph-io/badger"
	"github.com/stretchr/testify/assert"
)

const (
	EventCount  = int64(1000)
	SequenceSum = (EventCount * (EventCount + 1)) / 2
	BADGER_DIR  = "./badgerdb_test"

	ProjectID            = "testing"
	TopicID              = "testpub"
	MemorySubscriptionID = "testsubmemory"
	BadgerSubscriptionID = "testsubbadger"
)

type EventPayload struct {
	Sequence int64
	Bytes    []byte
}

var Events = []*EventPayload{}

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

	//memory subscription
	sub, err := client.CreateSubscription(ctx, MemorySubscriptionID, pubsub.SubscriptionConfig{
		Topic: topic,
		//#HACK for pubsub emulator in latest HEAD version of golang PUBSUB
		RetentionDuration: time.Duration(5 * 24 * time.Hour),
	})
	ok, err = sub.Exists(ctx)
	if !ok || err != nil {
		log.Panicf("Failed to Get Subscription: %v", err)
	}

	//badger subscription
	sub, err = client.CreateSubscription(ctx, BadgerSubscriptionID, pubsub.SubscriptionConfig{
		Topic: topic,
		//#HACK for pubsub emulator in latest HEAD version of golang PUBSUB
		RetentionDuration: time.Duration(5 * 24 * time.Hour),
	})
	ok, err = sub.Exists(ctx)
	if !ok || err != nil {
		log.Panicf("Failed to Get Subscription: %v", err)
	}

	for i := int64(0); i < EventCount; i++ {
		bytes := make([]byte, 8)
		binary.PutVarint(bytes, i+1)

		Events = append(
			Events,
			&EventPayload{i, bytes},
		)
	}
}

func TestPublish(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, ProjectID)
	assert.Nil(err)

	// Get Topic
	topic := client.Topic(TopicID)
	ok, err := topic.Exists(ctx)
	assert.Nil(err)
	assert.True(ok)

	results := []*pubsub.PublishResult{}
	for _, event := range Events {
		msg := &pubsub.Message{Data: event.Bytes, Attributes: make(map[string]string)}
		msg.Attributes["sequence"] = strconv.FormatInt(event.Sequence, 10)

		res := topic.Publish(ctx, msg)
		results = append(results, res)
		time.Sleep(10 * time.Millisecond)
	}

	for _, result := range results {
		_, err = result.Get(ctx)
		assert.Nil(err)
	}
}

func TestMemoryQueue(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, ProjectID)
	assert.Nil(err)

	// Get Topic
	topic := client.Topic(TopicID)
	ok, err := topic.Exists(ctx)
	assert.Nil(err)
	assert.True(ok)

	// Get Subscription
	sub := client.Subscription(MemorySubscriptionID)
	ok, err = sub.Exists(ctx)
	assert.Nil(err)
	assert.True(ok)

	rcvdEvents := make([][]byte, 0, 1000)
	handler := NewMemoryQueue(0)
	cctx, cancel := context.WithTimeout(ctx, time.Duration(15*time.Second))
	err = sub.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
		seq, err := strconv.ParseInt(m.Attributes["sequence"], 10, 64)
		if err != nil {
			m.Nack()
			cancel()
			t.Errorf("Sequence Parsing Error: %v", err)
		}

		if err := handler.Enqueue(seq, m.Data); err != nil {
			log.Print(err)
			m.Nack()
			cancel()
		}

		m.Ack()
	})

	log.Println("Sub Receive:", err, cctx.Err)

	it := handler.NewIterator()
	for it.HasNext() {
		bytes := it.Next()
		rcvdEvents = append(rcvdEvents, bytes)
	}

	assert.Nil(err)
	assert.Equal(len(Events), len(rcvdEvents))
	for i := 0; i < len(rcvdEvents); i++ {
		log.Println(binary.Varint(rcvdEvents[i]))
		assert.True(bytes.Equal(rcvdEvents[i], Events[i].Bytes))
	}
}

func TestBadgerQueue(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	bq, err := NewBadgerQueue(ProjectID, BadgerSubscriptionID, BADGER_DIR, time.Duration(400*time.Millisecond))
	assert.Nil(err)

	//seed the data value to 0
	dataKey := []byte{Other_Prefix}
	err = bq.db.Update(func(txn *badger.Txn) error {
		return txn.Set(dataKey, make([]byte, 8))
	})
	assert.Nil(err)

	rcvdEvents := [][]byte{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30*time.Second))
	err = bq.Receive(ctx, func(txn *badger.Txn, data []byte) error {
		//parse the data value, should be 1
		integer, _ := binary.Varint(data)
		item, err := txn.Get(dataKey)
		if err != nil {
			return err
		}
		val, err := item.Value()
		if err != nil {
			return err
		}

		integerVal, _ := binary.Varint(val)
		integerVal += integer
		newVal := make([]byte, 8)
		binary.PutVarint(newVal, integerVal)
		err = txn.Set(dataKey, newVal)

		rcvdEvents = append(rcvdEvents, data)
		if len(rcvdEvents) == len(Events) {
			cancel()
		}

		return err
	})
	assert.Nil(err)
	cancel()

	err = bq.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(dataKey)
		if err != nil {
			return err
		}

		val, err := item.Value()
		if err != nil {
			return err
		}

		total, _ := binary.Varint(val)
		assert.Equal(SequenceSum, total)
		return nil
	})
	assert.Nil(err)
}
