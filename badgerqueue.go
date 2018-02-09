package lucidpubsub

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/dgraph-io/badger"
)

const (
	Data_Prefix byte = iota
	Sequence_Prefix
	Other_Prefix
)

var (
	CurrSeqKey = []byte{Data_Prefix, 0}
)

type BadgerPayloadHandler func(txn *badger.Txn, data []byte) error

type BadgerQueue struct {
	listenWindow   time.Duration
	db             *badger.DB
	projectID      string
	subscriptionID string
}

func NewBadgerQueue(projectID, subscriptionID, storageDir string, listenWindow time.Duration) (*BadgerQueue, error) {
	opts := badger.DefaultOptions
	opts.Dir = storageDir
	opts.ValueDir = storageDir
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	//get current sequence
	if err := db.Update(func(txn *badger.Txn) error {
		if item, err := txn.Get(CurrSeqKey); err == badger.ErrKeyNotFound {
			fmt.Println("No Saved Sequence! Setting to 0")
			return txn.Set(CurrSeqKey, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0})
		} else {
			if savedSeq, err := item.Value(); err == nil {
				fmt.Println("Saved Sequence:", binary.BigEndian.Uint64(savedSeq))
			} else {
				return err
			}
		}
		return err
	}); err != nil {
		return nil, err
	}

	return &BadgerQueue{listenWindow, db, projectID, subscriptionID}, nil
}

func (e *BadgerQueue) Close() error {
	return e.db.Close()
}

func (e *BadgerQueue) Receive(ctx context.Context, handler BadgerPayloadHandler) error {
	client, err := pubsub.NewClient(ctx, e.projectID)
	if err != nil {
		return err
	}

	// Get Subscription
	sub := client.Subscription(e.subscriptionID)
	ok, err := sub.Exists(ctx)
	if !ok {
		return fmt.Errorf("Subscription Not Found: %v, error: %v", e.subscriptionID, err)
	}
	if err != nil {
		return err
	}

	for {
		select {
		case <-time.Tick(time.Duration(1 * time.Hour)):
			//badger disk GC, cannot be concurrently run with other transactions
			e.db.PurgeOlderVersions()
			if err := e.db.RunValueLogGC(0.5); err != nil {
				return err
			}
		default:
			//Listening Loop
			cctx, cancel := context.WithTimeout(ctx, e.listenWindow)
			err = sub.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
				//get sequence
				seq, err := strconv.ParseUint(m.Attributes["sequence"], 10, 64)
				if err != nil {
					m.Nack()
					cancel()
				}

				//save payload
				if err := e.db.Update(func(txn *badger.Txn) error {
					err := txn.Set(itobSeqPrefix(seq), m.Data)
					return err
				}); err != nil {
					m.Nack()
					cancel()
				}

				m.Ack()
			})
			cancel()

			if err == nil && cctx.Err() == context.DeadlineExceeded {
				err := e.Process(handler)
				if err != nil {
					return err
				}
			} else {
				return nil
			}
		}
	}
}

func (e *BadgerQueue) Process(handler BadgerPayloadHandler) error {
	return e.db.Update(func(txn *badger.Txn) error {
		//Iterate on sequence_prefix.  Badger Guarantees byte-wise lexicographical sorting order
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		prefix := []byte{Sequence_Prefix}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			//get current sequence
			seqItem, err := txn.Get(CurrSeqKey)
			if err != nil {
				return err
			}
			seqBytes, err := seqItem.Value()
			if err != nil {
				return err
			}
			currSequence := binary.BigEndian.Uint64(seqBytes)

			item := it.Item()
			key := item.Key()
			sequence := binary.BigEndian.Uint64(key[1:])
			if currSequence == sequence {
				if value, err := item.Value(); err != nil {
					return err
				} else {
					//if badger isn't used in this handler, a small chance of corruption can
					//occur if a crash happens here.  This can be mitigated with good testing and
					//a full backup of events, compensating actions, and monitoring.
					if err := handler(txn, value); err != nil {
						return err
					}
					currSequence++
					if err := txn.Set(CurrSeqKey, itob(currSequence)); err != nil {
						return err
					}
					if err := txn.Delete(itobSeqPrefix(sequence)); err != nil {
						return err
					}
				}
			} else if sequence > currSequence {
				// fmt.Println("OPTIMUS PRIME!!")
				//optimization, we quit early if key is greater than current index because we are guaranteed order
				return nil
			}
		}
		return nil
	})
}

//we use big endian because it can be lexicographically sorted (badger sorts keys)
func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func itobSeqPrefix(v uint64) []byte {
	b := make([]byte, 9)
	b[0] = Sequence_Prefix
	binary.BigEndian.PutUint64(b[1:], v)
	return b
}
