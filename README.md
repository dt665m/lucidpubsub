# Lucid PubSub

Google PubSub Ordering Queue in Go.

# Usage

1. Add an attribute in pubsub for the sequential index of the the published event
2. Receive using a context.WithTimeout on a loop.  150-350ms seems to work best
3. Iterate to dequeue

```go
handler := NewMemoryQueue(0)
cctx, cancel := context.WithTimeout(ctx, time.Duration(200*time.Millisecond))
err = sub.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
  seq, err := strconv.ParseInt(m.Attributes["sequence"], 10, 64)
  if err != nil {
    m.Nack()
    cancel()
  }

  if err := handler.Enqueue(seq, m.Data); err != nil {
    m.Nack()
    cancel()
  }

  m.Ack()
})
  
it := handler.NewIterator()
for it.HasNext() {
  bytes := it.Next()
  //take a bite out of the ordered []bytes!
}
```
