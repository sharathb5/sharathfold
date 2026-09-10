// Example: create a Dispatcher, start workers, and dispatch one event.
//
//	go run ./examples/basic
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/sharathb5/sharathfold"
)

func main() {
	ctx := context.Background()
	d, err := fold.New(fold.Config{Workers: 4})
	if err != nil {
		log.Fatal(err)
	}
	if err := d.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := d.Close(cctx); err != nil {
			log.Printf("close: %v", err)
		}
	}()

	payload, err := json.Marshal(map[string]string{"order_id": "ord_1"})
	if err != nil {
		log.Fatal(err)
	}
	if err := d.Dispatch(ctx, fold.Event{
		ID:      "evt_1",
		Type:    "order.created",
		Payload: payload,
	}, []fold.Subscriber{{
		ID:  "sub_acme",
		URL: "https://example.com/hooks/acme",
	}}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("dispatched evt_1")
}
