package sqs

import (
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/gabs"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/gabrielperezs/goreactor/lib"
	"github.com/gabrielperezs/goreactor/reactor"
	"github.com/gallir/dynsemaphore"
)

var MessageSystemAttributeNameSentTimestamp = sqs.MessageSystemAttributeNameSentTimestamp

var connPool sync.Map

type sqsListen struct {
	sync.Mutex
	URL                 string
	Region              string
	Profile             string
	MaxNumberOfMessages int64

	svc *sqs.SQS

	exiting  uint32
	exited   bool
	exitedMu sync.Mutex
	done     chan bool

	broadcastCh       sync.Map
	pendings          map[string]int
	messError         map[string]bool
	maxQueuedMessages *dynsemaphore.DynSemaphore // Max of goroutines wating to send the message
}

func newSQSListen(r *reactor.Reactor, c map[string]interface{}) (*sqsListen, error) {

	p := &sqsListen{
		done: make(chan bool),
	}

	for k, v := range c {
		switch strings.ToLower(k) {
		case "url":
			p.URL = v.(string)
		case "region":
			p.Region = v.(string)
		case "profile":
			p.Profile = v.(string)
		case "maxnumberofmessages":
			p.MaxNumberOfMessages, _ = v.(int64)
		}
	}

	if p.MaxNumberOfMessages == 0 {
		p.MaxNumberOfMessages = defaultMaxNumberOfMessages
	}

	if p.URL == "" {
		return nil, fmt.Errorf("SQS ERROR: URL not found or invalid")
	}

	if p.Region == "" {
		return nil, fmt.Errorf("SQS ERROR: Region not found or invalid")
	}

	log.Printf("SQS NEW %s", p.URL)

	sess, err := session.NewSessionWithOptions(session.Options{
		Profile:           p.Profile,
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, err
	}
	p.broadcastCh.Store(r, r.Concurrent)
	p.pendings = make(map[string]int)
	p.messError = make(map[string]bool)

	p.svc = sqs.New(sess, &aws.Config{Region: aws.String(p.Region)})
	p.maxQueuedMessages = dynsemaphore.New(0)
	p.updateConcurrency()

	go p.listen()

	return p, nil
}

func (p *sqsListen) AddOrUpdate(r *reactor.Reactor) {
	p.broadcastCh.Store(r, r.Concurrent)
	p.updateConcurrency()
}

func (p *sqsListen) updateConcurrency() {
	total := 0
	p.broadcastCh.Range(func(k, v interface{}) bool {
		total += v.(int)
		return true
	})
	maxPendings := total
	if maxPendings < runtime.NumCPU() {
		maxPendings = runtime.NumCPU()
	}
	maxPendings = maxPendings * 2 // Its means x*nreactors max pending messages via goroutines, What's the right number?
	log.Printf("SQS: total concurrency: %d, max pending in-flight messages: %d", total, maxPendings)
	p.maxQueuedMessages.SetConcurrency(total)
}

func (p *sqsListen) listen() {
	defer func() {
		p.done <- true
		log.Printf("SQS EXIT %s", p.URL)
	}()

	for {
		if atomic.LoadUint32(&p.exiting) > 0 {
			log.Printf("SQS Listener Stopped %s", p.URL)
			tries := 0
			for len(p.pendings) > 0 {
				time.Sleep(time.Second)
				tries++
				if tries > 120 { // Wait no more than 120 seconds, the usual max
					log.Printf("WARNING, timeout waiting for %d pending messages", len(p.pendings))
					break
				}
			}
			return
		}

		params := &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(p.URL),
			MaxNumberOfMessages: aws.Int64(p.MaxNumberOfMessages),
			WaitTimeSeconds:     aws.Int64(waitTimeSeconds),
			AttributeNames:      []*string{&MessageSystemAttributeNameSentTimestamp},
		}

		resp, err := p.svc.ReceiveMessage(params)

		if err != nil {
			log.Printf("ERROR: AWS session on %s - %s", p.URL, err)
			time.Sleep(15 * time.Second)
			continue
		}

		for _, msg := range resp.Messages {
			p.deliver(msg)
		}
	}
}

func (p *sqsListen) deliver(msg *sqs.Message) {
	// Flag to delete the message if don't match with at least one reactor condition
	atLeastOneValid := false

	timestamp, ok := msg.Attributes[sqs.MessageSystemAttributeNameSentTimestamp]
	var sentTimestamp int64
	if ok && timestamp != nil {
		sentTimestamp, _ = strconv.ParseInt(*timestamp, 10, 64)
	}

	m := &Msg{
		SQS: p.svc,
		B:   []byte(*msg.Body),
		M: &sqs.DeleteMessageBatchRequestEntry{
			Id:            msg.MessageId,
			ReceiptHandle: msg.ReceiptHandle,
		},
		URL:           aws.String(p.URL),
		SentTimestamp: sentTimestamp,
	}

	jsonParsed, err := gabs.ParseJSON(m.B)
	if err == nil && jsonParsed.Exists("Message") {
		s := strings.Replace(jsonParsed.S("Message").String(), "\\\"", "\"", -1)
		s = strings.TrimPrefix(s, "\"")
		s = strings.TrimSuffix(s, "\"")
		m.B = []byte(s)
	}

	p.broadcastCh.Range(func(k, v interface{}) bool {
		if err := k.(*reactor.Reactor).MatchConditions(m); err != nil {
			return true
		}
		atLeastOneValid = true
		p.addPending(m)
		// Send the message in parallel to avoid blocking all messages
		// due to a long standing reactor that has its chan full
		p.maxQueuedMessages.Access() // Check the limit of goroutines
		go func(m *Msg) {
			defer func() {
				if r := recover(); r != nil {
					return // Ignore "closed channel" error when the program finishes
				}
			}()
			k.(*reactor.Reactor).Ch <- m
			p.maxQueuedMessages.Release()
		}(m)
		return true
	})

	// We delete this message if is invalid for all the reactors
	if !atLeastOneValid {
		log.Printf("Invalid message from %s, deleted: %s", p.URL, *msg.Body)
		p.delete(m)
	}

}

func (p *sqsListen) Stop() {
	if atomic.LoadUint32(&p.exiting) > 0 {
		return
	}
	atomic.AddUint32(&p.exiting, 1)
	log.Printf("SQS Input Stopping %s", p.URL)
}

func (p *sqsListen) Exit() error {
	p.exitedMu.Lock()
	defer p.exitedMu.Unlock()

	if p.exited {
		return nil
	}

	p.Stop()
	p.exited = <-p.done
	return nil
}

func (p *sqsListen) delete(v lib.Msg) (err error) {
	msg, ok := v.(*Msg)
	if !ok {
		log.Printf("ERROR SQS Delete: invalid message %+v", v)
		return
	}

	if msg.SQS == nil || msg == nil {
		return
	}
	if _, err = msg.SQS.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      msg.URL,
		ReceiptHandle: msg.M.ReceiptHandle,
	}); err != nil {
		log.Printf("ERROR: %s - %s", *msg.URL, err)
	}
	return
}

func (p *sqsListen) addPending(m lib.Msg) {
	p.Lock()
	defer p.Unlock()
	msg, ok := m.(*Msg)
	if !ok {
		return
	}
	id := *msg.M.ReceiptHandle
	v, ok := p.pendings[id]
	if ok {
		v += 1
	} else {
		v = 1
	}
	p.pendings[id] = v
}

// Done removes the message from the pending queue.
func (p *sqsListen) Done(m lib.Msg, statusOk bool) {
	msg, ok := m.(*Msg)
	if !ok {
		return
	}
	id := *msg.M.ReceiptHandle

	p.Lock()
	defer p.Unlock()

	// If it's not in pending, ignore it
	v, ok := p.pendings[id]
	if !ok {
		return
	}
	v -= 1
	p.pendings[id] = v
	if !statusOk {
		p.messError[id] = true
	}

	// Check if it's the last
	if v <= 0 {
		delete(p.pendings, id)
		_, hadError := p.messError[id]
		if !hadError {
			// Delete the message if there's no more pending reactors
			go p.delete(m) // Execute delete message outside the Lock
		}
	}
}
