package queue_test

import (
	"errors"
	"strings"
	"testing"

	"simq/internal/queue"
)

func TestSendMessageAttributesTypesDigestAndDeepCopy(t *testing.T) {
	repository := queue.NewMemoryRepository()
	service := queue.NewService(repository, queue.WithMessageIDGenerator(func() string { return "message-attributes" }))
	mustCreateConformanceQueue(t, service, "orders", "30")
	binaryValue := []byte{1, 2, 3}
	attributes := map[string]queue.MessageAttribute{
		"Text":    {DataType: "String.custom", StringValue: "hello"},
		"Attempt": {DataType: "Number", StringValue: "1.25e2"},
		"Payload": {DataType: "Binary.application", BinaryValue: binaryValue},
	}
	sent, err := service.SendMessage(queue.SendMessageInput{
		QueueName:         "orders",
		MessageBody:       "body",
		MessageAttributes: attributes,
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if sent.MD5OfMessageAttributes == "" {
		t.Fatal("attribute digest is empty")
	}
	if got, want := sent.MD5OfMessageAttributes, "68ded19761b15af1d722a6de8d81440b"; got != want {
		t.Fatalf("MD5OfMessageAttributes = %q, want canonical %q", got, want)
	}

	attributes["Text"] = queue.MessageAttribute{DataType: "String", StringValue: "mutated"}
	binaryValue[0] = 9
	delete(attributes, "Attempt")
	stored := repository.Messages("orders")
	if len(stored) != 1 || len(stored[0].MessageAttributes) != 3 || stored[0].MessageAttributes["Text"].StringValue != "hello" || stored[0].MessageAttributes["Payload"].BinaryValue[0] != 1 {
		t.Fatalf("caller mutation changed stored attributes: %#v", stored)
	}

	stored[0].MessageAttributes["Payload"].BinaryValue[0] = 8
	stored[0].MessageAttributes["Text"] = queue.MessageAttribute{DataType: "String", StringValue: "changed"}
	again := repository.Messages("orders")
	if again[0].MessageAttributes["Payload"].BinaryValue[0] != 1 || again[0].MessageAttributes["Text"].StringValue != "hello" {
		t.Fatalf("repository result aliases stored attributes: %#v", again[0].MessageAttributes)
	}
}

func TestMessageAttributeDigestDoesNotDependOnMapOrder(t *testing.T) {
	first := map[string]queue.MessageAttribute{
		"A": {DataType: "String", StringValue: "one"},
		"B": {DataType: "Binary", BinaryValue: []byte{2, 3}},
	}
	second := map[string]queue.MessageAttribute{
		"B": {DataType: "Binary", BinaryValue: []byte{2, 3}},
		"A": {DataType: "String", StringValue: "one"},
	}
	firstDigest, err := queue.MessageAttributesMD5(first)
	if err != nil {
		t.Fatalf("first digest: %v", err)
	}
	secondDigest, err := queue.MessageAttributesMD5(second)
	if err != nil {
		t.Fatalf("second digest: %v", err)
	}
	if firstDigest == "" || firstDigest != secondDigest {
		t.Fatalf("digests = %q and %q", firstDigest, secondDigest)
	}
}

func TestSendMessageAttributeValidationAndSizeBoundaries(t *testing.T) {
	tests := []struct {
		name       string
		attributes map[string]queue.MessageAttribute
	}{
		{name: "reserved AWS prefix", attributes: map[string]queue.MessageAttribute{"aWs.Secret": {DataType: "String", StringValue: "x"}}},
		{name: "reserved Amazon prefix", attributes: map[string]queue.MessageAttribute{"AMAZON.Secret": {DataType: "String", StringValue: "x"}}},
		{name: "leading period", attributes: map[string]queue.MessageAttribute{".Bad": {DataType: "String", StringValue: "x"}}},
		{name: "trailing period", attributes: map[string]queue.MessageAttribute{"Bad.": {DataType: "String", StringValue: "x"}}},
		{name: "consecutive periods", attributes: map[string]queue.MessageAttribute{"Bad..Name": {DataType: "String", StringValue: "x"}}},
		{name: "invalid name character", attributes: map[string]queue.MessageAttribute{"Bad Name": {DataType: "String", StringValue: "x"}}},
		{name: "name too long", attributes: map[string]queue.MessageAttribute{strings.Repeat("A", 257): {DataType: "String", StringValue: "x"}}},
		{name: "unknown base", attributes: map[string]queue.MessageAttribute{"A": {DataType: "Boolean", StringValue: "true"}}},
		{name: "empty suffix", attributes: map[string]queue.MessageAttribute{"A": {DataType: "String.", StringValue: "x"}}},
		{name: "data type too long", attributes: map[string]queue.MessageAttribute{"A": {DataType: "String." + strings.Repeat("x", 250), StringValue: "x"}}},
		{name: "string with binary", attributes: map[string]queue.MessageAttribute{"A": {DataType: "String", BinaryValue: []byte{1}}}},
		{name: "binary with string", attributes: map[string]queue.MessageAttribute{"A": {DataType: "Binary", StringValue: "AQ=="}}},
		{name: "empty string", attributes: map[string]queue.MessageAttribute{"A": {DataType: "String"}}},
		{name: "empty binary", attributes: map[string]queue.MessageAttribute{"A": {DataType: "Binary", BinaryValue: []byte{}}}},
		{name: "invalid number", attributes: map[string]queue.MessageAttribute{"A": {DataType: "Number", StringValue: "NaN"}}},
		{name: "number precision", attributes: map[string]queue.MessageAttribute{"A": {DataType: "Number", StringValue: strings.Repeat("9", 39)}}},
		{name: "number magnitude low", attributes: map[string]queue.MessageAttribute{"A": {DataType: "Number", StringValue: "1e-129"}}},
		{name: "number magnitude high", attributes: map[string]queue.MessageAttribute{"A": {DataType: "Number", StringValue: "1.1e126"}}},
	}
	for _, number := range []string{"0", "-1e-128", "9.99e-128", "1e126", strings.Repeat("9", 38)} {
		service, _, _ := newReceiveTestService(t, "30")
		if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body", MessageAttributes: map[string]queue.MessageAttribute{"Number": {DataType: "Number.decimal", StringValue: number}}}); err != nil {
			t.Fatalf("valid number %q: %v", number, err)
		}
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, _, _ := newReceiveTestService(t, "30")
			_, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body", MessageAttributes: test.attributes})
			var invalid *queue.InvalidRequestError
			if !errors.As(err, &invalid) {
				t.Fatalf("SendMessage error = %v, want InvalidRequestError", err)
			}
		})
	}

	service, repository, _ := newReceiveTestService(t, "30")
	ten := make(map[string]queue.MessageAttribute, 10)
	for index := 0; index < 10; index++ {
		ten[string(rune('A'+index))] = queue.MessageAttribute{DataType: "String", StringValue: "x"}
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "x", MessageAttributes: ten}); err != nil {
		t.Fatalf("ten attributes: %v", err)
	}
	eleven := make(map[string]queue.MessageAttribute, 11)
	for name, value := range ten {
		eleven[name] = value
	}
	eleven["K"] = queue.MessageAttribute{DataType: "String", StringValue: "x"}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "x", MessageAttributes: eleven}); err == nil {
		t.Fatal("eleven attributes accepted")
	}

	boundaryRepository := queue.NewMemoryRepository()
	boundaryService := queue.NewService(boundaryRepository, queue.WithMessageIDGenerator(func() string { return "message-boundary" }))
	mustCreateConformanceQueue(t, boundaryService, "boundary", "30")
	name := "A"
	dataType := "Binary"
	exactValue := make([]byte, queue.MaxMessageBytes-len(name)-len(dataType)-1)
	if _, err := boundaryService.SendMessage(queue.SendMessageInput{QueueName: "boundary", MessageBody: "x", MessageAttributes: map[string]queue.MessageAttribute{name: {DataType: dataType, BinaryValue: exactValue}}}); err != nil {
		t.Fatalf("exact 1 MiB message: %v", err)
	}
	tooLarge := append(append([]byte(nil), exactValue...), 1)
	if _, err := boundaryService.SendMessage(queue.SendMessageInput{QueueName: "boundary", MessageBody: "x", MessageAttributes: map[string]queue.MessageAttribute{name: {DataType: dataType, BinaryValue: tooLarge}}}); err == nil {
		t.Fatal("one-byte-over message accepted")
	}
	if repository.MessageCount("orders") != 1 || boundaryRepository.MessageCount("boundary") != 1 {
		t.Fatalf("accepted message counts = %d and %d, want 1 and 1", repository.MessageCount("orders"), boundaryRepository.MessageCount("boundary"))
	}
}

func TestReceiveMessageAttributeProjectionDoesNotMutateStoredSet(t *testing.T) {
	service, repository, _ := newReceiveTestService(t, "0")
	attrs := map[string]queue.MessageAttribute{
		"A": {DataType: "String", StringValue: "one"},
		"B": {DataType: "Binary", BinaryValue: []byte{2}},
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body", MessageAttributes: attrs}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	selected, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", MessageAttributeNames: []string{"A"}})
	if err != nil || len(selected) != 1 || len(selected[0].MessageAttributes) != 1 || selected[0].MessageAttributes["A"].StringValue != "one" || selected[0].MD5OfMessageAttributes == "" {
		t.Fatalf("selected receive = %#v, %v", selected, err)
	}
	selected[0].MessageAttributes["A"] = queue.MessageAttribute{DataType: "String", StringValue: "mutated"}
	all, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", MessageAttributeNames: []string{"All"}})
	if err != nil || len(all) != 1 || len(all[0].MessageAttributes) != 2 || all[0].MessageAttributes["A"].StringValue != "one" {
		t.Fatalf("all receive = %#v, %v", all, err)
	}
	stored := repository.Messages("orders")
	if len(stored[0].MessageAttributes) != 2 || stored[0].MessageAttributes["A"].StringValue != "one" {
		t.Fatalf("projection changed stored set: %#v", stored)
	}

	for _, names := range [][]string{{"All", "A"}, {"A", "A"}, {".*"}, {"A.*"}, {""}} {
		if _, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", MessageAttributeNames: names}); err == nil {
			t.Fatalf("invalid projection %#v accepted", names)
		}
	}
}
