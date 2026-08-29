package sqssemantics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type AWSOptions struct {
	Region         string
	AllowMutations bool
}

type AWSBackend struct {
	client *sqs.Client
}

func NewAWSBackend(ctx context.Context, options AWSOptions) (*AWSBackend, error) {
	if !options.AllowMutations {
		return nil, errors.New("AWS backend requires explicit mutation opt-in")
	}
	region := strings.TrimSpace(options.Region)
	if region == "" {
		return nil, errors.New("AWS backend requires an explicit region")
	}
	loaded, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, errors.New("load external AWS configuration failed")
	}
	return &AWSBackend{client: sqs.NewFromConfig(loaded)}, nil
}

func (*AWSBackend) Name() string { return "aws-sqs" }

func (b *AWSBackend) CreateQueue(ctx context.Context, name string, options QueueOptions) (Queue, error) {
	attributes := map[string]string{"VisibilityTimeout": strconv.Itoa(seconds(options.VisibilityTimeout))}
	if options.FIFO {
		attributes["FifoQueue"] = "true"
	}
	if options.ContentBasedDeduplication {
		attributes["ContentBasedDeduplication"] = "true"
	}
	created, err := b.client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attributes})
	if err != nil {
		return Queue{}, awsFailure("CreateQueue", err)
	}
	if created.QueueUrl == nil || *created.QueueUrl == "" {
		return Queue{}, errors.New("AWS SQS CreateQueue omitted queue URL")
	}
	queue := Queue{URL: *created.QueueUrl}
	response, err := b.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       created.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		b.cleanupPartialQueue(queue)
		return Queue{}, awsFailure("GetQueueAttributes", err)
	}
	queue.ARN = response.Attributes[string(types.QueueAttributeNameQueueArn)]
	if queue.ARN == "" {
		b.cleanupPartialQueue(queue)
		return Queue{}, errors.New("AWS SQS GetQueueAttributes omitted queue ARN")
	}
	return queue, nil
}

func (b *AWSBackend) DeleteQueue(ctx context.Context, queue Queue) error {
	_, err := b.client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(queue.URL)})
	if err != nil {
		return awsFailure("DeleteQueue", err)
	}
	return nil
}

func (b *AWSBackend) Send(ctx context.Context, queue Queue, input SendInput) (SendResult, error) {
	request := &sqs.SendMessageInput{QueueUrl: aws.String(queue.URL), MessageBody: aws.String(input.Body)}
	if input.GroupID != "" {
		request.MessageGroupId = aws.String(input.GroupID)
	}
	if input.DeduplicationID != "" {
		request.MessageDeduplicationId = aws.String(input.DeduplicationID)
	}
	response, err := b.client.SendMessage(ctx, request)
	if err != nil {
		return SendResult{}, awsFailure("SendMessage", err)
	}
	if response.MessageId == nil || *response.MessageId == "" {
		return SendResult{}, errors.New("AWS SQS SendMessage omitted message ID")
	}
	return SendResult{MessageID: *response.MessageId}, nil
}

func (b *AWSBackend) Receive(ctx context.Context, queue Queue, input ReceiveInput) ([]Message, error) {
	request := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queue.URL),
		MaxNumberOfMessages: int32(input.MaxMessages),
		WaitTimeSeconds:     int32(seconds(input.WaitTime)),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
			types.MessageSystemAttributeNameMessageGroupId,
		},
	}
	if input.VisibilityTimeout != 0 {
		request.VisibilityTimeout = int32(seconds(input.VisibilityTimeout))
	}
	response, err := b.client.ReceiveMessage(ctx, request)
	if err != nil {
		return nil, awsFailure("ReceiveMessage", err)
	}
	messages := make([]Message, len(response.Messages))
	for index, item := range response.Messages {
		count, err := strconv.Atoi(item.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
		if err != nil {
			return nil, errors.New("AWS SQS ReceiveMessage returned invalid receive count")
		}
		messages[index] = Message{
			MessageID:     aws.ToString(item.MessageId),
			ReceiptHandle: aws.ToString(item.ReceiptHandle),
			Body:          aws.ToString(item.Body),
			ReceiveCount:  count,
			GroupID:       item.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)],
		}
	}
	return messages, nil
}

func (b *AWSBackend) Delete(ctx context.Context, queue Queue, receipt string) error {
	_, err := b.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(queue.URL), ReceiptHandle: aws.String(receipt)})
	if err != nil {
		return awsFailure("DeleteMessage", err)
	}
	return nil
}

func (b *AWSBackend) ChangeVisibility(ctx context.Context, queue Queue, receipt string, visibility time.Duration) error {
	_, err := b.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(queue.URL),
		ReceiptHandle:     aws.String(receipt),
		VisibilityTimeout: int32(seconds(visibility)),
	})
	if err != nil {
		return awsFailure("ChangeMessageVisibility", err)
	}
	return nil
}

func (b *AWSBackend) ConfigureRedrive(ctx context.Context, source, target Queue, maxReceiveCount int) error {
	policy := fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":%q}`, target.ARN, strconv.Itoa(maxReceiveCount))
	_, err := b.client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl: aws.String(source.URL),
		Attributes: map[string]string{
			"RedrivePolicy": policy,
		},
	})
	if err != nil {
		return awsFailure("SetQueueAttributes", err)
	}
	for {
		observed, getErr := b.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(source.URL),
			AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameRedrivePolicy},
		})
		if getErr != nil {
			return awsFailure("GetQueueAttributes", getErr)
		}
		if redrivePolicyMatches(observed.Attributes[string(types.QueueAttributeNameRedrivePolicy)], target.ARN, maxReceiveCount) {
			return nil
		}
		if err := sleepContext(ctx, time.Second); err != nil {
			return err
		}
	}
}

func redrivePolicyMatches(encoded, targetARN string, maxReceiveCount int) bool {
	var policy struct {
		DeadLetterTargetARN string `json:"deadLetterTargetArn"`
		MaxReceiveCount     string `json:"maxReceiveCount"`
	}
	return json.Unmarshal([]byte(encoded), &policy) == nil &&
		policy.DeadLetterTargetARN == targetARN &&
		policy.MaxReceiveCount == strconv.Itoa(maxReceiveCount)
}

func (b *AWSBackend) cleanupPartialQueue(queue Queue) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = b.client.DeleteQueue(cleanupCtx, &sqs.DeleteQueueInput{QueueUrl: aws.String(queue.URL)})
}

func awsFailure(action string, _ error) error {
	// SDK errors may include queue URLs and account IDs. Keep the comparative
	// report deliberately redacted; operators can enable SDK diagnostics in a
	// controlled environment when investigating a failing cloud run.
	return fmt.Errorf("AWS SQS %s failed", action)
}
