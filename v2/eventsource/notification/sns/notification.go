package notification

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/SKF/go-eventsource/v2/eventsource"
)

type snsNotification struct {
	topicARN string
	sns      *sns.Client
}

// New connection to the given SNS topic ARN, using the provided SNS client.
func NewWithClient(topicARN string, client *sns.Client) eventsource.NotificationService {
	return &snsNotification{topicARN, client}
}

func (sn *snsNotification) Send(record eventsource.Record) error {
	return sn.SendWithContext(context.Background(), record)
}

func (sn *snsNotification) SendWithContext(ctx context.Context, record eventsource.Record) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	input := sns.PublishInput{
		TopicArn: &sn.topicARN,
		Message:  aws.String(string(data)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"SKF.Hierarchy.EventType": {
				DataType:    aws.String("String"),
				StringValue: aws.String(record.Type),
			},
			"SKF.Hierarchy.Aggregate": {
				DataType:    aws.String("String"),
				StringValue: aws.String(record.AggregateID),
			},
		},
	}

	_, err = sn.sns.Publish(ctx, &input)

	return err
}
