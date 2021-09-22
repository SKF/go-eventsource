package notification

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"

	"github.com/SKF/go-eventsource/v2/eventsource"
)

type snsNotification struct {
	topicARN string
	sns      *sns.SNS
}

// New connection to the given SNS topic ARN.
func New(topicARN string) eventsource.NotificationService {
	return NewWithSession(topicARN, session.Must(session.NewSession()))
}

// New connection to the given SNS topic ARN, using the provided session.
func NewWithSession(topicARN string, sess *session.Session) eventsource.NotificationService {
	return &snsNotification{topicARN, sns.New(sess)}
}

// New connection to the given SNS topic ARN, using the provided SNS client.
func NewWithClient(topicARN string, client *sns.SNS) eventsource.NotificationService {
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
		MessageAttributes: map[string]*sns.MessageAttributeValue{
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

	_, err = sn.sns.PublishWithContext(ctx, &input)

	return err
}
