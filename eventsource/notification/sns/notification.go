package notification

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"

	"github.com/SKF/go-eventsource/eventsource"
)

type snsNotification struct {
	topic string
	sns   *sns.SNS
}

// NewSNSNotificationService creates an SNS topic
func NewSNSNotificationService(topic string) eventsource.NotificationService {
	snsClient := sns.New(
		session.Must(session.NewSession()),
	)
	return &snsNotification{topic, snsClient}
}

func (sn *snsNotification) SendNotification(record eventsource.Record) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	input := sns.PublishInput{
		TopicArn: &sn.topic,
		Message:  aws.String(string(data)),
	}

	if _, err = sn.sns.Publish(&input); err != nil {
		return err
	}
	return nil
}
