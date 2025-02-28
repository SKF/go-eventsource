package notification

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"

	"github.com/SKF/go-eventsource/eventsource"
)

type snsNotification struct {
	sns      *sns.SNS
	topicARN string
}

// NewSNSNotificationService connection to the given SNS topic ARN
func NewSNSNotificationService(topicARN string) eventsource.NotificationService {
	snsClient := sns.New(
		session.Must(session.NewSession()),
	)
	return &snsNotification{topicARN: topicARN, sns: snsClient}
}

func (sn *snsNotification) SendNotification(record eventsource.Record) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	input := sns.PublishInput{
		TopicArn: &sn.topicARN,
		Message:  aws.String(string(data)),
	}

	if _, err = sn.sns.Publish(&input); err != nil {
		return err
	}

	return nil
}
