package gosns

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Admiral-Piett/goaws/app/interfaces"
	"github.com/Admiral-Piett/goaws/app/models"
	"github.com/Admiral-Piett/goaws/app/utils"

	"github.com/Admiral-Piett/goaws/app"
	"github.com/Admiral-Piett/goaws/app/common"
	log "github.com/sirupsen/logrus"
)

// TODO - Admiral-Piett - Pick a MessageAttribute style and get rid of `utils.ConvertToOldMessageAttributeValueStructure`

// aws --endpoint-url http://localhost:47194 sns publish --topic-arn arn:aws:sns:yopa-local:000000000000:test1 --message "This is a test"
func PublishV1(req *http.Request) (int, interfaces.AbstractResponseBody) {
	requestBody := models.NewPublishRequest()
	ok := utils.REQUEST_TRANSFORMER(requestBody, req, false)
	if !ok {
		log.Error("Invalid Request - PublishV1")
		return utils.CreateErrorResponseV1("InvalidParameterValue", false)
	}

	// TODO - support TargetArn
	if requestBody.TopicArn == "" || requestBody.Message == "" {
		return utils.CreateErrorResponseV1("InvalidParameterValue", false)
	}

	arnSegments := strings.Split(requestBody.TopicArn, ":")
	topicName := arnSegments[len(arnSegments)-1]

	_, ok = app.SyncTopics.Topics[topicName]
	if !ok {
		return utils.CreateErrorResponseV1("TopicNotFound", false)
	}

	log.WithFields(log.Fields{
		"topic":    topicName,
		"topicArn": requestBody.TopicArn,
		"subject":  requestBody.Subject,
	}).Debug("Publish to Topic")
	messageAttributes := utils.ConvertToOldMessageAttributeValueStructure(requestBody.MessageAttributes)
	for _, subscription := range app.SyncTopics.Topics[topicName].Subscriptions {
		switch app.Protocol(subscription.Protocol) {
		case app.ProtocolSQS:
			err := publishSQS(subscription, requestBody.Message, messageAttributes, requestBody.Subject, topicName, requestBody.MessageStructure)
			if err != nil {
				utils.CreateErrorResponseV1(err.Error(), false)
			}
		case app.ProtocolHTTP:
			fallthrough
		case app.ProtocolHTTPS:
			publishHTTP(subscription, requestBody.Message, messageAttributes, requestBody.Subject, requestBody.TopicArn)
		}
	}

	//Create the response
	respStruct := models.PublishResponse{
		Xmlns: models.BASE_XMLNS,
		Result: models.PublishResult{
			MessageId: uuid.NewString(),
		},
		Metadata: app.ResponseMetadata{
			RequestId: uuid.NewString(),
		},
	}
	return http.StatusOK, respStruct
}

func publishSQS(subscription *app.Subscription, message string, messageAttributes map[string]app.MessageAttributeValue, subject string, topicName string, messageStructure string) error {
	if subscription.FilterPolicy != nil && !subscription.FilterPolicy.IsSatisfiedBy(messageAttributes) {
		return nil
	}

	endPoint := subscription.EndPoint
	uriSegments := strings.Split(endPoint, "/")
	queueName := uriSegments[len(uriSegments)-1]
	arnSegments := strings.Split(queueName, ":")
	queueName = arnSegments[len(arnSegments)-1]

	if _, ok := app.SyncQueues.Queues[queueName]; !ok {
		log.Infof("Queue %s does not exist, message discarded\n", queueName)
		return nil
	}

	msg := app.Message{}

	if subscription.Raw == false {
		m, err := createMessageBody(subscription, message, subject, messageStructure, messageAttributes)
		if err != nil {
			return err
		}

		msg.MessageBody = m
	} else {
		msg.MessageAttributes = messageAttributes
		msg.MD5OfMessageAttributes = common.HashAttributes(messageAttributes)
		m, err := extractMessageFromJSON(message, subscription.Protocol)
		if err == nil {
			msg.MessageBody = []byte(m)
		} else {
			msg.MessageBody = []byte(message)
		}
	}

	msg.MD5OfMessageBody = common.GetMD5Hash(message)
	msg.Uuid = uuid.NewString()
	app.SyncQueues.Lock()
	app.SyncQueues.Queues[queueName].Messages = append(app.SyncQueues.Queues[queueName].Messages, msg)
	app.SyncQueues.Unlock()

	log.Infof("%s: Topic: %s(%s), Message: %s\n", time.Now().Format("2006-01-02 15:04:05"), topicName, queueName, msg.MessageBody)
	return nil
}

func publishHTTP(subs *app.Subscription, message string, messageAttributes map[string]app.MessageAttributeValue, subject string, topicArn string) {
	id := uuid.NewString()
	msg := app.SNSMessage{
		Type:              "Notification",
		MessageId:         id,
		Timestamp:         time.Now().UTC().Format(time.RFC3339),
		SignatureVersion:  "1",
		Message:           message,
		TopicArn:          topicArn,
		Subject:           subject,
		SigningCertURL:    fmt.Sprintf("http://%s:%s/SimpleNotificationService/%s.pem", app.CurrentEnvironment.Host, app.CurrentEnvironment.Port, id),
		UnsubscribeURL:    fmt.Sprintf("http://%s:%s/?Action=Unsubscribe&SubscriptionArn=%s", app.CurrentEnvironment.Host, app.CurrentEnvironment.Port, subs.SubscriptionArn),
		MessageAttributes: formatAttributes(messageAttributes),
	}

	signature, err := signMessage(PrivateKEY, &msg)
	if err != nil {
		log.Error(err)
	} else {
		msg.Signature = signature
	}
	err = callEndpoint(subs.EndPoint, subs.SubscriptionArn, msg, subs.Raw)
	if err != nil {
		log.WithFields(log.Fields{
			"EndPoint": subs.EndPoint,
			"ARN":      subs.SubscriptionArn,
			"error":    err.Error(),
		}).Error("Error calling endpoint")
	}
}

func createMessageBody(subs *app.Subscription, msg string, subject string, messageStructure string,
	messageAttributes map[string]app.MessageAttributeValue) ([]byte, error) {

	msgId := uuid.NewString()
	message := app.SNSMessage{
		Type:              "Notification",
		MessageId:         msgId,
		TopicArn:          subs.TopicArn,
		Subject:           subject,
		Timestamp:         time.Now().UTC().Format(time.RFC3339),
		SignatureVersion:  "1",
		SigningCertURL:    fmt.Sprintf("http://%s:%s/SimpleNotificationService/%s.pem", app.CurrentEnvironment.Host, app.CurrentEnvironment.Port, msgId),
		UnsubscribeURL:    fmt.Sprintf("http://%s:%s/?Action=Unsubscribe&SubscriptionArn=%s", app.CurrentEnvironment.Host, app.CurrentEnvironment.Port, subs.SubscriptionArn),
		MessageAttributes: formatAttributes(messageAttributes),
	}

	if app.MessageStructure(messageStructure) == app.MessageStructureJSON {
		m, err := extractMessageFromJSON(msg, subs.Protocol)
		if err != nil {
			return nil, err
		}
		message.Message = m
	} else {
		message.Message = msg
	}

	signature, err := signMessage(PrivateKEY, &message)
	if err != nil {
		log.Error(err)
	} else {
		message.Signature = signature
	}

	byteMsg, _ := json.Marshal(message)
	return byteMsg, nil
}

func formatAttributes(values map[string]app.MessageAttributeValue) map[string]app.MsgAttr {
	attr := make(map[string]app.MsgAttr)
	for k, v := range values {
		attr[k] = app.MsgAttr{
			Type:  v.DataType,
			Value: v.Value,
		}
	}
	return attr
}
