package smoke_tests

import (
	"context"
	"encoding/xml"
	"net/http"
	"testing"
	"time"

	af "github.com/Admiral-Piett/goaws/app/fixtures"
	"github.com/Admiral-Piett/goaws/app/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/gavv/httpexpect/v2"
	"github.com/stretchr/testify/assert"
)

func Test_SendMessageV1_json_no_attributes(t *testing.T) {
	server := generateServer()
	defer func() {
		server.Close()
		models.ResetResources()
	}()

	sdkConfig, _ := config.LoadDefaultConfig(context.TODO())
	sdkConfig.BaseEndpoint = aws.String(server.URL)
	sqsClient := sqs.NewFromConfig(sdkConfig)
	sdkResponse, _ := sqsClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
		QueueName: &af.QueueName,
	})
	targetQueueUrl := sdkResponse.QueueUrl

	// Assert no messages in the queue before sending message
	getQueueAttributeOutput, _ := sqsClient.GetQueueAttributes(context.TODO(), &sqs.GetQueueAttributesInput{
		QueueUrl: targetQueueUrl,
	})
	assert.Equal(t, "0", getQueueAttributeOutput.Attributes["ApproximateNumberOfMessages"])

	// Target test: send a message
	targetMessageBody := "Test_SendMessageV1_json_no_attributes"
	sendMessageOutput, _ := sqsClient.SendMessage(context.TODO(), &sqs.SendMessageInput{
		QueueUrl:    targetQueueUrl,
		MessageBody: &targetMessageBody,
	})
	assert.NotNil(t, sendMessageOutput.MessageId)

	// Assert 1 message in the queue
	getQueueAttributeOutput, _ = sqsClient.GetQueueAttributes(context.TODO(), &sqs.GetQueueAttributesInput{
		QueueUrl: targetQueueUrl,
	})
	assert.Equal(t, "1", getQueueAttributeOutput.Attributes["ApproximateNumberOfMessages"])

	// Receive message and check attribute
	r3, _ := sqsClient.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
		QueueUrl: targetQueueUrl,
	})
	message := r3.Messages[0]
	assert.Equal(t, targetMessageBody, string(*message.Body))
	assert.Equal(t, 0, len(message.MessageAttributes))
}

func Test_SendMessageV1_json_with_attributes(t *testing.T) {
	server := generateServer()
	defer func() {
		server.Close()
		models.ResetResources()
	}()

	sdkConfig, _ := config.LoadDefaultConfig(context.TODO())
	sdkConfig.BaseEndpoint = aws.String(server.URL)
	sqsClient := sqs.NewFromConfig(sdkConfig)
	sdkResponse, _ := sqsClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
		QueueName: &af.QueueName,
	})
	targetQueueUrl := sdkResponse.QueueUrl

	// Target test: send a message
	targetMessageBody := "Test_SendMessageV1_json_with_attributes"
	attr1_dataType := "String"
	attr1_value := "attr1_value"
	attr2_dataType := "Number"
	attr2_value := "2"
	attr3_dataType := "Binary"
	attr3_value := []byte("attr3_value")
	sendMessageOutput, _ := sqsClient.SendMessage(context.TODO(), &sqs.SendMessageInput{
		QueueUrl:     targetQueueUrl,
		MessageBody:  &targetMessageBody,
		DelaySeconds: 1,
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"attr1": {
				DataType:    &attr1_dataType,
				StringValue: &attr1_value,
			},
			"attr2": {
				DataType:    &attr2_dataType,
				StringValue: &attr2_value,
			},
			"attr3": {
				DataType:    &attr3_dataType,
				BinaryValue: attr3_value,
			},
		},
	})
	assert.NotNil(t, sendMessageOutput.MessageId)

	// Wait for DelaySecond
	time.Sleep(1 * time.Second)

	// Assert 1 message in the queue
	getQueueAttributeOutput, _ := sqsClient.GetQueueAttributes(context.TODO(), &sqs.GetQueueAttributesInput{
		QueueUrl: targetQueueUrl,
	})
	assert.Equal(t, "1", getQueueAttributeOutput.Attributes["ApproximateNumberOfMessages"])

	// Receive message and check attribute
	receivedMessages, _ := sqsClient.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
		QueueUrl: targetQueueUrl,
	})

	assert.Len(t, receivedMessages.Messages, 1)
	assert.Equal(t, targetMessageBody, *receivedMessages.Messages[0].Body)
	assert.Len(t, receivedMessages.Messages[0].MessageAttributes, 3)
	assert.Equal(t, "6703346b272d00929423e54c28b05d71", *receivedMessages.Messages[0].MD5OfBody)
	assert.Equal(t, "f371a019316cbdb918de16040b58ccc2", *receivedMessages.Messages[0].MD5OfMessageAttributes)

	assert.Len(t, receivedMessages.Messages[0].MessageAttributes, 3)
	assert.Equal(t, attr1_dataType, *receivedMessages.Messages[0].MessageAttributes["attr1"].DataType)
	assert.Equal(t, attr1_value, *receivedMessages.Messages[0].MessageAttributes["attr1"].StringValue)
	assert.Equal(t, attr2_dataType, *receivedMessages.Messages[0].MessageAttributes["attr2"].DataType)
	assert.Equal(t, attr2_value, *receivedMessages.Messages[0].MessageAttributes["attr2"].StringValue)
	assert.Equal(t, attr3_dataType, *receivedMessages.Messages[0].MessageAttributes["attr3"].DataType)
	assert.Equal(t, attr3_value, receivedMessages.Messages[0].MessageAttributes["attr3"].BinaryValue)
}

func Test_SendMessageV1_json_MaximumMessageSize_TooBig(t *testing.T) {
	server := generateServer()
	defer func() {
		server.Close()
		models.ResetResources()
	}()

	sdkConfig, _ := config.LoadDefaultConfig(context.TODO())
	sdkConfig.BaseEndpoint = aws.String(server.URL)
	sqsClient := sqs.NewFromConfig(sdkConfig)
	sdkResponse, _ := sqsClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
		QueueName: &af.QueueName,
		Attributes: map[string]string{
			"MaximumMessageSize": "1",
		},
	})
	targetQueueUrl := sdkResponse.QueueUrl

	// Target test: send a message that is bigger than MaximumMessageSize
	targetMessageBody := "Test_SendMessageV1_json_no_attributes"
	sendMessageOutput, err := sqsClient.SendMessage(context.TODO(), &sqs.SendMessageInput{
		QueueUrl:    targetQueueUrl,
		MessageBody: &targetMessageBody,
	})
	assert.Contains(t, err.Error(), "400")
	assert.Contains(t, err.Error(), "InvalidParameterValue")
	assert.Nil(t, sendMessageOutput)
}

func Test_SendMessageV1_json_QueueNotExistant(t *testing.T) {
	server := generateServer()
	defer func() {
		server.Close()
		models.ResetResources()
	}()

	sdkConfig, _ := config.LoadDefaultConfig(context.TODO())
	sdkConfig.BaseEndpoint = aws.String(server.URL)
	sqsClient := sqs.NewFromConfig(sdkConfig)

	// Target test: send a message to a queue that is not exist
	queueUrl := "http://region.host:port/accountID/not-existant-queue"
	targetMessageBody := "Test_SendMessageV1_json_no_attributes"
	sendMessageOutput, err := sqsClient.SendMessage(context.TODO(), &sqs.SendMessageInput{
		QueueUrl:    &queueUrl,
		MessageBody: &targetMessageBody,
	})
	assert.Contains(t, err.Error(), "400")
	assert.Contains(t, err.Error(), "AWS.SimpleQueueService.NonExistentQueue")
	assert.Nil(t, sendMessageOutput)
}

func Test_SendMessageV1_xml_no_attributes(t *testing.T) {
	server := generateServer()
	defer func() {
		server.Close()
		models.ResetResources()
	}()

	sdkConfig, _ := config.LoadDefaultConfig(context.TODO())
	sdkConfig.BaseEndpoint = aws.String(server.URL)
	sqsClient := sqs.NewFromConfig(sdkConfig)
	sdkResponse, _ := sqsClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
		QueueName: &af.QueueName,
	})
	targetQueueUrl := sdkResponse.QueueUrl

	// Assert no messages in the queue before sending message
	getQueueAttributeOutput, _ := sqsClient.GetQueueAttributes(context.TODO(), &sqs.GetQueueAttributesInput{
		QueueUrl: targetQueueUrl,
	})
	assert.Equal(t, "0", getQueueAttributeOutput.Attributes["ApproximateNumberOfMessages"])

	// Target test: send a message
	sendMessageXML := struct {
		Action      string `xml:"Action"`
		Version     string `xml:"Version"`
		QueueUrl    string `xml:"QueueUrl"`
		MessageBody string `xml:"MessageBody"`
	}{
		Action:      "SendMessage",
		Version:     "2012-11-05",
		QueueUrl:    *targetQueueUrl,
		MessageBody: "Test Message",
	}
	e := httpexpect.Default(t, server.URL)
	r := e.POST("/").
		WithForm(sendMessageXML).
		Expect().
		Status(http.StatusOK).
		Body().Raw()
	r3 := models.SendMessageResult{}
	xml.Unmarshal([]byte(r), &r3)
	assert.NotNil(t, r3.MessageId)

	// Assert 1 message in the queue
	getQueueAttributeOutput, _ = sqsClient.GetQueueAttributes(context.TODO(), &sqs.GetQueueAttributesInput{
		QueueUrl: targetQueueUrl,
	})
	assert.Equal(t, "1", getQueueAttributeOutput.Attributes["ApproximateNumberOfMessages"])
}

func Test_SendMessageV1_xml_with_attributes(t *testing.T) {
	server := generateServer()
	defer func() {
		server.Close()
		models.ResetResources()
	}()

	sdkConfig, _ := config.LoadDefaultConfig(context.TODO())
	sdkConfig.BaseEndpoint = aws.String(server.URL)
	sqsClient := sqs.NewFromConfig(sdkConfig)
	sdkResponse, _ := sqsClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
		QueueName: &af.QueueName,
	})
	targetQueueUrl := sdkResponse.QueueUrl

	// Assert no messages in the queue before sending message
	getQueueAttributeOutput, _ := sqsClient.GetQueueAttributes(context.TODO(), &sqs.GetQueueAttributesInput{
		QueueUrl: targetQueueUrl,
	})
	assert.Equal(t, "0", getQueueAttributeOutput.Attributes["ApproximateNumberOfMessages"])

	// Target test: send a message
	sendMessageXML := struct {
		Action       string `xml:"Action"`
		Version      string `xml:"Version"`
		QueueUrl     string `xml:"QueueUrl"`
		MessageBody  string `xml:"MessageBody"`
		DelaySeconds string `xml:"DelaySeconds"`
	}{
		Action:       "SendMessage",
		Version:      "2012-11-05",
		QueueUrl:     *targetQueueUrl,
		MessageBody:  "Test Message",
		DelaySeconds: "1",
	}
	e := httpexpect.Default(t, server.URL)
	r := e.POST("/").
		WithForm(sendMessageXML).
		WithFormField("MessageAttribute.1.Name", "attr1").
		WithFormField("MessageAttribute.1.Value.DataType", "String").
		WithFormField("MessageAttribute.1.Value.StringValue", "attr1_value").
		WithFormField("MessageAttribute.2.Name", "attr2").
		WithFormField("MessageAttribute.2.Value.DataType", "Number").
		WithFormField("MessageAttribute.2.Value.StringValue", "2").
		WithFormField("MessageAttribute.3.Name", "attr3").
		WithFormField("MessageAttribute.3.Value.DataType", "Binary").
		WithFormField("MessageAttribute.3.Value.BinaryValue", "YXR0cjNfdmFsdWU="). // base64 encode string attr3_value
		Expect().
		Status(http.StatusOK).
		Body().Raw()
	r3 := models.SendMessageResult{}
	xml.Unmarshal([]byte(r), &r3)
	assert.NotNil(t, r3.MessageId)

	// Wait for DelaySecond
	time.Sleep(1 * time.Second)

	receivedMessages, _ := sqsClient.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
		QueueUrl:            &af.QueueUrl,
		MaxNumberOfMessages: 10,
	})

	assert.Len(t, receivedMessages.Messages, 1)
	assert.Equal(t, "Test Message", *receivedMessages.Messages[0].Body)
	assert.Len(t, receivedMessages.Messages[0].MessageAttributes, 3)
	assert.Equal(t, "d1d4180b7e411c4be86b00fb2ee103eb", *receivedMessages.Messages[0].MD5OfBody)
	assert.Equal(t, "f371a019316cbdb918de16040b58ccc2", *receivedMessages.Messages[0].MD5OfMessageAttributes)

	assert.Len(t, receivedMessages.Messages[0].MessageAttributes, 3)
	assert.Equal(t, "String", *receivedMessages.Messages[0].MessageAttributes["attr1"].DataType)
	assert.Equal(t, "attr1_value", *receivedMessages.Messages[0].MessageAttributes["attr1"].StringValue)
	assert.Equal(t, "Number", *receivedMessages.Messages[0].MessageAttributes["attr2"].DataType)
	assert.Equal(t, "2", *receivedMessages.Messages[0].MessageAttributes["attr2"].StringValue)
	assert.Equal(t, "Binary", *receivedMessages.Messages[0].MessageAttributes["attr3"].DataType)
	assert.Equal(t, []uint8("attr3_value"), receivedMessages.Messages[0].MessageAttributes["attr3"].BinaryValue)
}
