package smoke_tests

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"testing"

	af "github.com/Admiral-Piett/goaws/app/fixtures"
	"github.com/Admiral-Piett/goaws/app/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/gavv/httpexpect/v2"
	"github.com/stretchr/testify/assert"
)

func Test_SendMessageBatchV1_Json_Error_Queue_Not_Found(t *testing.T) {
	server := generateServer()
	defer func() {
		server.Close()
		models.ResetResources()
	}()

	sdkConfig, _ := config.LoadDefaultConfig(context.TODO())
	sdkConfig.BaseEndpoint = aws.String(server.URL)
	sqsClient := sqs.NewFromConfig(sdkConfig)

	testId := "test-msg"
	messageBody := "test%20message%20body%201"
	queueUrl := fmt.Sprintf("%s/%s", af.BASE_URL, af.QueueName)

	SendMessageBatchOutput, error := sqsClient.SendMessageBatch(context.TODO(), &sqs.SendMessageBatchInput{
		Entries: []types.SendMessageBatchRequestEntry{
			{
				Id:          &testId,
				MessageBody: &messageBody,
			},
		},
		QueueUrl: &queueUrl,
	})

	assert.Contains(t, error.Error(), "400")
	assert.Contains(t, error.Error(), "AWS.SimpleQueueService.NonExistentQueue")
	assert.Contains(t, error.Error(), "The specified queue does not exist for this wsdl version.")
	assert.Nil(t, SendMessageBatchOutput)

}

func Test_SendMessageBatchV1_Json_Error_No_Entry(t *testing.T) {
	server := generateServer()
	defer func() {
		server.Close()
		models.ResetResources()
	}()

	sdkConfig, _ := config.LoadDefaultConfig(context.TODO())
	sdkConfig.BaseEndpoint = aws.String(server.URL)
	sqsClient := sqs.NewFromConfig(sdkConfig)
	queueUrl := fmt.Sprintf("%s/%s", af.BASE_URL, af.QueueName)

	sqsClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
		QueueName: &af.QueueName,
	})

	SendMessageBatchOutput, error := sqsClient.SendMessageBatch(context.TODO(), &sqs.SendMessageBatchInput{
		Entries:  make([]types.SendMessageBatchRequestEntry, 0),
		QueueUrl: &queueUrl,
	})

	assert.Contains(t, error.Error(), "400")
	assert.Contains(t, error.Error(), "AWS.SimpleQueueService.EmptyBatchRequest")
	assert.Contains(t, error.Error(), "The batch request doesn't contain any entries.")
	assert.Nil(t, SendMessageBatchOutput)

}

func TestSendMessageBatchV1_Json_Error_IdNotDistinct(t *testing.T) {
	server := generateServer()
	defer func() {
		server.Close()
		models.ResetResources()
	}()

	sdkConfig, _ := config.LoadDefaultConfig(context.TODO())
	sdkConfig.BaseEndpoint = aws.String(server.URL)
	sqsClient := sqs.NewFromConfig(sdkConfig)
	queueUrl := fmt.Sprintf("%s/%s", af.BASE_URL, af.QueueName)

	sqsClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
		QueueName: &af.QueueName,
	})

	duplicatedId := "test_msg_001"
	messageBody1 := "test%20message%20body%201"
	messageBody2 := "test%20message%20body%202"
	messageBody3 := "test%20message%20body%203"

	SendMessageBatchOutput, error := sqsClient.SendMessageBatch(context.TODO(), &sqs.SendMessageBatchInput{
		Entries: []types.SendMessageBatchRequestEntry{
			{
				Id:          &duplicatedId,
				MessageBody: &messageBody1,
			},
			{
				Id:          &duplicatedId,
				MessageBody: &messageBody2,
			},
			{
				Id:          &duplicatedId,
				MessageBody: &messageBody3,
			},
		},
		QueueUrl: &queueUrl,
	})

	assert.Contains(t, error.Error(), "400")
	assert.Contains(t, error.Error(), "AWS.SimpleQueueService.BatchEntryIdsNotDistinct")
	assert.Contains(t, error.Error(), "Two or more batch entries in the request have the same Id.")
	assert.Nil(t, SendMessageBatchOutput)
}

func TestSendMessageBatchV1_Json_Error_TooManyEntries(t *testing.T) {
	server := generateServer()
	defer func() {
		server.Close()
		models.ResetResources()
	}()

	sdkConfig, _ := config.LoadDefaultConfig(context.TODO())
	sdkConfig.BaseEndpoint = aws.String(server.URL)
	sqsClient := sqs.NewFromConfig(sdkConfig)
	queueUrl := fmt.Sprintf("%s/%s", af.BASE_URL, af.QueueName)

	sqsClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
		QueueName: &af.QueueName,
	})

	messageId1 := "test_msg_001"
	messageId2 := "test_msg_002"
	messageId3 := "test_msg_003"
	messageId4 := "test_msg_004"
	messageId5 := "test_msg_005"
	messageId6 := "test_msg_006"
	messageId7 := "test_msg_007"
	messageId8 := "test_msg_008"
	messageId9 := "test_msg_009"
	messageId10 := "test_msg_010"
	messageId11 := "test_msg_011"

	messageBody1 := "test%20message%20body%201"
	messageBody2 := "test%20message%20body%202"
	messageBody3 := "test%20message%20body%203"
	messageBody4 := "test%20message%20body%204"
	messageBody5 := "test%20message%20body%205"
	messageBody6 := "test%20message%20body%206"
	messageBody7 := "test%20message%20body%207"
	messageBody8 := "test%20message%20body%208"
	messageBody9 := "test%20message%20body%209"
	messageBody10 := "test%20message%20body%210"
	messageBody11 := "test%20message%20body%211"

	SendMessageBatchOutput, error := sqsClient.SendMessageBatch(context.TODO(), &sqs.SendMessageBatchInput{
		Entries: []types.SendMessageBatchRequestEntry{
			{
				Id:          &messageId1,
				MessageBody: &messageBody1,
			},
			{
				Id:          &messageId2,
				MessageBody: &messageBody2,
			},
			{
				Id:          &messageId3,
				MessageBody: &messageBody3,
			},
			{
				Id:          &messageId4,
				MessageBody: &messageBody4,
			},
			{
				Id:          &messageId5,
				MessageBody: &messageBody5,
			},
			{
				Id:          &messageId6,
				MessageBody: &messageBody6,
			},
			{
				Id:          &messageId7,
				MessageBody: &messageBody7,
			},
			{
				Id:          &messageId8,
				MessageBody: &messageBody8,
			},
			{
				Id:          &messageId9,
				MessageBody: &messageBody9,
			},
			{
				Id:          &messageId10,
				MessageBody: &messageBody10,
			},
			{
				Id:          &messageId11,
				MessageBody: &messageBody11,
			},
		},
		QueueUrl: &queueUrl,
	})

	assert.Contains(t, error.Error(), "400")
	assert.Contains(t, error.Error(), "AWS.SimpleQueueService.TooManyEntriesInBatchRequest")
	assert.Contains(t, error.Error(), "Maximum number of entries per request are 10.")
	assert.Nil(t, SendMessageBatchOutput)
}

func TestSendMessageBatchV1_Json_Success_including_attributes(t *testing.T) {
	server := generateServer()
	defer func() {
		server.Close()
		models.ResetResources()
	}()

	sdkConfig, _ := config.LoadDefaultConfig(context.TODO())
	sdkConfig.BaseEndpoint = aws.String(server.URL)
	sqsClient := sqs.NewFromConfig(sdkConfig)
	queueUrl := fmt.Sprintf("%s/%s", af.BASE_URL, af.QueueName)

	sqsClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
		QueueName: &af.QueueName,
	})

	messageId1 := "test_msg_001"
	messageId2 := "test_msg_002"

	messageBody1 := "test%20message%20body%201"
	messageBody2 := "test%20message%20body%202"

	binaryAttributeKey := "binary-key"
	stringAttributeKey := "string-key"
	numberAttributeKey := "number-key"

	binaryType := "Binary"
	stringType := "String"
	numberType := "Number"

	binaryValue := "binary-value"
	stringValue := "string-value"
	numberValue := "100"

	sendMessageBatchOutput, error := sqsClient.SendMessageBatch(context.TODO(), &sqs.SendMessageBatchInput{
		Entries: []types.SendMessageBatchRequestEntry{
			{
				Id:          &messageId1,
				MessageBody: &messageBody1,
			},
			{
				Id:          &messageId2,
				MessageBody: &messageBody2,
				MessageAttributes: map[string]types.MessageAttributeValue{
					binaryAttributeKey: {
						BinaryValue: []byte(binaryValue),
						DataType:    &binaryType,
					},
					stringAttributeKey: {
						DataType:    &stringType,
						StringValue: &stringValue,
					},
					numberAttributeKey: {
						DataType:    &numberType,
						StringValue: &numberValue,
					},
				},
			},
		},
		QueueUrl: &queueUrl,
	})

	assert.NotNil(t, sendMessageBatchOutput)
	assert.Nil(t, error)

	getQueueAttributeOutput, _ := sqsClient.GetQueueAttributes(context.TODO(), &sqs.GetQueueAttributesInput{
		QueueUrl: &queueUrl,
	})
	assert.Equal(t, "2", getQueueAttributeOutput.Attributes["ApproximateNumberOfMessages"])

	receiveMessageOutput, err := sqsClient.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
		QueueUrl:            &queueUrl,
		MaxNumberOfMessages: 10,
	})

	assert.Len(t, receiveMessageOutput.Messages, 2)
	assert.Nil(t, err)

	receivedMessage1 := receiveMessageOutput.Messages[0]
	receivedMessage2 := receiveMessageOutput.Messages[1]

	assert.Equal(t, messageBody1, *receivedMessage1.Body)
	assert.Len(t, receivedMessage1.MessageAttributes, 0)
	assert.Equal(t, "1c538b76fce1a234bce865025c02b042", *receivedMessage1.MD5OfBody)
	assert.Nil(t, receivedMessage1.MD5OfMessageAttributes)

	assert.Equal(t, messageBody2, *receivedMessage2.Body)
	assert.Len(t, receivedMessage2.MessageAttributes, 3)
	assert.Equal(t, "58bdcfd42148396616e4260421a9b4e5", *receivedMessage2.MD5OfBody)
	assert.Equal(t, "ddfbe54b92058bf5b5f00055fa2032a5", *receivedMessage2.MD5OfMessageAttributes)

	assert.Len(t, receivedMessage2.MessageAttributes, 3)
	assert.Equal(t, stringType, *receivedMessage2.MessageAttributes[stringAttributeKey].DataType)
	assert.Equal(t, stringValue, *receivedMessage2.MessageAttributes[stringAttributeKey].StringValue)
	assert.Equal(t, numberType, *receivedMessage2.MessageAttributes[numberAttributeKey].DataType)
	assert.Equal(t, numberValue, *receivedMessage2.MessageAttributes[numberAttributeKey].StringValue)
	assert.Equal(t, binaryType, *receivedMessage2.MessageAttributes[binaryAttributeKey].DataType)
	assert.Equal(t, []uint8(binaryValue), receivedMessage2.MessageAttributes[binaryAttributeKey].BinaryValue)
}

func TestSendMessageBatchV1_Xml_Success_including_attributes(t *testing.T) {
	server := generateServer()
	defer func() {
		server.Close()
		models.ResetResources()
	}()

	sdkConfig, _ := config.LoadDefaultConfig(context.TODO())
	sdkConfig.BaseEndpoint = aws.String(server.URL)
	sqsClient := sqs.NewFromConfig(sdkConfig)
	sqsClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
		QueueName: &af.QueueName,
	})

	e := httpexpect.Default(t, server.URL)

	messageId1 := "test_msg_001"
	messageId2 := "test_msg_002"

	messageBody1 := "test%20message%20body%201"
	messageBody2 := "test%20message%20body%202"

	binaryAttributeKey := "binary-key"
	stringAttributeKey := "string-key"
	numberAttributeKey := "number-key"

	binaryType := "Binary"
	stringType := "String"
	numberType := "Number"

	binaryValue := []byte("binary-value")
	binaryValueEncodeString := base64.StdEncoding.EncodeToString([]byte("binary-value"))
	stringValue := "string-value"
	numberValue := "100"

	// Target test: send a message
	sendMessageBatchXML := struct {
		Action   string `xml:"Action"`
		Version  string `xml:"Version"`
		QueueUrl string `xml:"QueueUrl"`
	}{
		Action:   "SendMessageBatch",
		Version:  "2012-11-05",
		QueueUrl: af.QueueUrl,
	}

	r := e.POST("/").WithForm(sendMessageBatchXML).
		WithFormField("Entries.0.Id", messageId1).
		WithFormField("Entries.0.MessageBody", messageBody1).
		WithFormField("Entries.1.Id", messageId2).
		WithFormField("Entries.1.MessageBody", messageBody2).
		WithFormField("Entries.1.MessageAttributes.1.Name", binaryAttributeKey).
		WithFormField("Entries.1.MessageAttributes.1.Value.DataType", binaryType).
		WithFormField("Entries.1.MessageAttributes.1.Value.BinaryValue", binaryValueEncodeString).
		WithFormField("Entries.1.MessageAttributes.2.Name", stringAttributeKey).
		WithFormField("Entries.1.MessageAttributes.2.Value.DataType", stringType).
		WithFormField("Entries.1.MessageAttributes.2.Value.StringValue", stringValue).
		WithFormField("Entries.1.MessageAttributes.3.Name", numberAttributeKey).
		WithFormField("Entries.1.MessageAttributes.3.Value.DataType", numberType).
		WithFormField("Entries.1.MessageAttributes.3.Value.StringValue", numberValue).
		Expect().
		Status(http.StatusOK).
		Body().Raw()

	response := models.SendMessageBatchResponse{}

	xml.Unmarshal([]byte(r), &response)

	assert.NotNil(t, response.Result.Entry[0].MessageId)

	// Assert 1 message in the queue
	getQueueAttributeOutput, _ := sqsClient.GetQueueAttributes(context.TODO(), &sqs.GetQueueAttributesInput{
		QueueUrl: &af.QueueUrl,
	})
	assert.Equal(t, "2", getQueueAttributeOutput.Attributes["ApproximateNumberOfMessages"])

	receiveMessageOutput, err := sqsClient.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
		QueueUrl:            &af.QueueUrl,
		MaxNumberOfMessages: 10,
	})

	assert.Len(t, receiveMessageOutput.Messages, 2)
	assert.Nil(t, err)

	receivedMessage1 := receiveMessageOutput.Messages[0]
	receivedMessage2 := receiveMessageOutput.Messages[1]

	assert.Equal(t, messageBody1, *receivedMessage1.Body)
	assert.Len(t, receivedMessage1.MessageAttributes, 0)
	assert.Equal(t, "1c538b76fce1a234bce865025c02b042", *receivedMessage1.MD5OfBody)
	assert.Nil(t, receivedMessage1.MD5OfMessageAttributes)

	assert.Equal(t, messageBody2, *receivedMessage2.Body)
	assert.Len(t, receivedMessage2.MessageAttributes, 3)
	assert.Equal(t, "58bdcfd42148396616e4260421a9b4e5", *receivedMessage2.MD5OfBody)
	assert.Equal(t, "ddfbe54b92058bf5b5f00055fa2032a5", *receivedMessage2.MD5OfMessageAttributes)

	assert.Len(t, receivedMessage2.MessageAttributes, 3)
	assert.Equal(t, stringType, *receivedMessage2.MessageAttributes[stringAttributeKey].DataType)
	assert.Equal(t, stringValue, *receivedMessage2.MessageAttributes[stringAttributeKey].StringValue)
	assert.Equal(t, numberType, *receivedMessage2.MessageAttributes[numberAttributeKey].DataType)
	assert.Equal(t, numberValue, *receivedMessage2.MessageAttributes[numberAttributeKey].StringValue)
	assert.Equal(t, binaryType, *receivedMessage2.MessageAttributes[binaryAttributeKey].DataType)
	assert.Equal(t, []uint8(binaryValue), receivedMessage2.MessageAttributes[binaryAttributeKey].BinaryValue)
}
