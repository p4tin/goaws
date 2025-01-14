package models

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewCreateQueueRequest(t *testing.T) {
	CurrentEnvironment.QueueAttributeDefaults.MaximumMessageSize = 262144
	CurrentEnvironment.QueueAttributeDefaults.MessageRetentionPeriod = 345600
	CurrentEnvironment.QueueAttributeDefaults.ReceiveMessageWaitTimeSeconds = 10
	CurrentEnvironment.QueueAttributeDefaults.VisibilityTimeout = 30
	defer func() {
		ResetApp()
	}()

	expectedCreateQueueRequest := &CreateQueueRequest{
		Attributes: QueueAttributes{
			DelaySeconds:                  0,
			MaximumMessageSize:            262144,
			MessageRetentionPeriod:        345600,
			ReceiveMessageWaitTimeSeconds: 10,
			VisibilityTimeout:             30,
		},
	}

	result := NewCreateQueueRequest()

	assert.Equal(t, expectedCreateQueueRequest, result)
}

func TestCreateQueueRequest_SetAttributesFromForm_success(t *testing.T) {
	expectedRedrivePolicy := RedrivePolicy{
		MaxReceiveCount:     100,
		DeadLetterTargetArn: "dead-letter-queue-arn",
	}

	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "new-queue")
	form.Add("Version", "2012-11-05")
	form.Add("Attribute.1.Name", "DelaySeconds")
	form.Add("Attribute.1.Value", "1")
	form.Add("Attribute.2.Name", "MaximumMessageSize")
	form.Add("Attribute.2.Value", "2")
	form.Add("Attribute.3.Name", "MessageRetentionPeriod")
	form.Add("Attribute.3.Value", "3")
	form.Add("Attribute.4.Name", "Policy")
	form.Add("Attribute.4.Value", "{\"i-am\":\"the-policy\"}")
	form.Add("Attribute.5.Name", "ReceiveMessageWaitTimeSeconds")
	form.Add("Attribute.5.Value", "4")
	form.Add("Attribute.6.Name", "VisibilityTimeout")
	form.Add("Attribute.6.Value", "5")
	form.Add("Attribute.7.Name", "RedrivePolicy")
	form.Add("Attribute.7.Value", "{\"maxReceiveCount\": 100, \"deadLetterTargetArn\":\"dead-letter-queue-arn\"}")
	form.Add("Attribute.8.Name", "RedriveAllowPolicy")
	form.Add("Attribute.8.Value", "{\"i-am\":\"the-redrive-allow-policy\"}")

	cqr := &CreateQueueRequest{
		Attributes: QueueAttributes{
			DelaySeconds:                  1,
			MaximumMessageSize:            262144,
			MessageRetentionPeriod:        345600,
			ReceiveMessageWaitTimeSeconds: 10,
			VisibilityTimeout:             30,
		},
	}
	cqr.SetAttributesFromForm(form)

	assert.Equal(t, StringToInt(1), cqr.Attributes.DelaySeconds)
	assert.Equal(t, StringToInt(2), cqr.Attributes.MaximumMessageSize)
	assert.Equal(t, StringToInt(3), cqr.Attributes.MessageRetentionPeriod)
	assert.Equal(t, map[string]interface{}{"i-am": "the-policy"}, cqr.Attributes.Policy)
	assert.Equal(t, StringToInt(4), cqr.Attributes.ReceiveMessageWaitTimeSeconds)
	assert.Equal(t, StringToInt(5), cqr.Attributes.VisibilityTimeout)
	assert.Equal(t, expectedRedrivePolicy, cqr.Attributes.RedrivePolicy)
	assert.Equal(t, map[string]interface{}{"i-am": "the-redrive-allow-policy"}, cqr.Attributes.RedriveAllowPolicy)
}

func TestCreateQueueRequest_SetAttributesFromForm_success_handles_redrive_recieve_count_int(t *testing.T) {
	expectedRedrivePolicy := RedrivePolicy{
		MaxReceiveCount:     100,
		DeadLetterTargetArn: "dead-letter-queue-arn",
	}

	form := url.Values{}
	form.Add("Attribute.1.Name", "RedrivePolicy")
	form.Add("Attribute.1.Value", "{\"maxReceiveCount\": 100, \"deadLetterTargetArn\":\"dead-letter-queue-arn\"}")

	cqr := &CreateQueueRequest{
		Attributes: QueueAttributes{},
	}
	cqr.SetAttributesFromForm(form)

	assert.Equal(t, expectedRedrivePolicy, cqr.Attributes.RedrivePolicy)
}

func TestCreateQueueRequest_SetAttributesFromForm_success_handles_redrive_recieve_count_string(t *testing.T) {
	expectedRedrivePolicy := RedrivePolicy{
		MaxReceiveCount:     100,
		DeadLetterTargetArn: "dead-letter-queue-arn",
	}

	form := url.Values{}
	form.Add("Attribute.1.Name", "RedrivePolicy")
	form.Add("Attribute.1.Value", "{\"maxReceiveCount\": \"100\", \"deadLetterTargetArn\":\"dead-letter-queue-arn\"}")

	cqr := &CreateQueueRequest{
		Attributes: QueueAttributes{},
	}
	cqr.SetAttributesFromForm(form)

	assert.Equal(t, expectedRedrivePolicy, cqr.Attributes.RedrivePolicy)
}

func TestCreateQueueRequest_SetAttributesFromForm_success_default_unparsable_redrive_recieve_count(t *testing.T) {
	defaultRedrivePolicy := RedrivePolicy{
		MaxReceiveCount:     10,
		DeadLetterTargetArn: "dead-letter-queue-arn",
	}

	form := url.Values{}
	form.Add("Attribute.1.Name", "RedrivePolicy")
	form.Add("Attribute.1.Value", "{\"maxReceiveCount\": null, \"deadLetterTargetArn\":\"dead-letter-queue-arn\"}")

	cqr := &CreateQueueRequest{
		Attributes: QueueAttributes{},
	}
	cqr.SetAttributesFromForm(form)

	assert.Equal(t, defaultRedrivePolicy, cqr.Attributes.RedrivePolicy)
}

func TestCreateQueueRequest_SetAttributesFromForm_success_skips_invalid_values(t *testing.T) {
	form := url.Values{}
	form.Add("Attribute.1.Name", "DelaySeconds")
	form.Add("Attribute.1.Value", "garbage")
	form.Add("Attribute.2.Name", "MaximumMessageSize")
	form.Add("Attribute.2.Value", "garbage")
	form.Add("Attribute.3.Name", "MessageRetentionPeriod")
	form.Add("Attribute.3.Value", "garbage")
	form.Add("Attribute.4.Name", "Policy")
	form.Add("Attribute.4.Value", "garbage")
	form.Add("Attribute.5.Name", "ReceiveMessageWaitTimeSeconds")
	form.Add("Attribute.5.Value", "garbage")
	form.Add("Attribute.6.Name", "VisibilityTimeout")
	form.Add("Attribute.6.Value", "garbage")
	form.Add("Attribute.7.Name", "RedrivePolicy")
	form.Add("Attribute.7.Value", "garbage")
	form.Add("Attribute.8.Name", "RedriveAllowPolicy")
	form.Add("Attribute.8.Value", "garbage")

	cqr := &CreateQueueRequest{
		Attributes: QueueAttributes{
			DelaySeconds:                  1,
			MaximumMessageSize:            262144,
			MessageRetentionPeriod:        345600,
			ReceiveMessageWaitTimeSeconds: 10,
			VisibilityTimeout:             30,
		},
	}
	cqr.SetAttributesFromForm(form)

	assert.Equal(t, StringToInt(1), cqr.Attributes.DelaySeconds)
	assert.Equal(t, StringToInt(262144), cqr.Attributes.MaximumMessageSize)
	assert.Equal(t, StringToInt(345600), cqr.Attributes.MessageRetentionPeriod)
	assert.Equal(t, map[string]interface{}(nil), cqr.Attributes.Policy)
	assert.Equal(t, StringToInt(10), cqr.Attributes.ReceiveMessageWaitTimeSeconds)
	assert.Equal(t, StringToInt(30), cqr.Attributes.VisibilityTimeout)
	assert.Equal(t, RedrivePolicy{}, cqr.Attributes.RedrivePolicy)
	assert.Equal(t, map[string]interface{}(nil), cqr.Attributes.RedriveAllowPolicy)
}

func TestRedrivePolicy_UnmarshalJSON_handles_nested_json(t *testing.T) {
	request := struct {
		MaxReceiveCount     int    `json:"maxReceiveCount"`
		DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	}{
		MaxReceiveCount:     100,
		DeadLetterTargetArn: "arn:redrive-queue",
	}
	b, _ := json.Marshal(request)
	var r = RedrivePolicy{}
	err := r.UnmarshalJSON(b)

	assert.Nil(t, err)
	assert.Equal(t, StringToInt(100), r.MaxReceiveCount)
	assert.Equal(t, fmt.Sprintf("%s:%s", "arn", "redrive-queue"), r.DeadLetterTargetArn)
}

func TestRedrivePolicy_UnmarshalJSON_handles_escaped_string(t *testing.T) {
	request := `{"maxReceiveCount":"100","deadLetterTargetArn":"arn:redrive-queue"}`
	b, _ := json.Marshal(request)
	var r = RedrivePolicy{}
	err := r.UnmarshalJSON(b)

	assert.Nil(t, err)
	assert.Equal(t, StringToInt(100), r.MaxReceiveCount)
	assert.Equal(t, fmt.Sprintf("%s:%s", "arn", "redrive-queue"), r.DeadLetterTargetArn)
}

func TestRedrivePolicy_UnmarshalJSON_invalid_json_request_returns_error(t *testing.T) {
	request := fmt.Sprintf(`{\"maxReceiveCount\":\"100\",\"deadLetterTargetArn\":\"arn:redrive-queue\"}`)
	var r = RedrivePolicy{}
	err := r.UnmarshalJSON([]byte(request))

	assert.Error(t, err)
	assert.Equal(t, StringToInt(0), r.MaxReceiveCount)
	assert.Equal(t, "", r.DeadLetterTargetArn)
}

func TestRedrivePolicy_UnmarshalJSON_invalid_type_returns_error(t *testing.T) {
	request := `{"maxReceiveCount":true,"deadLetterTargetArn":"arn:redrive-queue"}`
	b, _ := json.Marshal(request)
	var r = RedrivePolicy{}
	err := r.UnmarshalJSON(b)

	assert.Error(t, err)
	assert.Equal(t, StringToInt(0), r.MaxReceiveCount)
	assert.Equal(t, "", r.DeadLetterTargetArn)
}

func TestNewListQueuesRequest_SetAttributesFromForm(t *testing.T) {
	form := url.Values{}
	form.Add("MaxResults", "1")
	form.Add("NextToken", "next-token")
	form.Add("QueueNamePrefix", "queue-name-prefix")

	lqr := &ListQueueRequest{}
	lqr.SetAttributesFromForm(form)

	assert.Equal(t, 1, lqr.MaxResults)
	assert.Equal(t, "next-token", lqr.NextToken)
	assert.Equal(t, "queue-name-prefix", lqr.QueueNamePrefix)
}

func TestListQueuesRequest_SetAttributesFromForm_invalid_max_results(t *testing.T) {
	form := url.Values{}
	form.Add("MaxResults", "1.0")
	form.Add("NextToken", "next-token")
	form.Add("QueueNamePrefix", "queue-name-prefix")

	lqr := &ListQueueRequest{}
	lqr.SetAttributesFromForm(form)

	assert.Equal(t, 0, lqr.MaxResults)
	assert.Equal(t, "next-token", lqr.NextToken)
	assert.Equal(t, "queue-name-prefix", lqr.QueueNamePrefix)
}

func TestGetQueueAttributesRequest_SetAttributesFromForm(t *testing.T) {
	form := url.Values{}
	form.Add("QueueUrl", "queue-url")
	form.Add("AttributeName.1", "attribute-1")
	form.Add("AttributeName.2", "attribute-2")

	lqr := &GetQueueAttributesRequest{}
	lqr.SetAttributesFromForm(form)

	assert.Equal(t, "queue-url", lqr.QueueUrl)
	assert.Equal(t, 2, len(lqr.AttributeNames))
	assert.Contains(t, lqr.AttributeNames, "attribute-1")
	assert.Contains(t, lqr.AttributeNames, "attribute-2")
}

func TestGetQueueAttributesRequest_SetAttributesFromForm_skips_invalid_key_sequence(t *testing.T) {
	form := url.Values{}
	form.Add("QueueUrl", "queue-url")
	form.Add("AttributeName.1", "attribute-1")
	form.Add("AttributeName.3", "attribute-3")

	lqr := &GetQueueAttributesRequest{}
	lqr.SetAttributesFromForm(form)

	assert.Equal(t, "queue-url", lqr.QueueUrl)
	assert.Equal(t, 1, len(lqr.AttributeNames))
	assert.Contains(t, lqr.AttributeNames, "attribute-1")
}

func TestSendMessageRequest_SetAttributesFromForm_success(t *testing.T) {
	form := url.Values{}
	form.Add("MessageAttribute.1.Name", "Attr1")
	form.Add("MessageAttribute.1.Value.DataType", "String")
	form.Add("MessageAttribute.1.Value.StringValue", "Value1")
	form.Add("MessageAttribute.2.Name", "Attr2")
	form.Add("MessageAttribute.2.Value.DataType", "Binary")
	form.Add("MessageAttribute.2.Value.BinaryValue", "VmFsdWUy")
	form.Add("MessageAttribute.3.Name", "")
	form.Add("MessageAttribute.3.Value.DataType", "String")
	form.Add("MessageAttribute.3.Value.StringValue", "Value")
	form.Add("MessageAttribute.4.Name", "Attr4")
	form.Add("MessageAttribute.4.Value.DataType", "")
	form.Add("MessageAttribute.4.Value.StringValue", "Value4")

	r := &SendMessageRequest{
		MessageAttributes:       make(map[string]MessageAttribute),
		MessageSystemAttributes: make(map[string]MessageAttribute),
	}
	r.SetAttributesFromForm(form)

	assert.Equal(t, 2, len(r.MessageAttributes))

	assert.NotNil(t, r.MessageAttributes["Attr1"])
	attr1 := r.MessageAttributes["Attr1"]
	assert.Equal(t, "String", attr1.DataType)
	assert.Equal(t, "Value1", attr1.StringValue)
	assert.Empty(t, attr1.BinaryValue)

	assert.NotNil(t, r.MessageAttributes["Attr2"])
	attr2 := r.MessageAttributes["Attr2"]
	assert.Equal(t, "Binary", attr2.DataType)
	assert.Empty(t, attr2.StringValue)
	assert.Equal(t, []uint8("Value2"), attr2.BinaryValue)
}

func TestSetQueueAttributesRequest_SetAttributesFromForm_success(t *testing.T) {
	expectedRedrivePolicy := RedrivePolicy{
		MaxReceiveCount:     100,
		DeadLetterTargetArn: "dead-letter-queue-arn",
	}

	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "new-queue")
	form.Add("Version", "2012-11-05")
	form.Add("Attribute.1.Name", "DelaySeconds")
	form.Add("Attribute.1.Value", "1")
	form.Add("Attribute.2.Name", "MaximumMessageSize")
	form.Add("Attribute.2.Value", "2")
	form.Add("Attribute.3.Name", "MessageRetentionPeriod")
	form.Add("Attribute.3.Value", "3")
	form.Add("Attribute.4.Name", "Policy")
	form.Add("Attribute.4.Value", "{\"i-am\":\"the-policy\"}")
	form.Add("Attribute.5.Name", "ReceiveMessageWaitTimeSeconds")
	form.Add("Attribute.5.Value", "4")
	form.Add("Attribute.6.Name", "VisibilityTimeout")
	form.Add("Attribute.6.Value", "5")
	form.Add("Attribute.7.Name", "RedrivePolicy")
	form.Add("Attribute.7.Value", "{\"maxReceiveCount\": 100, \"deadLetterTargetArn\":\"dead-letter-queue-arn\"}")
	form.Add("Attribute.8.Name", "RedriveAllowPolicy")
	form.Add("Attribute.8.Value", "{\"i-am\":\"the-redrive-allow-policy\"}")

	cqr := &SetQueueAttributesRequest{
		Attributes: QueueAttributes{
			DelaySeconds:                  1,
			MaximumMessageSize:            262144,
			MessageRetentionPeriod:        345600,
			ReceiveMessageWaitTimeSeconds: 10,
			VisibilityTimeout:             30,
		},
	}
	cqr.SetAttributesFromForm(form)

	assert.Equal(t, StringToInt(1), cqr.Attributes.DelaySeconds)
	assert.Equal(t, StringToInt(2), cqr.Attributes.MaximumMessageSize)
	assert.Equal(t, StringToInt(3), cqr.Attributes.MessageRetentionPeriod)
	assert.Equal(t, map[string]interface{}{"i-am": "the-policy"}, cqr.Attributes.Policy)
	assert.Equal(t, StringToInt(4), cqr.Attributes.ReceiveMessageWaitTimeSeconds)
	assert.Equal(t, StringToInt(5), cqr.Attributes.VisibilityTimeout)
	assert.Equal(t, expectedRedrivePolicy, cqr.Attributes.RedrivePolicy)
	assert.Equal(t, map[string]interface{}{"i-am": "the-redrive-allow-policy"}, cqr.Attributes.RedriveAllowPolicy)
}

func TestSetQueueAttributesRequest_SetAttributesFromForm_success_handles_redrive_recieve_count_int(t *testing.T) {
	expectedRedrivePolicy := RedrivePolicy{
		MaxReceiveCount:     100,
		DeadLetterTargetArn: "dead-letter-queue-arn",
	}

	form := url.Values{}
	form.Add("Attribute.1.Name", "RedrivePolicy")
	form.Add("Attribute.1.Value", "{\"maxReceiveCount\": 100, \"deadLetterTargetArn\":\"dead-letter-queue-arn\"}")

	cqr := &SetQueueAttributesRequest{
		Attributes: QueueAttributes{},
	}
	cqr.SetAttributesFromForm(form)

	assert.Equal(t, expectedRedrivePolicy, cqr.Attributes.RedrivePolicy)
}

func TestSetQueueAttributesRequest_SetAttributesFromForm_success_handles_redrive_recieve_count_string(t *testing.T) {
	expectedRedrivePolicy := RedrivePolicy{
		MaxReceiveCount:     100,
		DeadLetterTargetArn: "dead-letter-queue-arn",
	}

	form := url.Values{}
	form.Add("Attribute.1.Name", "RedrivePolicy")
	form.Add("Attribute.1.Value", "{\"maxReceiveCount\": \"100\", \"deadLetterTargetArn\":\"dead-letter-queue-arn\"}")

	cqr := &SetQueueAttributesRequest{
		Attributes: QueueAttributes{},
	}
	cqr.SetAttributesFromForm(form)

	assert.Equal(t, expectedRedrivePolicy, cqr.Attributes.RedrivePolicy)
}

func TestSetQueueAttributesRequest_SetAttributesFromForm_success_default_unparsable_redrive_recieve_count(t *testing.T) {
	defaultRedrivePolicy := RedrivePolicy{
		MaxReceiveCount:     10,
		DeadLetterTargetArn: "dead-letter-queue-arn",
	}

	form := url.Values{}
	form.Add("Attribute.1.Name", "RedrivePolicy")
	form.Add("Attribute.1.Value", "{\"maxReceiveCount\": null, \"deadLetterTargetArn\":\"dead-letter-queue-arn\"}")

	cqr := &SetQueueAttributesRequest{
		Attributes: QueueAttributes{},
	}
	cqr.SetAttributesFromForm(form)

	assert.Equal(t, defaultRedrivePolicy, cqr.Attributes.RedrivePolicy)
}

func TestSetQueueAttributesRequest_SetAttributesFromForm_success_skips_invalid_values(t *testing.T) {
	form := url.Values{}
	form.Add("Attribute.1.Name", "DelaySeconds")
	form.Add("Attribute.1.Value", "garbage")
	form.Add("Attribute.2.Name", "MaximumMessageSize")
	form.Add("Attribute.2.Value", "garbage")
	form.Add("Attribute.3.Name", "MessageRetentionPeriod")
	form.Add("Attribute.3.Value", "garbage")
	form.Add("Attribute.4.Name", "Policy")
	form.Add("Attribute.4.Value", "garbage")
	form.Add("Attribute.5.Name", "ReceiveMessageWaitTimeSeconds")
	form.Add("Attribute.5.Value", "garbage")
	form.Add("Attribute.6.Name", "VisibilityTimeout")
	form.Add("Attribute.6.Value", "garbage")
	form.Add("Attribute.7.Name", "RedrivePolicy")
	form.Add("Attribute.7.Value", "garbage")
	form.Add("Attribute.8.Name", "RedriveAllowPolicy")
	form.Add("Attribute.8.Value", "garbage")

	cqr := &SetQueueAttributesRequest{
		Attributes: QueueAttributes{
			DelaySeconds:                  1,
			MaximumMessageSize:            262144,
			MessageRetentionPeriod:        345600,
			ReceiveMessageWaitTimeSeconds: 10,
			VisibilityTimeout:             30,
		},
	}
	cqr.SetAttributesFromForm(form)

	assert.Equal(t, StringToInt(1), cqr.Attributes.DelaySeconds)
	assert.Equal(t, StringToInt(262144), cqr.Attributes.MaximumMessageSize)
	assert.Equal(t, StringToInt(345600), cqr.Attributes.MessageRetentionPeriod)
	assert.Equal(t, map[string]interface{}(nil), cqr.Attributes.Policy)
	assert.Equal(t, StringToInt(10), cqr.Attributes.ReceiveMessageWaitTimeSeconds)
	assert.Equal(t, StringToInt(30), cqr.Attributes.VisibilityTimeout)
	assert.Equal(t, RedrivePolicy{}, cqr.Attributes.RedrivePolicy)
	assert.Equal(t, map[string]interface{}(nil), cqr.Attributes.RedriveAllowPolicy)
}

func TestNewCreateTopicRequest(t *testing.T) {
	defer func() {
		ResetApp()
	}()

	result := NewCreateTopicRequest()

	assert.Equal(t, false, result.Attributes.FifoTopic)
	assert.Equal(t, StringToInt(1), result.Attributes.SignatureVersion)
	assert.Equal(t, "Active", result.Attributes.TracingConfig)
	assert.Equal(t, false, result.Attributes.ContentBasedDeduplication)
}

func TestCreateTopicRequest_SetAttributesFromForm_success(t *testing.T) {
	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "new-queue")
	form.Add("Version", "2012-11-05")
	form.Add("Attribute.1.Name", "DeliveryPolicy")
	form.Add("Attribute.1.Value", "{\"i-am\":\"the-policy\", \"name\":\"delivery-policy\"}")
	form.Add("Attribute.2.Name", "DisplayName")
	form.Add("Attribute.2.Value", "Foo")
	form.Add("Attribute.3.Name", "FifoTopic")
	form.Add("Attribute.3.Value", "true")
	form.Add("Attribute.4.Name", "Policy")
	form.Add("Attribute.4.Value", "{\"i-am\":\"the-policy\", \"name\":\"policy\"}")
	form.Add("Attribute.5.Name", "SignatureVersion")
	form.Add("Attribute.5.Value", "99")
	form.Add("Attribute.6.Name", "TracingConfig")
	form.Add("Attribute.6.Value", "PassThrough")
	form.Add("Attribute.7.Name", "KmsMasterKeyId")
	form.Add("Attribute.7.Value", "1234abcd-12ab-34cd-56ef-1234567890ab")
	form.Add("Attribute.8.Name", "ArchivePolicy")
	form.Add("Attribute.8.Value", "{\"i-am\":\"the-policy\", \"name\":\"archive-policy\"}")
	form.Add("Attribute.9.Name", "BeginningArchiveTime")
	form.Add("Attribute.9.Value", "2024-07-01T23:59:59+09:00")
	form.Add("Attribute.10.Name", "ContentBasedDeduplication")
	form.Add("Attribute.10.Value", "true")

	ctr := &CreateTopicRequest{}
	ctr.SetAttributesFromForm(form)

	assert.Equal(t, 2, len(ctr.Attributes.DeliveryPolicy))
	assert.Equal(t, "the-policy", ctr.Attributes.DeliveryPolicy["i-am"])
	assert.Equal(t, "delivery-policy", ctr.Attributes.DeliveryPolicy["name"])
	assert.Equal(t, "Foo", ctr.Attributes.DisplayName)
	assert.Equal(t, true, ctr.Attributes.FifoTopic)
	assert.Equal(t, 2, len(ctr.Attributes.Policy))
	assert.Equal(t, "the-policy", ctr.Attributes.Policy["i-am"])
	assert.Equal(t, "policy", ctr.Attributes.Policy["name"])
	assert.Equal(t, StringToInt(99), ctr.Attributes.SignatureVersion)
	assert.Equal(t, "PassThrough", ctr.Attributes.TracingConfig)
	assert.Equal(t, "1234abcd-12ab-34cd-56ef-1234567890ab", ctr.Attributes.KmsMasterKeyId)
	assert.Equal(t, 2, len(ctr.Attributes.ArchivePolicy))
	assert.Equal(t, "the-policy", ctr.Attributes.ArchivePolicy["i-am"])
	assert.Equal(t, "archive-policy", ctr.Attributes.ArchivePolicy["name"])
	assert.Equal(t, "2024-07-01T23:59:59+09:00", ctr.Attributes.BeginningArchiveTime)
	assert.Equal(t, true, ctr.Attributes.ContentBasedDeduplication)
}

func TestSubscribeRequest_SetAttributesFromForm_success(t *testing.T) {
	form := url.Values{}
	form.Add("Attributes.entry.1.key", "RawMessageDelivery")
	form.Add("Attributes.entry.1.value", "true")
	form.Add("Attributes.entry.2.key", "FilterPolicy")
	form.Add("Attributes.entry.2.value", "{\"filter\": [\"policy\"]}")

	cqr := &SubscribeRequest{
		Attributes: SubscriptionAttributes{},
	}
	cqr.SetAttributesFromForm(form)

	assert.True(t, cqr.Attributes.RawMessageDelivery)
	assert.Equal(t, FilterPolicy{"filter": []string{"policy"}}, cqr.Attributes.FilterPolicy)
}

func TestSubscribeRequest_SetAttributesFromForm_skips_invalid_values(t *testing.T) {
	form := url.Values{}
	form.Add("Attributes.entry.1.key", "RawMessageDelivery")
	form.Add("Attributes.entry.1.value", "garbage")
	form.Add("Attributes.entry.2.key", "FilterPolicy")
	form.Add("Attributes.entry.2.value", "also-garbage")

	cqr := &SubscribeRequest{
		Attributes: SubscriptionAttributes{},
	}
	cqr.SetAttributesFromForm(form)

	assert.False(t, cqr.Attributes.RawMessageDelivery)
	assert.Equal(t, FilterPolicy(nil), cqr.Attributes.FilterPolicy)
}

func TestSubscribeRequest_SetAttributesFromForm_stops_if_attributes_not_numbered_sequentially(t *testing.T) {
	form := url.Values{}
	form.Add("Attributes.entry.2.key", "RawMessageDelivery")
	form.Add("Attributes.entry.2.value", "garbage")
	form.Add("Attributes.entry.3.key", "FilterPolicy")
	form.Add("Attributes.entry.3.value", "also-garbage")

	cqr := &SubscribeRequest{
		Attributes: SubscriptionAttributes{},
	}
	cqr.SetAttributesFromForm(form)

	assert.False(t, cqr.Attributes.RawMessageDelivery)
	assert.Equal(t, FilterPolicy(nil), cqr.Attributes.FilterPolicy)
}

func Test_DeleteMessageBatchRequest_SetAttributesFromForm_success(t *testing.T) {
	form := url.Values{}
	form.Add("DeleteMessageBatchRequestEntry.1.Id", "message-id-1")
	form.Add("DeleteMessageBatchRequestEntry.1.ReceiptHandle", "receipt-handle-1")
	form.Add("DeleteMessageBatchRequestEntry.2.Id", "message-id-2")
	form.Add("DeleteMessageBatchRequestEntry.2.ReceiptHandle", "receipt-handle-2")
	form.Add("DeleteMessageBatchRequestEntry.3.Id", "message-id-3")
	form.Add("DeleteMessageBatchRequestEntry.3.ReceiptHandle", "receipt-handle-3")

	dmbr := &DeleteMessageBatchRequest{}
	dmbr.SetAttributesFromForm(form)

	assert.Len(t, dmbr.Entries, 3)
	assert.Equal(t, "message-id-1", dmbr.Entries[0].Id)
	assert.Equal(t, "receipt-handle-1", dmbr.Entries[0].ReceiptHandle)
	assert.Equal(t, "message-id-2", dmbr.Entries[1].Id)
	assert.Equal(t, "receipt-handle-2", dmbr.Entries[1].ReceiptHandle)
	assert.Equal(t, "message-id-3", dmbr.Entries[2].Id)
	assert.Equal(t, "receipt-handle-3", dmbr.Entries[2].ReceiptHandle)
}

func Test_DeleteMessageBatchRequest_SetAttributesFromForm_stops_at_non_sequential_keys(t *testing.T) {
	form := url.Values{}
	form.Add("DeleteMessageBatchRequestEntry.1.Id", "message-id-1")
	form.Add("DeleteMessageBatchRequestEntry.1.ReceiptHandle", "receipt-handle-1")
	form.Add("DeleteMessageBatchRequestEntry.4.Id", "message-id-2")
	form.Add("DeleteMessageBatchRequestEntry.4.ReceiptHandle", "receipt-handle-2")
	form.Add("DeleteMessageBatchRequestEntry.3.Id", "message-id-3")
	form.Add("DeleteMessageBatchRequestEntry.3.ReceiptHandle", "receipt-handle-3")

	dmbr := &DeleteMessageBatchRequest{}
	dmbr.SetAttributesFromForm(form)

	assert.Len(t, dmbr.Entries, 1)
	assert.Equal(t, "message-id-1", dmbr.Entries[0].Id)
	assert.Equal(t, "receipt-handle-1", dmbr.Entries[0].ReceiptHandle)
}

func Test_DeleteMessageBatchRequest_SetAttributesFromForm_stops_at_invalid_keys(t *testing.T) {
	form := url.Values{}
	form.Add("DeleteMessageBatchRequestEntry.1.Id", "message-id-1")
	form.Add("DeleteMessageBatchRequestEntry.1.ReceiptHandle", "receipt-handle-1")
	form.Add("INVALID_DeleteMessageBatchRequestEntry.2.Id", "message-id-2")
	form.Add("DeleteMessageBatchRequestEntry.2.ReceiptHandle", "receipt-handle-2")
	form.Add("DeleteMessageBatchRequestEntry.3.Id", "message-id-3")
	form.Add("DeleteMessageBatchRequestEntry.3.ReceiptHandle", "receipt-handle-3")

	dmbr := &DeleteMessageBatchRequest{}
	dmbr.SetAttributesFromForm(form)

	assert.Len(t, dmbr.Entries, 1)
	assert.Equal(t, "message-id-1", dmbr.Entries[0].Id)
	assert.Equal(t, "receipt-handle-1", dmbr.Entries[0].ReceiptHandle)
}

func TestPublishRequest_SetAttributesFromForm_success_concurrent(t *testing.T) {
	form := url.Values{}
	form.Add("MessageAttributes.entry.1.Name", "test1")
	form.Add("MessageAttributes.entry.1.Value.DataType", "String")
	form.Add("MessageAttributes.entry.1.Value.StringValue", "sample-string")
	form.Add("MessageAttributes.entry.2.Name", "test2")
	form.Add("MessageAttributes.entry.2.Value.DataType", "Binary")
	form.Add("MessageAttributes.entry.2.Value.BinaryValue", "YmluYXJ5LXZhbHVl")

	// if the code is not thread-safe, repeated runs increase the chance of detecting a race.
	for r := 0; r < 10; r++ {
		var wg sync.WaitGroup
		goroutineCount := 40
		// launch goroutines in parallel to simulate concurrent access.
		for g := 0; g < goroutineCount; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// introduce a random delay to encourage goroutine interleaving
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
				cqr := &PublishRequest{
					MessageAttributes: make(map[string]MessageAttribute),
				}

				cqr.SetAttributesFromForm(form)

				// validate the expected DataType values
				assert.Equal(t, "String", cqr.MessageAttributes["test1"].DataType)
				assert.Equal(t, "Binary", cqr.MessageAttributes["test2"].DataType)
			}()
		}
		wg.Wait()
	}
}

func TestParseMessageAttributes(t *testing.T) {
	for _, tc := range []struct {
		description string
		values      url.Values
		keyPrefix   string
		want        map[string]MessageAttribute
	}{
		{
			description: "empty",
			values:      url.Values{},
			keyPrefix:   "foo",
			want:        nil,
		},
		{
			description: "simple",
			values: url.Values{
				"MessageAttribute.1.Name":              []string{"Attr1"},
				"MessageAttribute.1.Value.DataType":    []string{"String"},
				"MessageAttribute.1.Value.StringValue": []string{"Value1"},
				"MessageAttribute.2.Name":              []string{"Attr2"},
				"MessageAttribute.2.Value.DataType":    []string{"Binary"},
				"MessageAttribute.2.Value.BinaryValue": []string{"VmFsdWUy"},
			},
			keyPrefix: "MessageAttribute",
			want: map[string]MessageAttribute{
				"Attr1": {
					DataType:    "String",
					StringValue: "Value1",
					BinaryValue: []byte{},
				},
				"Attr2": {
					DataType:    "Binary",
					BinaryValue: []byte("Value2"),
				},
			},
		},
		{
			description: "attributes after empty name ignored",
			values: url.Values{
				"MessageAttribute.1.Name":              []string{""},
				"MessageAttribute.1.Value.DataType":    []string{"String"},
				"MessageAttribute.1.Value.StringValue": []string{"Value4"},
				"MessageAttribute.2.Name":              []string{"Attr2"},
				"MessageAttribute.2.Value.DataType":    []string{"Binary"},
				"MessageAttribute.2.Value.BinaryValue": []string{"VmFsdWUy"},
			},
			keyPrefix: "MessageAttribute",
			want:      nil,
		},
		{
			description: "attributes after missing number ignored",
			values: url.Values{
				// Note starting from 2
				"MessageAttribute.2.Name":              []string{"Attr2"},
				"MessageAttribute.2.Value.DataType":    []string{"Binary"},
				"MessageAttribute.2.Value.BinaryValue": []string{"VmFsdWUy"},
			},
			keyPrefix: "MessageAttribute",
			want:      nil,
		},
		{
			description: "empty DataType ignored",
			values: url.Values{
				"MessageAttribute.1.Name":              []string{"Attr4"},
				"MessageAttribute.1.Value.DataType":    []string{""},
				"MessageAttribute.1.Value.StringValue": []string{"Value4"},
			},
			keyPrefix: "MessageAttribute",
			want:      nil,
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			got := parseMessageAttributes(tc.values, tc.keyPrefix)
			assert.Equal(t, tc.want, got)
		})
	}
}
