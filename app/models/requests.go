package models

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	log "github.com/sirupsen/logrus"
)

type CreateQueueRequest struct {
	QueueName  string            `json:"QueueName" schema:"QueueName"`
	Attributes QueueAttributes   `json:"Attributes" schema:"Attribute"`
	Tags       map[string]string `json:"Tags" schema:"Tags"`
	Version    string            `json:"Version" schema:"Version"`
}

// TODO - is there an easier way to do this?  Similar to the StringToInt type?
func (r *CreateQueueRequest) SetAttributesFromForm(values url.Values) {
	for i := 1; true; i++ {
		nameKey := fmt.Sprintf("Attribute.%d.Name", i)
		attrName := values.Get(nameKey)
		if attrName == "" {
			break
		}

		valueKey := fmt.Sprintf("Attribute.%d.Value", i)
		attrValue := values.Get(valueKey)
		if attrValue == "" {
			continue
		}
		switch attrName {
		case "DelaySeconds":
			tmp, err := strconv.Atoi(attrValue)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.DelaySeconds = StringToInt(tmp)
		case "MaximumMessageSize":
			tmp, err := strconv.Atoi(attrValue)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.MaximumMessageSize = StringToInt(tmp)
		case "MessageRetentionPeriod":
			tmp, err := strconv.Atoi(attrValue)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.MessageRetentionPeriod = StringToInt(tmp)
		case "Policy":
			var tmp map[string]interface{}
			err := json.Unmarshal([]byte(attrValue), &tmp)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.Policy = tmp
		case "ReceiveMessageWaitTimeSeconds":
			tmp, err := strconv.Atoi(attrValue)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.ReceiveMessageWaitTimeSeconds = StringToInt(tmp)
		case "VisibilityTimeout":
			tmp, err := strconv.Atoi(attrValue)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.VisibilityTimeout = StringToInt(tmp)
		case "RedrivePolicy":
			tmp := RedrivePolicy{}
			var decodedPolicy struct {
				MaxReceiveCount     interface{} `json:"maxReceiveCount"`
				DeadLetterTargetArn string      `json:"deadLetterTargetArn"`
			}
			err := json.Unmarshal([]byte(attrValue), &decodedPolicy)
			if err != nil || decodedPolicy.DeadLetterTargetArn == "" {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			// Support both int and string types (historic processing), set a default of 10 if not provided.
			// Go will default into float64 for interface{} types when parsing numbers
			receiveCount, ok := decodedPolicy.MaxReceiveCount.(float64)
			if !ok {
				receiveCount = 10
				t, ok := decodedPolicy.MaxReceiveCount.(string)
				if ok {
					r, err := strconv.ParseFloat(t, 64)
					if err == nil {
						receiveCount = r
					} else {
						log.Debugf("Failed to parse form attribute (maxReceiveCount) - %s: %s", attrName, attrValue)
					}
				} else {
					log.Debugf("Failed to parse form attribute (maxReceiveCount) - %s: %s", attrName, attrValue)
				}
			}
			tmp.MaxReceiveCount = StringToInt(receiveCount)
			tmp.DeadLetterTargetArn = decodedPolicy.DeadLetterTargetArn
			r.Attributes.RedrivePolicy = tmp
		case "RedriveAllowPolicy":
			var tmp map[string]interface{}
			err := json.Unmarshal([]byte(attrValue), &tmp)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.RedriveAllowPolicy = tmp
		}
	}
	return
}

func NewListQueuesRequest() *ListQueueRequest {
	return &ListQueueRequest{}
}

type ListQueueRequest struct {
	MaxResults      int    `json:"MaxResults" schema:"MaxResults"`
	NextToken       string `json:"NextToken" schema:"NextToken"`
	QueueNamePrefix string `json:"QueueNamePrefix" schema:"QueueNamePrefix"`
}

func (r *ListQueueRequest) SetAttributesFromForm(values url.Values) {
	maxResults, err := strconv.Atoi(values.Get("MaxResults"))
	if err == nil {
		r.MaxResults = maxResults
	}
	r.NextToken = values.Get("NextToken")
	r.QueueNamePrefix = values.Get("QueueNamePrefix")
}

func NewGetQueueAttributesRequest() *GetQueueAttributesRequest {
	return &GetQueueAttributesRequest{}
}

type GetQueueAttributesRequest struct {
	QueueUrl       string   `json:"QueueUrl"`
	AttributeNames []string `json:"AttributeNames"`
}

func (r *GetQueueAttributesRequest) SetAttributesFromForm(values url.Values) {
	r.QueueUrl = values.Get("QueueUrl")
	for i := 1; true; i++ {
		attrKey := fmt.Sprintf("AttributeName.%d", i)
		attrValue := values.Get(attrKey)
		if attrValue == "" {
			break
		}
		r.AttributeNames = append(r.AttributeNames, attrValue)
	}
}

/*** Send Message Request */
func NewSendMessageRequest() *SendMessageRequest {
	return &SendMessageRequest{
		MessageAttributes:       make(map[string]MessageAttribute),
		MessageSystemAttributes: make(map[string]MessageAttribute),
	}
}

type SendMessageRequest struct {
	DelaySeconds int `json:"DelaySeconds" schema:"DelaySeconds"`
	// MessageAttributes is custom attributes that users can add on the message as they like.
	// Please see: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html#SQS-SendMessage-request-MessageAttributes
	MessageAttributes      map[string]MessageAttribute `json:"MessageAttributes" schema:"MessageAttributes"`
	MessageBody            string                      `json:"MessageBody" schema:"MessageBody"`
	MessageDeduplicationId string                      `json:"MessageDeduplicationId" schema:"MessageDeduplicationId"`
	MessageGroupId         string                      `json:"MessageGroupId" schema:"MessageGroupId"`
	// MessageSystemAttributes is custom attributes for AWS services.
	// Please see: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html#SQS-SendMessage-request-MessageSystemAttributes
	// On AWS, the only supported attribute is "AWSTraceHeader" that is for AWS X-Ray.
	// Goaws does not contains X-Ray emulation, so currently MessageSystemAttributes is unsupported.
	// TODO: Replace with a struct with known attributes "AWSTraceHeader".
	MessageSystemAttributes map[string]MessageAttribute `json:"MessageSystemAttributes" schema:"MessageSystemAttributes"`
	QueueUrl                string                      `json:"QueueUrl" schema:"QueueUrl"`
}

func parseMessageAttributes(values url.Values, keyPrefix string) map[string]MessageAttribute {
	result := map[string]MessageAttribute{}

	for i := 1; true; i++ {
		nameKey := fmt.Sprintf("%s.%d.Name", keyPrefix, i)
		name := values.Get(nameKey)
		if name == "" {
			break
		}

		dataTypeKey := fmt.Sprintf("%s.%d.Value.DataType", keyPrefix, i)
		dataType := values.Get(dataTypeKey)
		if dataType == "" {
			log.Warnf("DataType of message attribute %s is missing, MD5 checksum will most probably be wrong!\n", name)
			continue
		}

		stringValue := values.Get(fmt.Sprintf("%s.%d.Value.StringValue", keyPrefix, i))
		encodedBinaryValue := values.Get(fmt.Sprintf("%s.%d.Value.BinaryValue", keyPrefix, i))

		binaryValue, err := base64.StdEncoding.DecodeString(encodedBinaryValue)
		if err != nil {
			log.Warnf("Failed to base64 decode. %s may not have been base64 encoded.", encodedBinaryValue)
			binaryValue = []byte(encodedBinaryValue)
		}

		result[name] = MessageAttribute{
			DataType:    dataType,
			StringValue: stringValue,
			BinaryValue: binaryValue,
		}
	}

	if len(result) > 0 {
		return result
	}
	return nil
}

func (r *SendMessageRequest) SetAttributesFromForm(values url.Values) {
	r.MessageAttributes = parseMessageAttributes(values, "MessageAttribute")
}

func NewSendMessageBatchRequest() *SendMessageBatchRequest {
	return &SendMessageBatchRequest{}
}

type SendMessageBatchRequest struct {
	Entries  []SendMessageBatchRequestEntry
	QueueUrl string
}

func (r *SendMessageBatchRequest) SetAttributesFromForm(values url.Values) {
	for entryIndex := range r.Entries {
		r.Entries[entryIndex].MessageAttributes = parseMessageAttributes(values, fmt.Sprintf("Entries.%d.MessageAttributes", entryIndex))
	}
}

type SendMessageBatchRequestEntry struct {
	Id                      string                      `json:"Id" schema:"Id"`
	MessageBody             string                      `json:"MessageBody" schema:"MessageBody"`
	DelaySeconds            int                         `json:"DelaySeconds" schema:"DelaySeconds"` // NOTE: not implemented
	MessageAttributes       map[string]MessageAttribute `json:"MessageAttributes" schema:"MessageAttributes"`
	MessageDeduplicationId  string                      `json:"MessageDeduplicationId" schema:"MessageDeduplicationId"`
	MessageGroupId          string                      `json:"MessageGroupId" schema:"MessageGroupId"`
	MessageSystemAttributes map[string]MessageAttribute `json:"MessageSystemAttributes" schema:"MessageSystemAttributes"` // NOTE: not implemented
}

// Get Queue Url Request
func NewGetQueueUrlRequest() *GetQueueUrlRequest {
	return &GetQueueUrlRequest{}
}

type GetQueueUrlRequest struct {
	QueueName              string `json:"QueueName"`
	QueueOwnerAWSAccountId string `json:"QueueOwnerAWSAccountId"` // NOTE: not implemented
}

func (r *GetQueueUrlRequest) SetAttributesFromForm(values url.Values) {}

func NewSetQueueAttributesRequest() *SetQueueAttributesRequest {
	return &SetQueueAttributesRequest{}
}

type SetQueueAttributesRequest struct {
	QueueUrl   string          `json:"QueueUrl"`
	Attributes QueueAttributes `json:"Attributes"`
}

func (r *SetQueueAttributesRequest) SetAttributesFromForm(values url.Values) {
	r.QueueUrl = values.Get("QueueUrl")
	// TODO - could we share with CreateQueueRequest?
	for i := 1; true; i++ {
		nameKey := fmt.Sprintf("Attribute.%d.Name", i)
		attrName := values.Get(nameKey)
		if attrName == "" {
			break
		}

		valueKey := fmt.Sprintf("Attribute.%d.Value", i)
		attrValue := values.Get(valueKey)
		if attrValue == "" {
			continue
		}
		switch attrName {
		case "DelaySeconds":
			tmp, err := strconv.Atoi(attrValue)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.DelaySeconds = StringToInt(tmp)
		case "MaximumMessageSize":
			tmp, err := strconv.Atoi(attrValue)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.MaximumMessageSize = StringToInt(tmp)
		case "MessageRetentionPeriod":
			tmp, err := strconv.Atoi(attrValue)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.MessageRetentionPeriod = StringToInt(tmp)
		case "Policy":
			var tmp map[string]interface{}
			err := json.Unmarshal([]byte(attrValue), &tmp)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.Policy = tmp
		case "ReceiveMessageWaitTimeSeconds":
			tmp, err := strconv.Atoi(attrValue)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.ReceiveMessageWaitTimeSeconds = StringToInt(tmp)
		case "VisibilityTimeout":
			tmp, err := strconv.Atoi(attrValue)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.VisibilityTimeout = StringToInt(tmp)
		case "RedrivePolicy":
			tmp := RedrivePolicy{}
			var decodedPolicy struct {
				MaxReceiveCount     interface{} `json:"maxReceiveCount"`
				DeadLetterTargetArn string      `json:"deadLetterTargetArn"`
			}
			err := json.Unmarshal([]byte(attrValue), &decodedPolicy)
			if err != nil || decodedPolicy.DeadLetterTargetArn == "" {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			// Support both int and string types (historic processing), set a default of 10 if not provided.
			// Go will default into float64 for interface{} types when parsing numbers
			receiveCount, ok := decodedPolicy.MaxReceiveCount.(float64)
			if !ok {
				receiveCount = 10
				t, ok := decodedPolicy.MaxReceiveCount.(string)
				if ok {
					r, err := strconv.ParseFloat(t, 64)
					if err == nil {
						receiveCount = r
					} else {
						log.Debugf("Failed to parse form attribute (maxReceiveCount) - %s: %s", attrName, attrValue)
					}
				} else {
					log.Debugf("Failed to parse form attribute (maxReceiveCount) - %s: %s", attrName, attrValue)
				}
			}
			tmp.MaxReceiveCount = StringToInt(receiveCount)
			tmp.DeadLetterTargetArn = decodedPolicy.DeadLetterTargetArn
			r.Attributes.RedrivePolicy = tmp
		case "RedriveAllowPolicy":
			var tmp map[string]interface{}
			err := json.Unmarshal([]byte(attrValue), &tmp)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.RedriveAllowPolicy = tmp
		}
	}
	return
}

// TODO - there are FIFO attributes and things too
// QueueAttributes - SQS QueueAttributes Available in create/set attributes requests.
// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html#SQS-CreateQueue-request-attributes
type QueueAttributes struct {
	DelaySeconds                  StringToInt            `json:"DelaySeconds"`
	MaximumMessageSize            StringToInt            `json:"MaximumMessageSize"`
	MessageRetentionPeriod        StringToInt            `json:"MessageRetentionPeriod"` // NOTE: not implemented
	Policy                        map[string]interface{} `json:"Policy"`                 // NOTE: not implemented
	ReceiveMessageWaitTimeSeconds StringToInt            `json:"ReceiveMessageWaitTimeSeconds"`
	VisibilityTimeout             StringToInt            `json:"VisibilityTimeout"`
	// Dead Letter Queues Only
	RedrivePolicy      RedrivePolicy          `json:"RedrivePolicy"`
	RedriveAllowPolicy map[string]interface{} `json:"RedriveAllowPolicy"` // NOTE: not implemented
}

type RedrivePolicy struct {
	MaxReceiveCount     StringToInt `json:"maxReceiveCount"`
	DeadLetterTargetArn string      `json:"deadLetterTargetArn"`
}

// UnmarshalJSON this will convert a JSON string of a Redrive Policy sub-doc (escaped characters and all) or
// a regular json document into the appropriate resulting struct.
func (r *RedrivePolicy) UnmarshalJSON(data []byte) error {
	type basicRequest RedrivePolicy

	err := json.Unmarshal(data, (*basicRequest)(r))
	if err == nil {
		return nil
	}

	tmp, _ := strconv.Unquote(string(data))
	err = json.Unmarshal([]byte(tmp), (*basicRequest)(r))
	if err != nil {
		return err
	}
	return nil
}

func NewReceiveMessageRequest() *ReceiveMessageRequest {
	return &ReceiveMessageRequest{}
}

type ReceiveMessageRequest struct {
	QueueUrl                    string   `json:"QueueUrl" schema:"QueueUrl"`
	AttributeNames              []string `json:"AttributeNames" schema:"AttributeNames"`
	MessageSystemAttributeNames []string `json:"MessageSystemAttributeNames" schema:"MessageSystemAttributeNames"`
	MessageAttributeNames       []string `json:"MessageAttributeNames" schema:"MessageAttributeNames"`
	MaxNumberOfMessages         int      `json:"MaxNumberOfMessages" schema:"MaxNumberOfMessages"`
	VisibilityTimeout           int      `json:"VisibilityTimeout" schema:"VisibilityTimeout"`
	WaitTimeSeconds             int      `json:"WaitTimeSeconds" schema:"WaitTimeSeconds"`
	ReceiveRequestAttemptId     string   `json:"ReceiveRequestAttemptId" schema:"ReceiveRequestAttemptId"`
}

func (r *ReceiveMessageRequest) SetAttributesFromForm(values url.Values) {}

func NewCreateQueueRequest() *CreateQueueRequest {
	return &CreateQueueRequest{
		Attributes: QueueAttributes{
			DelaySeconds:                  0,
			MaximumMessageSize:            StringToInt(CurrentEnvironment.QueueAttributeDefaults.MaximumMessageSize),
			MessageRetentionPeriod:        StringToInt(CurrentEnvironment.QueueAttributeDefaults.MessageRetentionPeriod),
			ReceiveMessageWaitTimeSeconds: StringToInt(CurrentEnvironment.QueueAttributeDefaults.ReceiveMessageWaitTimeSeconds),
			VisibilityTimeout:             StringToInt(CurrentEnvironment.QueueAttributeDefaults.VisibilityTimeout),
		},
	}
}

func NewChangeMessageVisibilityRequest() *ChangeMessageVisibilityRequest {
	return &ChangeMessageVisibilityRequest{}
}

type ChangeMessageVisibilityRequest struct {
	QueueUrl          string `json:"QueueUrl" schema:"QueueUrl"`
	ReceiptHandle     string `json:"ReceiptHandle" schema:"ReceiptHandle"`
	VisibilityTimeout int    `json:"VisibilityTimeout" schema:"VisibilityTimeout"`
}

func (r *ChangeMessageVisibilityRequest) SetAttributesFromForm(values url.Values) {}

func NewDeleteMessageRequest() *DeleteMessageRequest {
	return &DeleteMessageRequest{}
}

type DeleteMessageRequest struct {
	QueueUrl      string `json:"QueueUrl" schema:"QueueUrl"`
	ReceiptHandle string `json:"ReceiptHandle" schema:"ReceiptHandle"`
}

func (r *DeleteMessageRequest) SetAttributesFromForm(values url.Values) {}

func NewPurgeQueueRequest() *PurgeQueueRequest {
	return &PurgeQueueRequest{}
}

type PurgeQueueRequest struct {
	QueueUrl string `json:"QueueUrl" schema:"QueueUrl"`
}

func (r *PurgeQueueRequest) SetAttributesFromForm(values url.Values) {}

func NewDeleteQueueRequest() *DeleteQueueRequest {
	return &DeleteQueueRequest{}
}

type DeleteQueueRequest struct {
	QueueUrl string `json:"QueueUrl" schema:"QueueUrl"`
}

func (r *DeleteQueueRequest) SetAttributesFromForm(values url.Values) {}

type DeleteMessageBatchRequestEntry struct {
	Id            string `json:"Id" schema:"Id"`
	ReceiptHandle string `json:"ReceiptHandle" schema:"ReceiptHandle"`
}

type DeleteMessageBatchRequest struct {
	Entries  []DeleteMessageBatchRequestEntry `json:"Entries"`
	QueueUrl string                           `json:"QueueUrl" schema:"QueueUrl"`
}

func NewDeleteMessageBatchRequest() *DeleteMessageBatchRequest {
	return &DeleteMessageBatchRequest{}
}

func (r *DeleteMessageBatchRequest) SetAttributesFromForm(values url.Values) {
	entries := []DeleteMessageBatchRequestEntry{}
	for i := 1; true; i++ {
		msgIdKey := fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.Id", i)
		receiptHandleKey := fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.ReceiptHandle", i)

		msgId := values.Get(msgIdKey)
		receiptHandle := values.Get(receiptHandleKey)
		if msgId == "" || receiptHandle == "" {
			break
		}
		entries = append(entries, DeleteMessageBatchRequestEntry{
			Id:            msgId,
			ReceiptHandle: receiptHandle,
		})
	}
	if len(entries) > 0 {
		r.Entries = entries
	}
}

// ---- SNS ----
func NewCreateTopicRequest() *CreateTopicRequest {
	return &CreateTopicRequest{
		Attributes: TopicAttributes{
			FifoTopic:                 false,
			SignatureVersion:          1,
			TracingConfig:             "Active",
			ContentBasedDeduplication: false,
		},
	}
}

type CreateTopicRequest struct {
	Name string `json:"Name" schema:"Name"`

	// Goaws unsupports below properties currently.
	DataProtectionPolicy string            `json:"DataProtectionPolicy" schema:"DataProtectionPolicy"`
	Attributes           TopicAttributes   `json:"Attributes" schema:"Attributes"`
	Tags                 map[string]string `json:"Tags" schema:"Tags"`
}

// Ref: https://docs.aws.amazon.com/sns/latest/api/API_CreateTopic.html
type TopicAttributes struct {
	DeliveryPolicy            map[string]interface{} `json:"DeliveryPolicy"`            // NOTE: not implemented
	DisplayName               string                 `json:"DisplayName"`               // NOTE: not implemented
	FifoTopic                 bool                   `json:"FifoTopic"`                 // NOTE: not implemented
	Policy                    map[string]interface{} `json:"Policy"`                    // NOTE: not implemented
	SignatureVersion          StringToInt            `json:"SignatureVersion"`          // NOTE: not implemented
	TracingConfig             string                 `json:"TracingConfig"`             // NOTE: not implemented
	KmsMasterKeyId            string                 `json:"KmsMasterKeyId"`            // NOTE: not implemented
	ArchivePolicy             map[string]interface{} `json:"ArchivePolicy"`             // NOTE: not implemented
	BeginningArchiveTime      string                 `json:"BeginningArchiveTime"`      // NOTE: not implemented
	ContentBasedDeduplication bool                   `json:"ContentBasedDeduplication"` // NOTE: not implemented
}

func (r *CreateTopicRequest) SetAttributesFromForm(values url.Values) {
	for i := 1; true; i++ {
		nameKey := fmt.Sprintf("Attribute.%d.Name", i)
		attrName := values.Get(nameKey)
		if attrName == "" {
			break
		}

		valueKey := fmt.Sprintf("Attribute.%d.Value", i)
		attrValue := values.Get(valueKey)
		if attrValue == "" {
			continue
		}

		switch attrName {
		case "DeliveryPolicy":
			var tmp map[string]interface{}
			err := json.Unmarshal([]byte(attrValue), &tmp)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.DeliveryPolicy = tmp
		case "DisplayName":
			r.Attributes.DisplayName = attrValue
		case "FifoTopic":
			tmp, err := strconv.ParseBool(attrValue)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.FifoTopic = tmp
		case "Policy":
			var tmp map[string]interface{}
			err := json.Unmarshal([]byte(attrValue), &tmp)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.Policy = tmp
		case "SignatureVersion":
			tmp, err := strconv.Atoi(attrValue)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.SignatureVersion = StringToInt(tmp)
		case "TracingConfig":
			r.Attributes.TracingConfig = attrValue
		case "KmsMasterKeyId":
			r.Attributes.KmsMasterKeyId = attrValue
		case "ArchivePolicy":
			var tmp map[string]interface{}
			err := json.Unmarshal([]byte(attrValue), &tmp)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.ArchivePolicy = tmp
		case "BeginningArchiveTime":
			r.Attributes.BeginningArchiveTime = attrValue
		case "ContentBasedDeduplication":
			tmp, err := strconv.ParseBool(attrValue)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.ContentBasedDeduplication = tmp
		}
	}
}

func NewSubscribeRequest() *SubscribeRequest {
	return &SubscribeRequest{}
}

type SubscribeRequest struct {
	TopicArn   string                 `json:"TopicArn" schema:"TopicArn"`
	Endpoint   string                 `json:"Endpoint" schema:"Endpoint"`
	Protocol   string                 `json:"Protocol" schema:"Protocol"`
	Attributes SubscriptionAttributes `json:"Attributes"`
}

func (r *SubscribeRequest) SetAttributesFromForm(values url.Values) {
	for i := 1; true; i++ {
		nameKey := fmt.Sprintf("Attributes.entry.%d.key", i)
		attrName := values.Get(nameKey)
		if attrName == "" {
			break
		}

		valueKey := fmt.Sprintf("Attributes.entry.%d.value", i)
		attrValue := values.Get(valueKey)
		if attrValue == "" {
			continue
		}
		switch attrName {
		case "RawMessageDelivery":
			tmp, err := strconv.ParseBool(attrValue)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.RawMessageDelivery = tmp
		case "FilterPolicy":
			var tmp map[string][]string
			err := json.Unmarshal([]byte(attrValue), &tmp)
			if err != nil {
				log.Debugf("Failed to parse form attribute - %s: %s", attrName, attrValue)
				continue
			}
			r.Attributes.FilterPolicy = tmp
		}
	}
	return
}

type SubscriptionAttributes struct {
	FilterPolicy       FilterPolicy `json:"FilterPolicy" schema:"FilterPolicy"`
	RawMessageDelivery bool         `json:"RawMessageDelivery" schema:"RawMessageDelivery"`
	//DeliveryPolicy      map[string]interface{} `json:"DeliveryPolicy" schema:"DeliveryPolicy"`
	//FilterPolicyScope   string                 `json:"FilterPolicyScope" schema:"FilterPolicyScope"`
	//RedrivePolicy       RedrivePolicy          `json:"RedrivePolicy" schema:"RawMessageDelivery"`
	//SubscriptionRoleArn string                 `json:"SubscriptionRoleArn" schema:"SubscriptionRoleArn"`
	//ReplayPolicy        string                 `json:"ReplayPolicy" schema:"ReplayPolicy"`
	//ReplayStatus        string                 `json:"ReplayStatus" schema:"ReplayStatus"`
}

func NewUnsubscribeRequest() *UnsubscribeRequest {
	return &UnsubscribeRequest{}
}

type UnsubscribeRequest struct {
	SubscriptionArn string `json:"SubscriptionArn" schema:"SubscriptionArn"`
}

func (r *UnsubscribeRequest) SetAttributesFromForm(values url.Values) {}

func NewPublishRequest() *PublishRequest {
	return &PublishRequest{}
}

type PublishRequest struct {
	Message                string                      `json:"Message" schema:"Message"`
	MessageAttributes      map[string]MessageAttribute `json:"MessageAttributes" schema:"MessageAttributes"`
	MessageDeduplicationId string                      `json:"MessageDeduplicationId" schema:"MessageDeduplicationId"` // Not implemented
	MessageGroupId         string                      `json:"MessageGroupId" schema:"MessageGroupId"`                 // Not implemented
	MessageStructure       string                      `json:"MessageStructure" schema:"MessageStructure"`
	PhoneNumber            string                      `json:"PhoneNumber" schema:"PhoneNumber"` // Not implemented
	Subject                string                      `json:"Subject" schema:"Subject"`
	TargetArn              string                      `json:"TargetArn" schema:"TargetArn"` // Not implemented
	TopicArn               string                      `json:"TopicArn" schema:"TopicArn"`
}

func (r *PublishRequest) SetAttributesFromForm(values url.Values) {
	r.MessageAttributes = parseMessageAttributes(values, "MessageAttributes.entry")
}

// Satisfy the AbstractPublishEntry interface
func (r *PublishRequest) GetMessage() string {
	return r.Message
}

func (r *PublishRequest) GetMessageAttributes() map[string]MessageAttribute {
	return r.MessageAttributes
}

func (r *PublishRequest) GetMessageStructure() string {
	return r.MessageStructure
}

func (r *PublishRequest) GetSubject() string {
	return r.Subject
}

// ListTopics

func NewListTopicsRequest() *ListTopicsRequest {
	return &ListTopicsRequest{}
}

type ListTopicsRequest struct {
	NextToken string `json:"NextToken" schema:"NextToken"` // not implemented
}

func (r *ListTopicsRequest) SetAttributesFromForm(values url.Values) {}

// DeleteTopicV1

func NewDeleteTopicRequest() *DeleteTopicRequest {
	return &DeleteTopicRequest{}
}

type DeleteTopicRequest struct {
	TopicArn string `json:"TopicArn" schema:"TopicArn"`
}

func (r *DeleteTopicRequest) SetAttributesFromForm(values url.Values) {}

// ListSubscriptionsV1

func NewListSubscriptionsRequest() *ListSubscriptionsRequest {
	return &ListSubscriptionsRequest{}
}

type ListSubscriptionsRequest struct {
	NextToken string `json:"NextToken" schema:"NextToken"` // not implemented
}

func (r *ListSubscriptionsRequest) SetAttributesFromForm(values url.Values) {}

// Get Subscription Attributes V1
func NewGetSubscriptionAttributesRequest() *GetSubscriptionAttributesRequest {
	return &GetSubscriptionAttributesRequest{}
}

type GetSubscriptionAttributesRequest struct {
	SubscriptionArn string `json:"SubscriptionArn" schema:"SubscriptionArn"`
}

func (r *GetSubscriptionAttributesRequest) SetAttributesFromForm(values url.Values) {}

// SetSubscriptionAttributes

func NewSetSubscriptionAttributesRequest() *SetSubscriptionAttributesRequest {
	return &SetSubscriptionAttributesRequest{}
}

// Ref: https://docs.aws.amazon.com/sns/latest/api/API_SetSubscriptionAttributes.html
type SetSubscriptionAttributesRequest struct {
	SubscriptionArn string `json:"SubscriptionArn" schema:"SubscriptionArn"`
	AttributeName   string `json:"AttributeName" schema:"AttributeName"`
	AttributeValue  string `json:"AttributeValue" schema:"AttributeValue"`
}

func (r *SetSubscriptionAttributesRequest) SetAttributesFromForm(values url.Values) {}

// List Subscriptions By Topic

func NewListSubscriptionsByTopicRequest() *ListSubscriptionsByTopicRequest {
	return &ListSubscriptionsByTopicRequest{}
}

type ListSubscriptionsByTopicRequest struct {
	NextToken string `json:"NextToken" schema:"NextToken"` // not implemented
	TopicArn  string `json:"TopicArn" schema:"TopicArn"`
}

func (r *ListSubscriptionsByTopicRequest) SetAttributesFromForm(values url.Values) {}

// Confirm Subscription V1

func NewConfirmSubscriptionRequest() *ConfirmSubscriptionRequest {
	return &ConfirmSubscriptionRequest{}
}

type ConfirmSubscriptionRequest struct {
	AuthenticateOnUnsubscribe bool   `json:"AuthenticateOnUnsubscribe" schema:"AuthenticateOnUnsubscribe"` // not implemented
	TopicArn                  string `json:"TopicArn" schema:"TopicArn"`
	Token                     string `json:"Token" schema:"Token"`
}

func (r *ConfirmSubscriptionRequest) SetAttributesFromForm(values url.Values) {}

func NewPublishBatchRequest() *PublishBatchRequest {
	return &PublishBatchRequest{}
}

type PublishBatchRequest struct {
	PublishBatchRequestEntries PublishBatchRequestEntries `json:"PublishBatchRequestEntries" schema:"PublishBatchRequestEntries"`
	TopicArn                   string                     `json:"TopicArn" schema:"TopicArn"`
}

func (r *PublishBatchRequest) SetAttributesFromForm(values url.Values) {
	for entryIndex, entry := range r.PublishBatchRequestEntries.Member {
		if entry == nil {
			// The form values are 1-indexed; at the point that this is called, the first element in the
			// list of requests will be nil.
			continue
		}
		entry.MessageAttributes = parseMessageAttributes(values, fmt.Sprintf("PublishBatchRequestEntries.member.%d.MessageAttributes.entry", entryIndex))
	}
}

type PublishBatchRequestEntries struct {
	Member []*PublishBatchRequestEntry `json:"member" schema:"member"`
}

type PublishBatchRequestEntry struct {
	ID                     string                      `json:"Id" schema:"Id"`
	Message                string                      `json:"Message" schema:"Message"`
	MessageAttributes      map[string]MessageAttribute `json:"MessageAttributes" schema:"MessageAttributes"`           // Not implemented
	MessageDeduplicationId string                      `json:"MessageDeduplicationId" schema:"MessageDeduplicationId"` // Not implemented
	MessageGroupId         string                      `json:"MessageGroupId" schema:"MessageGroupId"`                 // Not implemented
	MessageStructure       string                      `json:"MessageStructure" schema:"MessageStructure"`
	Subject                string                      `json:"Subject" schema:"Subject"`
}

// Satisfy the AbstractPublishEntry interface
func (r *PublishBatchRequestEntry) GetMessage() string {
	return r.Message
}

func (r *PublishBatchRequestEntry) GetMessageAttributes() map[string]MessageAttribute {
	return r.MessageAttributes
}

func (r *PublishBatchRequestEntry) GetMessageStructure() string {
	return r.MessageStructure
}

func (r *PublishBatchRequestEntry) GetSubject() string {
	return r.Subject
}
