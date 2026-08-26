package queue

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const maxMessageAttributes = 10

var queueAttributeNames = []string{
	"ContentBasedDeduplication",
	"DelaySeconds",
	"FifoQueue",
	"MessageRetentionPeriod",
	"Policy",
	"QueueArn",
	"RedrivePolicy",
	"VisibilityTimeout",
}

func ValidateMessageAttributes(body string, attributes map[string]MessageAttribute) error {
	return validateMessageAttributes(body, attributes, true)
}

func MessageLogicalSize(body string, attributes map[string]MessageAttribute) int {
	total := len(body)
	for name, attribute := range attributes {
		total += len(name) + len(attribute.DataType) + len(attribute.StringValue) + len(attribute.BinaryValue)
	}
	return total
}

func MessageAttributesMD5(attributes map[string]MessageAttribute) (string, error) {
	if len(attributes) == 0 {
		return "", nil
	}
	if err := validateMessageAttributes("", attributes, false); err != nil {
		return "", err
	}
	names := make([]string, 0, len(attributes))
	for name := range attributes {
		names = append(names, name)
	}
	sort.Strings(names)
	hash := md5.New()
	var length [4]byte
	writePart := func(value []byte) {
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(value)
	}
	for _, name := range names {
		attribute := attributes[name]
		base, _ := messageAttributeBaseType(attribute.DataType)
		writePart([]byte(name))
		writePart([]byte(attribute.DataType))
		if base == "Binary" {
			_, _ = hash.Write([]byte{2})
			writePart(attribute.BinaryValue)
		} else {
			_, _ = hash.Write([]byte{1})
			writePart([]byte(attribute.StringValue))
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func validateMessageAttributes(body string, attributes map[string]MessageAttribute, checkTotal bool) error {
	if checkTotal && (len(body) < 1 || !utf8.ValidString(body)) {
		return invalidRequest("MessageBody must be a non-empty valid UTF-8 string")
	}
	if len(attributes) > maxMessageAttributes {
		return invalidRequest("MessageAttributes must contain at most 10 entries")
	}
	total := len(body)
	for name, attribute := range attributes {
		if err := validateMessageAttributeName(name); err != nil {
			return err
		}
		base, err := messageAttributeBaseType(attribute.DataType)
		if err != nil {
			return err
		}
		total += len(name) + len(attribute.DataType)
		switch base {
		case "String":
			if attribute.StringValue == "" || !utf8.ValidString(attribute.StringValue) || attribute.BinaryValue != nil {
				return invalidRequest("String message attributes require exactly one non-empty StringValue")
			}
			total += len(attribute.StringValue)
		case "Number":
			if attribute.StringValue == "" || attribute.BinaryValue != nil || !validMessageAttributeNumber(attribute.StringValue) {
				return invalidRequest("Number message attributes require a valid non-empty StringValue")
			}
			total += len(attribute.StringValue)
		case "Binary":
			if attribute.StringValue != "" || len(attribute.BinaryValue) == 0 {
				return invalidRequest("Binary message attributes require exactly one non-empty BinaryValue")
			}
			total += len(attribute.BinaryValue)
		}
	}
	if checkTotal && (total < 1 || total > MaxMessageBytes) {
		return invalidRequest("MessageBody and MessageAttributes must total between 1 byte and 1 MiB")
	}
	return nil
}

func validateMessageAttributeName(name string) error {
	if len(name) < 1 || len(name) > 256 {
		return invalidRequest("message attribute names must be between 1 and 256 bytes")
	}
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "aws.") || strings.HasPrefix(lower, "amazon.") || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") || strings.Contains(name, "..") {
		return invalidRequest("message attribute name is reserved or has invalid periods")
	}
	for index := 0; index < len(name); index++ {
		character := name[index]
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '_' || character == '-' || character == '.' {
			continue
		}
		return invalidRequest("message attribute name contains an invalid character")
	}
	return nil
}

func messageAttributeBaseType(dataType string) (string, error) {
	if len(dataType) < 1 || len(dataType) > 256 {
		return "", invalidRequest("DataType must be between 1 and 256 bytes")
	}
	parts := strings.Split(dataType, ".")
	if parts[0] != "String" && parts[0] != "Number" && parts[0] != "Binary" {
		return "", invalidRequest("DataType must use String, Number, or Binary")
	}
	for _, suffix := range parts[1:] {
		if suffix == "" {
			return "", invalidRequest("DataType custom suffix must not be empty")
		}
		for index := 0; index < len(suffix); index++ {
			character := suffix[index]
			if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '_' || character == '-' {
				continue
			}
			return "", invalidRequest("DataType custom suffix contains an invalid character")
		}
	}
	return parts[0], nil
}

func validMessageAttributeNumber(value string) bool {
	if value == "" || strings.TrimSpace(value) != value {
		return false
	}
	index := 0
	if value[index] == '+' || value[index] == '-' {
		index++
		if index == len(value) {
			return false
		}
	}
	exponentIndex := strings.IndexAny(value[index:], "eE")
	mantissaEnd := len(value)
	exponent := 0
	if exponentIndex >= 0 {
		exponentIndex += index
		mantissaEnd = exponentIndex
		exponentText := value[exponentIndex+1:]
		if exponentText == "" || len(exponentText) > 6 {
			return false
		}
		parsed, err := strconv.Atoi(exponentText)
		if err != nil {
			return false
		}
		exponent = parsed
		if strings.ContainsAny(exponentText, "eE") {
			return false
		}
	}
	mantissa := value[index:mantissaEnd]
	dot := strings.IndexByte(mantissa, '.')
	integerDigits := len(mantissa)
	if dot >= 0 {
		if strings.IndexByte(mantissa[dot+1:], '.') >= 0 || dot == 0 && len(mantissa) == 1 || dot == len(mantissa)-1 {
			return false
		}
		integerDigits = dot
	}
	digits := make([]byte, 0, len(mantissa))
	for position := 0; position < len(mantissa); position++ {
		if mantissa[position] == '.' {
			continue
		}
		if mantissa[position] < '0' || mantissa[position] > '9' {
			return false
		}
		digits = append(digits, mantissa[position])
	}
	if len(digits) == 0 {
		return false
	}
	first := 0
	for first < len(digits) && digits[first] == '0' {
		first++
	}
	if first == len(digits) {
		return true
	}
	last := len(digits)
	for last > first && digits[last-1] == '0' {
		last--
	}
	if last-first > 38 {
		return false
	}
	adjustedExponent := exponent + integerDigits - first - 1
	if adjustedExponent < -128 || adjustedExponent > 126 {
		return false
	}
	if adjustedExponent == 126 {
		if digits[first] != '1' {
			return false
		}
		for position := first + 1; position < len(digits); position++ {
			if digits[position] != '0' {
				return false
			}
		}
	}
	return true
}

func cloneMessageAttribute(attribute MessageAttribute) MessageAttribute {
	attribute.BinaryValue = append([]byte(nil), attribute.BinaryValue...)
	return attribute
}

func cloneMessageAttributes(attributes map[string]MessageAttribute) map[string]MessageAttribute {
	if len(attributes) == 0 {
		return nil
	}
	result := make(map[string]MessageAttribute, len(attributes))
	for name, attribute := range attributes {
		result[name] = cloneMessageAttribute(attribute)
	}
	return result
}

func cloneMessage(message Message) Message {
	message.MessageAttributes = cloneMessageAttributes(message.MessageAttributes)
	return message
}

func messageAttributeSelection(names []string) (bool, map[string]struct{}, error) {
	selected := make(map[string]struct{}, len(names))
	all := false
	for _, name := range names {
		if name == "All" {
			if all || len(names) != 1 {
				return false, nil, invalidRequest("All must be the only MessageAttributeNames entry")
			}
			all = true
			continue
		}
		if strings.Contains(name, "*") {
			return false, nil, invalidRequest("message attribute wildcards are not supported")
		}
		if err := validateMessageAttributeName(name); err != nil {
			return false, nil, err
		}
		if _, exists := selected[name]; exists {
			return false, nil, invalidRequest("MessageAttributeNames entries must be unique")
		}
		selected[name] = struct{}{}
	}
	return all, selected, nil
}

func projectMessageAttributes(message Message, all bool, selected map[string]struct{}) (Message, error) {
	projected := make(map[string]MessageAttribute)
	for name, attribute := range message.MessageAttributes {
		_, wanted := selected[name]
		if all || wanted {
			projected[name] = cloneMessageAttribute(attribute)
		}
	}
	if len(projected) == 0 {
		projected = nil
	}
	message.MessageAttributes = projected
	digest, err := MessageAttributesMD5(projected)
	if err != nil {
		return Message{}, err
	}
	message.MD5OfMessageAttributes = digest
	return message, nil
}

func queueAttributeSelection(names []string) (bool, map[string]struct{}, error) {
	selected := make(map[string]struct{}, len(names))
	all := false
	for _, name := range names {
		if name == "All" {
			if all || len(names) != 1 {
				return false, nil, invalidRequest("All must be the only AttributeNames entry")
			}
			all = true
			continue
		}
		if _, exists := selected[name]; exists {
			return false, nil, invalidRequest("AttributeNames entries must be unique")
		}
		supported := false
		for _, candidate := range queueAttributeNames {
			if name == candidate {
				supported = true
				break
			}
		}
		if !supported {
			return false, nil, invalidRequest("unsupported queue attribute %q", name)
		}
		selected[name] = struct{}{}
	}
	return all, selected, nil
}
