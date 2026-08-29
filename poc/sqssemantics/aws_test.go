package sqssemantics

import (
	"context"
	"testing"
)

func TestAWSBackendFailsClosedWithoutMutationOptIn(t *testing.T) {
	if _, err := NewAWSBackend(context.Background(), AWSOptions{Region: "ap-northeast-2"}); err == nil {
		t.Fatal("AWS backend initialized without mutation opt-in")
	}
}

func TestAWSBackendRequiresExplicitRegionBeforeLoadingCredentials(t *testing.T) {
	if _, err := NewAWSBackend(context.Background(), AWSOptions{AllowMutations: true}); err == nil {
		t.Fatal("AWS backend initialized without region")
	}
}

func TestRedrivePolicyMatchesSemanticFields(t *testing.T) {
	if !redrivePolicyMatches(`{"maxReceiveCount":"2","deadLetterTargetArn":"arn:test"}`, "arn:test", 2) {
		t.Fatal("reordered valid redrive policy did not match")
	}
	if redrivePolicyMatches(`{"maxReceiveCount":"3","deadLetterTargetArn":"arn:test"}`, "arn:test", 2) {
		t.Fatal("wrong receive count matched")
	}
}
