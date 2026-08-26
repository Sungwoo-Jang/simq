package api

import (
	"encoding/json"
	"net/http"
)

func writeError(response http.ResponseWriter, status int, code, message, requestID string) {
	writeJSON(response, status, errorEnvelope{
		Error: errorBody{
			Code:    code,
			Message: message,
		},
		RequestID: requestID,
	})
}

func writeJSON(response http.ResponseWriter, status int, body interface{}) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(body)
}
