package werror

import (
	"bytes"
	"fmt"
	"sort"
)

// GenerateErrorString will attempt to pretty print an error depending on its underlying type
// If it is a werror then:
// 1) Each message and params will be groups together on a separate line
// 2) Only the deepest werror stacktrace will be printed
// 3) GenerateErrorString will be called recursively to pretty print underlying errors as well
// If the error implements the fmt.Formatter interface, then it will be printed verbosely
// Otherwise, the error's underlying Error() function will be called and returned
func GenerateErrorString(err error, outputEveryCallingStack bool) string {
	if werror, ok := err.(*werror); ok {
		return generateWerrorString(werror, outputEveryCallingStack)
	}
	if fancy, ok := err.(fmt.Formatter); ok {
		// This is a rich error type, like those produced by github.com/pkg/errors.
		return fmt.Sprintf("%+v", fancy)
	}
	return err.Error()
}

func generateWerrorString(err *werror, outputEveryCallingStack bool) string {
	var buffer bytes.Buffer
	writeMessage(err, &buffer)
	writeParams(err, &buffer)
	writeCause(err, &buffer, outputEveryCallingStack)
	writeStack(err, &buffer, outputEveryCallingStack)
	return buffer.String()
}

func writeMessage(err *werror, buffer *bytes.Buffer) {
	if err.message == "" {
		return
	}
	buffer.WriteString(err.message)
}

func writeParams(err *werror, buffer *bytes.Buffer) {
	safeParams := getSafeParamsAtCurrentLevel(err)
	var safeKeys []string
	for k := range safeParams {
		safeKeys = append(safeKeys, k)
	}
	sort.Strings(safeKeys)
	messageAndParams := err.message != "" && len(safeParams) != 0
	messageOrParams := err.message != "" || len(safeParams) != 0
	if messageAndParams {
		buffer.WriteString(" ")
	}
	for _, safeKey := range safeKeys {
		buffer.WriteString(fmt.Sprintf("%+v:%+v", safeKey, safeParams[safeKey]))
		// If it is not the last param, add a separator
		if !(safeKeys[len(safeKeys)-1] == safeKey) {
			buffer.WriteString(", ")
		}
	}
	if messageOrParams {
		buffer.WriteString("\n")
	}
}

func getSafeParamsAtCurrentLevel(err *werror) map[string]interface{} {
	safeParamsAtThisLevel := make(map[string]interface{}, 0)
	childSafeParams := getChildSafeParams(err)
	for k, v := range err.SafeParams() {
		_, ok := childSafeParams[k]
		if ok {
			continue
		}
		safeParamsAtThisLevel[k] = v
	}
	return safeParamsAtThisLevel
}

func getChildSafeParams(err *werror) map[string]interface{} {
	if err.cause == nil {
		return make(map[string]interface{}, 0)
	}
	causeAsWerror, ok := err.cause.(*werror)
	if !ok {
		return make(map[string]interface{}, 0)
	}
	return causeAsWerror.SafeParams()
}

func writeCause(err *werror, buffer *bytes.Buffer, outputEveryCallingStack bool) {
	if err.cause != nil {
		buffer.WriteString(GenerateErrorString(err.cause, outputEveryCallingStack))
	}
}

func writeStack(err *werror, buffer *bytes.Buffer, outputEveryCallingStack bool) {
	if _, ok := err.cause.(*werror); ok {
		if !outputEveryCallingStack {
			return
		}
	}
	buffer.WriteString(fmt.Sprintf("%+v", err.stack))
}
