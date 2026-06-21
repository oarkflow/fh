package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"dagflow/pkg/dagflow"
)

type EmailRequest struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}
type EmailValidationResult struct {
	Request EmailRequest `json:"request"`
	Valid   bool         `json:"valid"`
	Reason  string       `json:"reason,omitempty"`
}
type EmailSendResult struct {
	Request   EmailRequest `json:"request"`
	Sent      bool         `json:"sent"`
	MessageID string       `json:"message_id"`
}
type StoreResult struct {
	Request   EmailRequest `json:"request"`
	Status    string       `json:"status"`
	MessageID string       `json:"message_id,omitempty"`
	Reason    string       `json:"reason,omitempty"`
	StoredAt  time.Time    `json:"stored_at"`
}

func RegisterExampleHandlers(e *dagflow.Engine) {
	e.Register("receive_email", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		var req EmailRequest
		b, _ := json.Marshal(input)
		if err := json.Unmarshal(b, &req); err != nil {
			return nil, err
		}
		return req, nil
	})
	e.Register("validate_email", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		req := toEmailRequest(input)
		if req.To == "" || !strings.Contains(req.To, "@") || !strings.Contains(req.To, ".") {
			return EmailValidationResult{Request: req, Valid: false, Reason: "invalid recipient email"}, nil
		}
		if strings.TrimSpace(req.Subject) == "" || strings.TrimSpace(req.Body) == "" {
			return EmailValidationResult{Request: req, Valid: false, Reason: "subject and body are required"}, nil
		}
		return EmailValidationResult{Request: req, Valid: true}, nil
	})
	e.Register("send_email", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		v := toEmailValidation(input)
		if !v.Valid {
			return nil, errors.New("cannot send invalid email")
		}
		if strings.Contains(v.Request.Subject, "fail") {
			return nil, errors.New("simulated send failure because subject contains fail")
		}
		time.Sleep(75 * time.Millisecond)
		return EmailSendResult{Request: v.Request, Sent: true, MessageID: dagflow.NewID("msg")}, nil
	})
	e.Register("store_message", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		switch x := normalizeEmailResult(input).(type) {
		case EmailSendResult:
			return StoreResult{Request: x.Request, Status: "sent_stored", MessageID: x.MessageID, StoredAt: time.Now()}, nil
		case EmailValidationResult:
			return StoreResult{Request: x.Request, Status: "invalid_stored", Reason: x.Reason, StoredAt: time.Now()}, nil
		case map[string]any:
			return map[string]any{"status": "error_stored", "source": x, "stored_at": time.Now()}, nil
		default:
			return nil, fmt.Errorf("store_message unsupported input %T", input)
		}
	})
	e.Register("notify_success", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		log.Printf("notify success task=%s", rc.TaskID)
		return map[string]any{"ok": true, "message": "email sent and stored", "data": input}, nil
	})
	e.Register("notify_failure", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		log.Printf("notify failure task=%s", rc.TaskID)
		return map[string]any{"ok": false, "message": "email rejected/stored", "data": input}, nil
	})
	e.Register("audit", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		return map[string]any{"audited": true, "at": time.Now(), "payload": input, "previous_node": rc.PreviousNode}, nil
	})
	e.Register("audit_event", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		return map[string]any{"audited": true, "at": time.Now(), "payload": input, "previous_node": rc.PreviousNode, "task_id": rc.TaskID}, nil
	})
	e.Register("outbox_publish", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		if s, ok := e.Store().(dagflow.ExtendedStore); ok {
			topic := fmt.Sprint(rc.NodeParams["topic"])
			if topic == "" || topic == "<nil>" {
				topic = "workflow.event"
			}
			ev := dagflow.OutboxEvent{ID: dagflow.NewID("outbox"), TaskID: rc.TaskID, NodeID: rc.NodeID, Topic: topic, Payload: input, Status: "pending", NextRunAt: time.Now(), CreatedAt: time.Now(), UpdatedAt: time.Now()}
			if err := s.SaveOutbox(ev); err != nil {
				return nil, err
			}
			return map[string]any{"outbox_id": ev.ID, "topic": ev.Topic, "queued": true, "payload": input}, nil
		}
		return map[string]any{"queued": false, "payload": input}, nil
	})
	e.Register("identity", func(rc *dagflow.ExecutionContext, input any) (any, error) { return input, nil })
	e.Register("uppercase", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		return strings.ToUpper(fmt.Sprint(input)), nil
	})
	e.Register("check_inventory", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		return map[string]any{"check": "inventory", "ok": true, "input": input}, nil
	})
	e.Register("check_payment", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		return map[string]any{"check": "payment", "ok": true, "input": input}, nil
	})
	e.Register("check_fraud", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		return map[string]any{"check": "fraud", "ok": true, "input": input}, nil
	})
	e.Register("approve_order", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		return map[string]any{"approved": true, "checks": input, "approved_at": time.Now()}, nil
	})
	e.Register("api_response", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		return map[string]any{"success": true, "data": input}, nil
	})
	e.Register("receive_batch", func(rc *dagflow.ExecutionContext, input any) (any, error) { xs, err := toSlice(input); return xs, err })
	e.Register("uppercase_one", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		time.Sleep(20 * time.Millisecond)
		return strings.ToUpper(fmt.Sprint(input)), nil
	})
	e.Register("finalize_batch", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		xs, _ := toSlice(input)
		return map[string]any{"items": input, "count": len(xs), "previous_results": rc.NodeResults}, nil
	})
	e.Register("approval_result", func(rc *dagflow.ExecutionContext, input any) (any, error) {
		return map[string]any{"approved_payload": input, "approved_at": time.Now()}, nil
	})
}

func toEmailRequest(v any) EmailRequest {
	if x, ok := v.(EmailRequest); ok {
		return x
	}
	var out EmailRequest
	b, _ := json.Marshal(v)
	_ = json.Unmarshal(b, &out)
	return out
}
func toEmailValidation(v any) EmailValidationResult {
	if x, ok := v.(EmailValidationResult); ok {
		return x
	}
	var out EmailValidationResult
	b, _ := json.Marshal(v)
	_ = json.Unmarshal(b, &out)
	return out
}
func normalizeEmailResult(v any) any {
	if _, ok := v.(EmailSendResult); ok {
		return v
	}
	if _, ok := v.(EmailValidationResult); ok {
		return v
	}
	b, _ := json.Marshal(v)
	var send EmailSendResult
	if json.Unmarshal(b, &send) == nil && send.Request.To != "" && send.MessageID != "" {
		return send
	}
	var val EmailValidationResult
	if json.Unmarshal(b, &val) == nil && val.Request.To != "" {
		return val
	}
	var m map[string]any
	if json.Unmarshal(b, &m) == nil && len(m) > 0 {
		return m
	}
	return v
}
func toSlice(v any) ([]any, error) {
	if xs, ok := v.([]any); ok {
		return xs, nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, fmt.Errorf("expected slice/array, got %T", v)
	}
	out := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = rv.Index(i).Interface()
	}
	return out, nil
}
