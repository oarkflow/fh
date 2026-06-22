package main

import (
	"io"
	"log"
	"time"

	"github.com/oarkflow/fastjson"
	"github.com/oarkflow/fh"
	fiber "github.com/oarkflow/fh"
)

func init() {
	fh.MustSetJSONEngine(fh.JSONEngineFuncSet{
		MarshalFunc:   fastjson.Marshal,
		UnmarshalFunc: fastjson.Unmarshal,
		NewEncoderFunc: func(w io.Writer) fh.JSONEncoder {
			return fastjson.NewEncoder(w)
		},
		NewDecoderFunc: func(r io.Reader) fh.JSONDecoder {
			return fastjson.NewDecoder(r)
		},
		ValidFunc: fastjson.Valid,
	})
}

type Address struct {
	Street  string `json:"street"`
	City    string `json:"city"`
	Country string `json:"country"`
	ZipCode string `json:"zip_code"`
}

type Attachment struct {
	Name        string `json:"name"`
	ContentType string `json:"content_type"`
	Data        []byte `json:"data"` // JSON should encode/decode this as base64 string
	Size        int64  `json:"size"`
}

type Request struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Email       string            `json:"email"`
	Age         int               `json:"age"`
	Active      bool              `json:"active"`
	Score       float64           `json:"score"`
	RawToken    []byte            `json:"raw_token"` // base64 in JSON
	Tags        []string          `json:"tags"`
	Roles       []string          `json:"roles"`
	Metadata    map[string]string `json:"metadata"`
	Address     Address           `json:"address"`
	Attachments []Attachment      `json:"attachments"`
	CreatedAt   time.Time         `json:"created_at"`
}

type Response struct {
	Message     string       `json:"message"`
	RequestID   string       `json:"request_id"`
	User        Request      `json:"user"`
	ServerTime  time.Time    `json:"server_time"`
	BytesEchoed int          `json:"bytes_echoed"`
	OK          bool         `json:"ok"`
	Extra       fiber.Map    `json:"extra"`
}

// Fiber instance
func main() {
	app := fiber.New()

	app.Get("/", hello)
	app.Get("/json", jsonHandler)

	// fixed spelling too; keeping old typo route for compatibility
	app.Post("/unmarshal", unmarshalHandler)
	app.Post("/umarshal", unmarshalHandler)

	log.Fatal(app.Listen(":3000"))
}

func unmarshalHandler(c *fiber.Ctx) error {
	var req Request

	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	bytesEchoed := len(req.RawToken)
	for _, att := range req.Attachments {
		bytesEchoed += len(att.Data)
	}

	return c.JSON(Response{
		Message:     "Hello, " + req.Name + " 👋!",
		RequestID:   req.ID,
		User:        req,
		ServerTime:  time.Now().UTC(),
		BytesEchoed: bytesEchoed,
		OK:          true,
		Extra: fiber.Map{
			"tag_count":        len(req.Tags),
			"role_count":       len(req.Roles),
			"attachment_count": len(req.Attachments),
			"metadata_count":   len(req.Metadata),
		},
	})
}

func jsonHandler(c *fiber.Ctx) error {
	resp := Response{
		Message:   "Hello, World 👋!",
		RequestID: "resp-001",
		ServerTime: time.Now().UTC(),
		OK:        true,
		User: Request{
			ID:       "user-123",
			Name:     "Alice Johnson",
			Email:    "alice@example.com",
			Age:      30,
			Active:   true,
			Score:    98.75,
			RawToken: []byte("raw-token-bytes-123"),
			Tags:     []string{"fastjson", "fh", "benchmark"},
			Roles:    []string{"admin", "developer"},
			Metadata: map[string]string{
				"env":     "local",
				"version": "v1",
				"engine":  "fastjson",
			},
			Address: Address{
				Street:  "Main Street 10",
				City:    "Kathmandu",
				Country: "Nepal",
				ZipCode: "44600",
			},
			Attachments: []Attachment{
				{
					Name:        "hello.txt",
					ContentType: "text/plain",
					Data:        []byte("hello from bytes"),
					Size:        int64(len("hello from bytes")),
				},
				{
					Name:        "payload.bin",
					ContentType: "application/octet-stream",
					Data:        []byte{1, 2, 3, 4, 5, 255},
					Size:        6,
				},
			},
			CreatedAt: time.Now().UTC(),
		},
		BytesEchoed: len([]byte("raw-token-bytes-123")) + len([]byte("hello from bytes")) + 6,
		Extra: fiber.Map{
			"note":       "This response tests strings, bytes, structs, slices, maps, bools, numbers, and time.",
			"byte_field": []byte("extra-byte-data"),
		},
	}

	return c.JSON(resp)
}

func hello(c *fiber.Ctx) error {
	return c.SendString("Hello, World 👋!")
}
