# validate middleware

Applies the core validation engine to non-typed handlers. Typed routes already
perform tag and `fh.Validator` validation automatically.

```go
type CreateUser struct {
    Name string `json:"name" validate:"required,min=2"`
}

app.Post("/users",
    validate.Body(func() any { return &CreateUser{} }),
    func(c fh.Ctx) error {
        return c.SendStatus(fh.StatusCreated)
    },
)
```

`validate.Query(value)` and `validate.Headers(value)` bind and validate query or
header structs. Configure `OnError` for custom validation responses and `Skip`
for explicit bypasses.
