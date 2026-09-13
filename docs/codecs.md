# Codec System

fh has a pluggable body codec system that automatically selects the appropriate decoder based on the request's `Content-Type` header.

## Built-in Codecs

| Codec | Content-Type | Target Types |
|-------|-------------|--------------|
| **JSON** | `application/json`, `text/json`, `+json` | Any struct, map, slice |
| **XML** | `application/xml`, `text/xml`, `+xml` | Any struct |
| **Form** | `application/x-www-form-urlencoded` | Struct with `form:` tags, map |
| **Multipart** | `multipart/form-data` | Struct with `form:` tags, `*fh.MultipartForm`, maps |
| **CSV** | `text/csv` | `[][]string`, `[]map[string]string` |
| **NDJSON** | `application/x-ndjson` | Any struct (newline-delimited JSON) |
| **Text** | `text/plain`, `text/html`, `text/css`, `text/javascript`, `application/javascript`, `application/graphql`, `application/sql` | `*string`, `*[]byte` |
| **Binary** | `application/octet-stream`, `application/pdf`, `application/zip`, `image/png`, `image/jpeg`, `image/gif`, `image/webp` | `*[]byte` |

## Usage

### Auto-Detect

```go
var user User
err := c.BodyParser(&user)
// Content-Type detection is automatic
```

### Content-Type selection

```go
// The client must send Content-Type: application/json.
var user User
if err := c.BodyParser(&user); err != nil {
    return fh.NewHTTPError(fh.StatusBadRequest, "INVALID_BODY", err.Error())
}
```

### Defensive limits

```go
// Configure once during process startup. Zero-valued fields retain defaults.
fh.SetCodecOptions(fh.CodecOptions{MaxFormPairs: 5000})
```

## JSON Codec

```go
var user User
c.BodyParser(&user)

// Install a custom JSONEngine during process startup.
fh.MustSetJSONEngine(myJSONEngine)
```

**Struct tags:** `json:""` (standard Go)

## XML Codec

```go
type Person struct {
    XMLName xml.Name `xml:"person"`
    Name    string   `xml:"name"`
    Age     int      `xml:"age"`
}

var p Person
c.BodyParser(&p)
```

**Struct tags:** `xml:""` (standard Go)

## Form Codec

Supports bracket-notation nesting for maps and slices.

```go
type Filter struct {
    Page  int      `form:"page"`
    Sort  string   `form:"sort"`
    Tags  []string `form:"tags"`
}

var filter Filter
c.BodyParser(&filter)
```

**Form data:** `page=1&sort=asc&tags[]=go&tags[]=http`

**Struct tags:** `form:""`

## Multipart Codec

```go
type UploadForm struct {
    Name   string             `form:"name"`
    Avatar fh.MultipartFile   `form:"avatar"`
    Photos []fh.MultipartFile `form:"photos"`
}

var form UploadForm
c.BodyParser(&form)

// Save uploaded file
form.Avatar.Save("/uploads/avatar.jpg")

// Access file info
form.Avatar.FileName  // original filename
form.Avatar.Size      // file size
form.Avatar.Header    // multipart.FileHeader
```

**`MultipartFile` methods:**

```go
file.Save(dst string) error          // save to file
file.Open() io.ReadCloser            // open an in-memory reader
```

## CSV Codec

```go
// As [][]string
var rows [][]string
c.BodyParser(&rows)

// As []map[string]string (first row as headers)
var records []map[string]string
c.BodyParser(&records)
```

## NDJSON Codec

Newline-delimited JSON. Each line is parsed independently.

```go
var users []User
c.BodyParser(&users)
```

## Text Codec

```go
var text string
c.BodyParser(&text)

var bytes []byte
c.BodyParser(&bytes)
```

## Binary Codec

```go
var data []byte
c.BodyParser(&data)
```

---

## Custom Codecs

Register a custom codec for new content types.

```go
import "github.com/oarkflow/fh"

type YAMLCodec struct{}

func (c *YAMLCodec) ContentType() string { return "application/yaml" }

func (c *YAMLCodec) Unmarshal(data []byte, v any) error {
    return yaml.Unmarshal(data, v)
}

func (c *YAMLCodec) Marshal(v any) ([]byte, error) {
    return yaml.Marshal(v)
}

func init() {
    fh.RegisterCodec(&YAMLCodec{})
    fh.RegisterCodecAlias("text/yaml", &YAMLCodec{})
}
```

### Codec Interfaces

```go
// Basic codec (read-only)
type Codec interface {
    ContentType() string
    Unmarshal(data []byte, v any) error
}

// Codec with encoding support
type EncoderCodec interface {
    Codec
    Marshal(v any) ([]byte, error)
}

// Content-type aware codec
type ContentTypeAwareCodec interface {
    Codec
    UnmarshalWithContentType(data []byte, contentType string, v any) error
}

// Resettable codec (for pooling)
type ResettableCodec interface {
    Codec
    Reset()
}
```

## Content-Type Detection

The codec is selected by normalized media type. Register additional exact media
types with `RegisterCodecAlias`. The built-in registry includes JSON/XML suffix
handling such as `application/problem+json` and `application/atom+xml`.

Priority order for content-type matching:
1. Exact match
2. Type/subtype match (e.g., `application/json`)
3. Suffix match (e.g., `+json`)

---

## Default Limits

| Limit | Default | Description |
|-------|---------|-------------|
| `MaxFormPairs` | 10,000 | Max form key-value pairs |
| `MaxFormKeyBytes` | 4KB | Max form key length |
| `MaxFormValueBytes` | 4MB | Max form value length |
| `MaxFormDepth` | 32 | Max nesting depth |
| `MaxMultipartParts` | 10,000 | Max multipart parts |
| `MaxMultipartFieldSize` | 8MB | Max multipart field |
| `MaxMultipartFileSize` | 64MB | Max file upload |
| `MaxNDJSONLineBytes` | 8MB | Max NDJSON line |
| `MaxCSVRecordBytes` | 8MB | Max CSV record |
