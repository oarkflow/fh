package fh

import (
	"fmt"
	"strings"
)

// EnableSwaggerUI mounts an embedded interactive Swagger UI documentation viewer at prefix and live OpenAPI spec at prefix/openapi.json.
func (a *App) EnableSwaggerUI(prefix string) *App {
	if prefix == "" {
		prefix = "/docs"
	}
	prefix = "/" + strings.Trim(prefix, "/")
	specURL := prefix + "/openapi.json"

	a.Get(specURL, func(c Ctx) error {
		return c.JSON(a.OpenAPISpec())
	})

	html := fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>fh API Documentation</title>
  <link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
  <style>
    html { box-sizing: border-box; overflow: -moz-scrollbars-vertical; overflow-y: scroll; }
    *, *:before, *:after { box-sizing: inherit; }
    body { margin:0; background: #fafafa; }
  </style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" charset="UTF-8"></script>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-standalone-preset.js" charset="UTF-8"></script>
  <script>
    window.onload = function() {
      window.ui = SwaggerUIBundle({
        url: "%s",
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIStandalonePreset
        ],
        plugins: [
          SwaggerUIBundle.plugins.DownloadUrl
        ],
        layout: "StandaloneLayout"
      });
    };
  </script>
</body>
</html>`, specURL)

	a.Get(prefix, func(c Ctx) error {
		c.Type("text/html; charset=utf-8")
		return c.SendString(html)
	})

	return a
}
