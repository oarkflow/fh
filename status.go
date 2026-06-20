package fh

// ── Status codes ──────────────────────────────────────────────────────────────
const (
	StatusContinue                      = 100
	StatusSwitchingProtocols            = 101
	StatusProcessing                    = 102
	StatusEarlyHints                    = 103
	StatusOK                            = 200
	StatusCreated                       = 201
	StatusAccepted                      = 202
	StatusNonAuthoritativeInformation   = 203
	StatusNoContent                     = 204
	StatusResetContent                  = 205
	StatusPartialContent                = 206
	StatusMultiStatus                   = 207
	StatusAlreadyReported               = 208
	StatusIMUsed                        = 226
	StatusMultipleChoices               = 300
	StatusMovedPermanently              = 301
	StatusFound                         = 302
	StatusSeeOther                      = 303
	StatusNotModified                   = 304
	StatusUseProxy                      = 305
	StatusTemporaryRedirect             = 307
	StatusPermanentRedirect             = 308
	StatusBadRequest                    = 400
	StatusUnauthorized                  = 401
	StatusPaymentRequired               = 402
	StatusForbidden                     = 403
	StatusNotFound                      = 404
	StatusMethodNotAllowed              = 405
	StatusNotAcceptable                 = 406
	StatusProxyAuthenticationRequired   = 407
	StatusRequestTimeout                = 408
	StatusConflict                      = 409
	StatusGone                          = 410
	StatusLengthRequired                = 411
	StatusPreconditionFailed            = 412
	StatusPayloadTooLarge               = 413
	StatusURITooLong                    = 414
	StatusUnsupportedMediaType          = 415
	StatusRangeNotSatisfiable           = 416
	StatusExpectationFailed             = 417
	StatusTeapot                        = 418
	StatusMisdirectedRequest            = 421
	StatusUnprocessableEntity           = 422
	StatusLocked                        = 423
	StatusFailedDependency              = 424
	StatusTooEarly                      = 425
	StatusUpgradeRequired               = 426
	StatusPreconditionRequired          = 428
	StatusTooManyRequests               = 429
	StatusRequestHeaderFieldsTooLarge   = 431
	StatusUnavailableForLegalReasons    = 451
	StatusInternalServerError           = 500
	StatusNotImplemented                = 501
	StatusBadGateway                    = 502
	StatusServiceUnavailable            = 503
	StatusGatewayTimeout                = 504
	StatusHTTPVersionNotSupported       = 505
	StatusVariantAlsoNegotiates         = 506
	StatusInsufficientStorage           = 507
	StatusLoopDetected                  = 508
	StatusNotExtended                   = 510
	StatusNetworkAuthenticationRequired = 511
)

// ── Status reason phrases ─────────────────────────────────────────────────────
const (
	StatusTextContinue                      = "Continue"
	StatusTextSwitchingProtocols            = "Switching Protocols"
	StatusTextProcessing                    = "Processing"
	StatusTextEarlyHints                    = "Early Hints"
	StatusTextOK                            = "OK"
	StatusTextCreated                       = "Created"
	StatusTextAccepted                      = "Accepted"
	StatusTextNonAuthoritativeInformation   = "Non-Authoritative Information"
	StatusTextNoContent                     = "No Content"
	StatusTextResetContent                  = "Reset Content"
	StatusTextPartialContent                = "Partial Content"
	StatusTextMultiStatus                   = "Multi-Status"
	StatusTextAlreadyReported               = "Already Reported"
	StatusTextIMUsed                        = "IM Used"
	StatusTextMultipleChoices               = "Multiple Choices"
	StatusTextMovedPermanently              = "Moved Permanently"
	StatusTextFound                         = "Found"
	StatusTextSeeOther                      = "See Other"
	StatusTextNotModified                   = "Not Modified"
	StatusTextUseProxy                      = "Use Proxy"
	StatusTextTemporaryRedirect             = "Temporary Redirect"
	StatusTextPermanentRedirect             = "Permanent Redirect"
	StatusTextBadRequest                    = "Bad Request"
	StatusTextUnauthorized                  = "Unauthorized"
	StatusTextPaymentRequired               = "Payment Required"
	StatusTextForbidden                     = "Forbidden"
	StatusTextNotFound                      = "Not Found"
	StatusTextMethodNotAllowed              = "Method Not Allowed"
	StatusTextNotAcceptable                 = "Not Acceptable"
	StatusTextProxyAuthenticationRequired   = "Proxy Authentication Required"
	StatusTextRequestTimeout                = "Request Timeout"
	StatusTextConflict                      = "Conflict"
	StatusTextGone                          = "Gone"
	StatusTextLengthRequired                = "Length Required"
	StatusTextPreconditionFailed            = "Precondition Failed"
	StatusTextPayloadTooLarge               = "Payload Too Large"
	StatusTextURITooLong                    = "URI Too Long"
	StatusTextUnsupportedMediaType          = "Unsupported Media Type"
	StatusTextRangeNotSatisfiable           = "Range Not Satisfiable"
	StatusTextExpectationFailed             = "Expectation Failed"
	StatusTextTeapot                        = "I'm a teapot"
	StatusTextMisdirectedRequest            = "Misdirected Request"
	StatusTextUnprocessableEntity           = "Unprocessable Entity"
	StatusTextLocked                        = "Locked"
	StatusTextFailedDependency              = "Failed Dependency"
	StatusTextTooEarly                      = "Too Early"
	StatusTextUpgradeRequired               = "Upgrade Required"
	StatusTextPreconditionRequired          = "Precondition Required"
	StatusTextTooManyRequests               = "Too Many Requests"
	StatusTextRequestHeaderFieldsTooLarge   = "Request Header Fields Too Large"
	StatusTextUnavailableForLegalReasons    = "Unavailable For Legal Reasons"
	StatusTextInternalServerError           = "Internal Server Error"
	StatusTextNotImplemented                = "Not Implemented"
	StatusTextBadGateway                    = "Bad Gateway"
	StatusTextServiceUnavailable            = "Service Unavailable"
	StatusTextGatewayTimeout                = "Gateway Timeout"
	StatusTextHTTPVersionNotSupported       = "HTTP Version Not Supported"
	StatusTextVariantAlsoNegotiates         = "Variant Also Negotiates"
	StatusTextInsufficientStorage           = "Insufficient Storage"
	StatusTextLoopDetected                  = "Loop Detected"
	StatusTextNotExtended                   = "Not Extended"
	StatusTextNetworkAuthenticationRequired = "Network Authentication Required"
)

// StatusReason returns the standard reason phrase for code.
// It returns an empty string for unknown status codes.
func StatusReason(code int) string {
	switch code {
	case StatusContinue:
		return StatusTextContinue
	case StatusSwitchingProtocols:
		return StatusTextSwitchingProtocols
	case StatusProcessing:
		return StatusTextProcessing
	case StatusEarlyHints:
		return StatusTextEarlyHints
	case StatusOK:
		return StatusTextOK
	case StatusCreated:
		return StatusTextCreated
	case StatusAccepted:
		return StatusTextAccepted
	case StatusNonAuthoritativeInformation:
		return StatusTextNonAuthoritativeInformation
	case StatusNoContent:
		return StatusTextNoContent
	case StatusResetContent:
		return StatusTextResetContent
	case StatusPartialContent:
		return StatusTextPartialContent
	case StatusMultiStatus:
		return StatusTextMultiStatus
	case StatusAlreadyReported:
		return StatusTextAlreadyReported
	case StatusIMUsed:
		return StatusTextIMUsed
	case StatusMultipleChoices:
		return StatusTextMultipleChoices
	case StatusMovedPermanently:
		return StatusTextMovedPermanently
	case StatusFound:
		return StatusTextFound
	case StatusSeeOther:
		return StatusTextSeeOther
	case StatusNotModified:
		return StatusTextNotModified
	case StatusUseProxy:
		return StatusTextUseProxy
	case StatusTemporaryRedirect:
		return StatusTextTemporaryRedirect
	case StatusPermanentRedirect:
		return StatusTextPermanentRedirect
	case StatusBadRequest:
		return StatusTextBadRequest
	case StatusUnauthorized:
		return StatusTextUnauthorized
	case StatusPaymentRequired:
		return StatusTextPaymentRequired
	case StatusForbidden:
		return StatusTextForbidden
	case StatusNotFound:
		return StatusTextNotFound
	case StatusMethodNotAllowed:
		return StatusTextMethodNotAllowed
	case StatusNotAcceptable:
		return StatusTextNotAcceptable
	case StatusProxyAuthenticationRequired:
		return StatusTextProxyAuthenticationRequired
	case StatusRequestTimeout:
		return StatusTextRequestTimeout
	case StatusConflict:
		return StatusTextConflict
	case StatusGone:
		return StatusTextGone
	case StatusLengthRequired:
		return StatusTextLengthRequired
	case StatusPreconditionFailed:
		return StatusTextPreconditionFailed
	case StatusPayloadTooLarge:
		return StatusTextPayloadTooLarge
	case StatusURITooLong:
		return StatusTextURITooLong
	case StatusUnsupportedMediaType:
		return StatusTextUnsupportedMediaType
	case StatusRangeNotSatisfiable:
		return StatusTextRangeNotSatisfiable
	case StatusExpectationFailed:
		return StatusTextExpectationFailed
	case StatusTeapot:
		return StatusTextTeapot
	case StatusMisdirectedRequest:
		return StatusTextMisdirectedRequest
	case StatusUnprocessableEntity:
		return StatusTextUnprocessableEntity
	case StatusLocked:
		return StatusTextLocked
	case StatusFailedDependency:
		return StatusTextFailedDependency
	case StatusTooEarly:
		return StatusTextTooEarly
	case StatusUpgradeRequired:
		return StatusTextUpgradeRequired
	case StatusPreconditionRequired:
		return StatusTextPreconditionRequired
	case StatusTooManyRequests:
		return StatusTextTooManyRequests
	case StatusRequestHeaderFieldsTooLarge:
		return StatusTextRequestHeaderFieldsTooLarge
	case StatusUnavailableForLegalReasons:
		return StatusTextUnavailableForLegalReasons
	case StatusInternalServerError:
		return StatusTextInternalServerError
	case StatusNotImplemented:
		return StatusTextNotImplemented
	case StatusBadGateway:
		return StatusTextBadGateway
	case StatusServiceUnavailable:
		return StatusTextServiceUnavailable
	case StatusGatewayTimeout:
		return StatusTextGatewayTimeout
	case StatusHTTPVersionNotSupported:
		return StatusTextHTTPVersionNotSupported
	case StatusVariantAlsoNegotiates:
		return StatusTextVariantAlsoNegotiates
	case StatusInsufficientStorage:
		return StatusTextInsufficientStorage
	case StatusLoopDetected:
		return StatusTextLoopDetected
	case StatusNotExtended:
		return StatusTextNotExtended
	case StatusNetworkAuthenticationRequired:
		return StatusTextNetworkAuthenticationRequired
	default:
		return ""
	}
}
