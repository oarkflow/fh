package fh

// ── Request / response headers ────────────────────────────────────────────────
const (
	HeaderAccept                          = "Accept"
	HeaderAcceptCharset                   = "Accept-Charset"
	HeaderAcceptEncoding                  = "Accept-Encoding"
	HeaderAcceptLanguage                  = "Accept-Language"
	HeaderAcceptPatch                     = "Accept-Patch"
	HeaderAcceptPost                      = "Accept-Post"
	HeaderAcceptRanges                    = "Accept-Ranges"
	HeaderAge                             = "Age"
	HeaderAllow                           = "Allow"
	HeaderAltSvc                          = "Alt-Svc"
	HeaderAuthorization                   = "Authorization"
	HeaderCacheControl                    = "Cache-Control"
	HeaderClearSiteData                   = "Clear-Site-Data"
	HeaderConnection                      = "Connection"
	HeaderContentDisposition              = "Content-Disposition"
	HeaderContentEncoding                 = "Content-Encoding"
	HeaderContentLanguage                 = "Content-Language"
	HeaderContentLength                   = "Content-Length"
	HeaderContentLocation                 = "Content-Location"
	HeaderContentRange                    = "Content-Range"
	HeaderContentSecurityPolicy           = "Content-Security-Policy"
	HeaderContentSecurityPolicyReportOnly = "Content-Security-Policy-Report-Only"
	HeaderContentType                     = "Content-Type"
	HeaderCookie                          = "Cookie"
	HeaderDate                            = "Date"
	HeaderDigest                          = "Digest"
	HeaderDNT                             = "DNT"
	HeaderEarlyData                       = "Early-Data"
	HeaderETag                            = "ETag"
	HeaderExpect                          = "Expect"
	HeaderExpires                         = "Expires"
	HeaderForwarded                       = "Forwarded"
	HeaderFrom                            = "From"
	HeaderHost                            = "Host"
	HeaderIfMatch                         = "If-Match"
	HeaderIfModifiedSince                 = "If-Modified-Since"
	HeaderIfNoneMatch                     = "If-None-Match"
	HeaderIfRange                         = "If-Range"
	HeaderIfUnmodifiedSince               = "If-Unmodified-Since"
	HeaderKeepAlive                       = "Keep-Alive"
	HeaderLastModified                    = "Last-Modified"
	HeaderLink                            = "Link"
	HeaderLocation                        = "Location"
	HeaderNEL                             = "NEL"
	HeaderOrigin                          = "Origin"
	HeaderPragma                          = "Pragma"
	HeaderPriority                        = "Priority"
	HeaderProxyAuthenticate               = "Proxy-Authenticate"
	HeaderProxyAuthorization              = "Proxy-Authorization"
	HeaderRange                           = "Range"
	HeaderReferer                         = "Referer"
	HeaderReferrerPolicy                  = "Referrer-Policy"
	HeaderRefresh                         = "Refresh"
	HeaderRetryAfter                      = "Retry-After"
	HeaderServer                          = "Server"
	HeaderServerTiming                    = "Server-Timing"
	HeaderSetCookie                       = "Set-Cookie"
	HeaderSetCookie2                      = "Set-Cookie2"
	HeaderSourceMap                       = "SourceMap"
	HeaderStrictTransportSecurity         = "Strict-Transport-Security"
	HeaderTE                              = "TE"
	HeaderTimingAllowOrigin               = "Timing-Allow-Origin"
	HeaderTrailer                         = "Trailer"
	HeaderTransferEncoding                = "Transfer-Encoding"
	HeaderUpgrade                         = "Upgrade"
	HeaderUpgradeInsecureRequests         = "Upgrade-Insecure-Requests"
	HeaderUserAgent                       = "User-Agent"
	HeaderVary                            = "Vary"
	HeaderVia                             = "Via"
	HeaderWantDigest                      = "Want-Digest"
	HeaderWarning                         = "Warning"
	HeaderWWWAuthenticate                 = "WWW-Authenticate"
)

// ── CORS ──────────────────────────────────────────────────────────────────────
const (
	HeaderAccessControlAllowCredentials = "Access-Control-Allow-Credentials"
	HeaderAccessControlAllowHeaders     = "Access-Control-Allow-Headers"
	HeaderAccessControlAllowMethods     = "Access-Control-Allow-Methods"
	HeaderAccessControlAllowOrigin      = "Access-Control-Allow-Origin"
	HeaderAccessControlExposeHeaders    = "Access-Control-Expose-Headers"
	HeaderAccessControlMaxAge           = "Access-Control-Max-Age"
	HeaderAccessControlRequestHeaders   = "Access-Control-Request-Headers"
	HeaderAccessControlRequestMethod    = "Access-Control-Request-Method"
	HeaderCrossOriginEmbedderPolicy     = "Cross-Origin-Embedder-Policy"
	HeaderCrossOriginOpenerPolicy       = "Cross-Origin-Opener-Policy"
	HeaderCrossOriginResourcePolicy     = "Cross-Origin-Resource-Policy"
)

// ── Fetch metadata ────────────────────────────────────────────────────────────
const (
	HeaderSecFetchDest = "Sec-Fetch-Dest"
	HeaderSecFetchMode = "Sec-Fetch-Mode"
	HeaderSecFetchSite = "Sec-Fetch-Site"
	HeaderSecFetchUser = "Sec-Fetch-User"
)

// ── Client hints ──────────────────────────────────────────────────────────────
const (
	HeaderAcceptCH           = "Accept-CH"
	HeaderCriticalCH         = "Critical-CH"
	HeaderSecCHUA            = "Sec-CH-UA"
	HeaderSecCHUAMobile      = "Sec-CH-UA-Mobile"
	HeaderSecCHUAPlatform    = "Sec-CH-UA-Platform"
	HeaderSecCHUAArch        = "Sec-CH-UA-Arch"
	HeaderSecCHUABitness     = "Sec-CH-UA-Bitness"
	HeaderSecCHUAFullVer     = "Sec-CH-UA-Full-Version"
	HeaderSecCHUAFullVerList = "Sec-CH-UA-Full-Version-List"
	HeaderSecCHUAModel       = "Sec-CH-UA-Model"
	HeaderSaveData           = "Save-Data"
	HeaderViewportWidth      = "Viewport-Width"
	HeaderWidth              = "Width"
	HeaderDPR                = "DPR"
	HeaderDeviceMemory       = "Device-Memory"
	HeaderECT                = "ECT"
	HeaderRTT                = "RTT"
	HeaderDownlink           = "Downlink"
)

// ── Proxy / forwarding ────────────────────────────────────────────────────────
const (
	HeaderXForwardedFor    = "X-Forwarded-For"
	HeaderXForwardedHost   = "X-Forwarded-Host"
	HeaderXForwardedPort   = "X-Forwarded-Port"
	HeaderXForwardedProto  = "X-Forwarded-Proto"
	HeaderXForwardedServer = "X-Forwarded-Server"
	HeaderXRealIP          = "X-Real-IP"
	HeaderXRequestID       = "X-Request-ID"
	HeaderXCorrelationID   = "X-Correlation-ID"
	HeaderXRequestStart    = "X-Request-Start"
)

// ── Security / legacy ─────────────────────────────────────────────────────────
const (
	HeaderPermissionsPolicy             = "Permissions-Policy"
	HeaderFeaturePolicy                 = "Feature-Policy"
	HeaderXContentTypeOptions           = "X-Content-Type-Options"
	HeaderXDNSPrefetchControl           = "X-DNS-Prefetch-Control"
	HeaderXDownloadOptions              = "X-Download-Options"
	HeaderXFrameOptions                 = "X-Frame-Options"
	HeaderXPermittedCrossDomainPolicies = "X-Permitted-Cross-Domain-Policies"
	HeaderXPoweredBy                    = "X-Powered-By"
	HeaderXRequestedWith                = "X-Requested-With"
	HeaderXRobotsTag                    = "X-Robots-Tag"
	HeaderXXSSProtection                = "X-XSS-Protection"
)

// ── WebSocket ─────────────────────────────────────────────────────────────────
const (
	HeaderSecWebSocketAccept     = "Sec-WebSocket-Accept"
	HeaderSecWebSocketExtensions = "Sec-WebSocket-Extensions"
	HeaderSecWebSocketKey        = "Sec-WebSocket-Key"
	HeaderSecWebSocketProtocol   = "Sec-WebSocket-Protocol"
	HeaderSecWebSocketVersion    = "Sec-WebSocket-Version"
)

// ── HTTP/2 / HTTP/3 ──────────────────────────────────────────────────────────
const (
	HeaderHTTP2Settings = "HTTP2-Settings"
	HeaderAltUsed       = "Alt-Used"
	HeaderConnectionID  = "Connection-ID"
)

// ── MIME / content types ──────────────────────────────────────────────────────
const (
	MIMETextPlain               = "text/plain"
	MIMETextPlainCharsetUTF8    = "text/plain; charset=utf-8"
	MIMETextHTML                = "text/html"
	MIMETextHTMLCharsetUTF8     = "text/html; charset=utf-8"
	MIMETextCSS                 = "text/css"
	MIMETextCSSCharsetUTF8      = "text/css; charset=utf-8"
	MIMETextCSV                 = "text/csv"
	MIMETextCSVCharsetUTF8      = "text/csv; charset=utf-8"
	MIMETextXML                 = "text/xml"
	MIMETextXMLCharsetUTF8      = "text/xml; charset=utf-8"
	MIMETextMarkdown            = "text/markdown"
	MIMETextMarkdownCharsetUTF8 = "text/markdown; charset=utf-8"
	MIMETextEventStream         = "text/event-stream"

	MIMEApplicationJSON            = "application/json"
	MIMEApplicationJSONCharsetUTF8 = "application/json; charset=utf-8"
	MIMEApplicationXML             = "application/xml"
	MIMEApplicationXMLCharsetUTF8  = "application/xml; charset=utf-8"
	MIMEApplicationForm            = "application/x-www-form-urlencoded"
	MIMEApplicationMultipartForm   = "multipart/form-data"
	MIMEApplicationOctetStream     = "application/octet-stream"
	MIMEApplicationPDF             = "application/pdf"
	MIMEApplicationZip             = "application/zip"
	MIMEApplicationGzip            = "application/gzip"
	MIMEApplicationTar             = "application/x-tar"
	MIMEApplication7z              = "application/x-7z-compressed"
	MIMEApplicationWASM            = "application/wasm"
	MIMEApplicationJavaScript      = "application/javascript"
	MIMEApplicationECMAScript      = "application/ecmascript"
	MIMEApplicationProtobuf        = "application/protobuf"
	MIMEApplicationMsgPack         = "application/msgpack"
	MIMEApplicationNDJSON          = "application/x-ndjson"
	MIMEApplicationGraphQL         = "application/graphql"
	MIMEApplicationGraphQLJSON     = "application/graphql-response+json"
	MIMEApplicationProblemJSON     = "application/problem+json"
	MIMEApplicationProblemXML      = "application/problem+xml"
	MIMEApplicationJWT             = "application/jwt"
	MIMEApplicationCBOR            = "application/cbor"
	MIMEApplicationRSSXML          = "application/rss+xml"
	MIMEApplicationAtomXML         = "application/atom+xml"
	MIMEApplicationSOAPXML         = "application/soap+xml"
	MIMEApplicationYAML            = "application/yaml"
	MIMEApplicationXHTML           = "application/xhtml+xml"

	MIMEImagePNG  = "image/png"
	MIMEImageJPEG = "image/jpeg"
	MIMEImageGIF  = "image/gif"
	MIMEImageWEBP = "image/webp"
	MIMEImageSVG  = "image/svg+xml"
	MIMEImageICO  = "image/x-icon"
	MIMEImageAVIF = "image/avif"
	MIMEImageBMP  = "image/bmp"
	MIMEImageTIFF = "image/tiff"

	MIMEAudioMPEG = "audio/mpeg"
	MIMEAudioMP4  = "audio/mp4"
	MIMEAudioOGG  = "audio/ogg"
	MIMEAudioWAV  = "audio/wav"
	MIMEAudioWEBM = "audio/webm"
	MIMEAudioAAC  = "audio/aac"
	MIMEAudioFLAC = "audio/flac"

	MIMEVideoMP4       = "video/mp4"
	MIMEVideoMPEG      = "video/mpeg"
	MIMEVideoWEBM      = "video/webm"
	MIMEVideoOGG       = "video/ogg"
	MIMEVideoQuickTime = "video/quicktime"
	MIMEVideoAVI       = "video/x-msvideo"

	MIMEFontWOFF  = "font/woff"
	MIMEFontWOFF2 = "font/woff2"
	MIMEFontTTF   = "font/ttf"
	MIMEFontOTF   = "font/otf"
	MIMEFontEOT   = "application/vnd.ms-fontobject"
)

// ── HTTP methods ──────────────────────────────────────────────────────────────
const (
	MethodGET     = "GET"
	MethodHEAD    = "HEAD"
	MethodPOST    = "POST"
	MethodPUT     = "PUT"
	MethodPATCH   = "PATCH"
	MethodDELETE  = "DELETE"
	MethodCONNECT = "CONNECT"
	MethodOPTIONS = "OPTIONS"
	MethodTRACE   = "TRACE"

	MethodCOPY       = "COPY"
	MethodLOCK       = "LOCK"
	MethodMKCOL      = "MKCOL"
	MethodMOVE       = "MOVE"
	MethodPROPFIND   = "PROPFIND"
	MethodPROPPATCH  = "PROPPATCH"
	MethodUNLOCK     = "UNLOCK"
	MethodREPORT     = "REPORT"
	MethodMKACTIVITY = "MKACTIVITY"
	MethodCHECKOUT   = "CHECKOUT"
	MethodMERGE      = "MERGE"
	MethodSEARCH     = "SEARCH"
	MethodPURGE      = "PURGE"
)

// ── Protocol versions ─────────────────────────────────────────────────────────
const (
	HTTP09 = "HTTP/0.9"
	HTTP10 = "HTTP/1.0"
	HTTP11 = "HTTP/1.1"
	HTTP2  = "HTTP/2"
	HTTP3  = "HTTP/3"
)

// ── Protocol tokens ───────────────────────────────────────────────────────────
const (
	CRLF           = "\r\n"
	LF             = "\n"
	CR             = "\r"
	Colon          = ":"
	ColonSpace     = ": "
	Comma          = ","
	CommaSpace     = ", "
	Semicolon      = ";"
	SemicolonSpace = "; "
	Space          = " "
	Tab            = "\t"
	Slash          = "/"
	Question       = "?"
	Ampersand      = "&"
	Equals         = "="
	Dash           = "-"
	Dot            = "."
	Quote          = "\""
)

// ── Header values / connection tokens ─────────────────────────────────────────
const (
	ValueKeepAlive      = "keep-alive"
	ValueClose          = "close"
	ValueUpgrade        = "upgrade"
	ValueChunked        = "chunked"
	ValueTrailers       = "trailers"
	ValueGzip           = "gzip"
	ValueDeflate        = "deflate"
	ValueBr             = "br"
	ValueCompress       = "compress"
	ValueIdentity       = "identity"
	ValueNoCache        = "no-cache"
	ValueNoStore        = "no-store"
	ValueMaxAge0        = "max-age=0"
	ValuePrivate        = "private"
	ValuePublic         = "public"
	ValueMustRevalidate = "must-revalidate"
	ValueNoTransform    = "no-transform"
	ValueImmutable      = "immutable"
	ValueBytes          = "bytes"
	ValueNone           = "none"
	ValueSameOrigin     = "sameorigin"
	ValueDeny           = "DENY"
	ValueNosniff        = "nosniff"
	ValueWebSocket      = "websocket"
	ValueXMLHttpRequest = "XMLHttpRequest"
	ValueBoundary       = "boundary="
	ValueBearer         = "Bearer"
	ValueBasic          = "Basic"
	ValueDigest         = "Digest"
)

// ── Charset names ─────────────────────────────────────────────────────────────
const (
	CharsetUTF8     = "utf-8"
	CharsetUTF16    = "utf-16"
	CharsetISO88591 = "iso-8859-1"
	CharsetUSASCII  = "us-ascii"
)

// ── Cache-Control directives ──────────────────────────────────────────────────
const (
	CacheNoCache              = "no-cache"
	CacheNoStore              = "no-store"
	CacheNoTransform          = "no-transform"
	CacheOnlyIfCached         = "only-if-cached"
	CacheMaxAge               = "max-age"
	CacheSMaxAge              = "s-maxage"
	CacheMaxStale             = "max-stale"
	CacheMinFresh             = "min-fresh"
	CachePublic               = "public"
	CachePrivate              = "private"
	CacheMustRevalidate       = "must-revalidate"
	CacheProxyRevalidate      = "proxy-revalidate"
	CacheImmutable            = "immutable"
	CacheStaleWhileRevalidate = "stale-while-revalidate"
	CacheStaleIfError         = "stale-if-error"
)

// ── SameSite cookie values ────────────────────────────────────────────────────
const (
	CookieSameSiteStrict = "Strict"
	CookieSameSiteLax    = "Lax"
	CookieSameSiteNone   = "None"
)

// ── Cookie attribute names ────────────────────────────────────────────────────
const (
	CookieAttrExpires  = "Expires"
	CookieAttrMaxAge   = "Max-Age"
	CookieAttrDomain   = "Domain"
	CookieAttrPath     = "Path"
	CookieAttrSecure   = "Secure"
	CookieAttrHTTPOnly = "HttpOnly"
	CookieAttrSameSite = "SameSite"
)

// AppendHeader appends "Key: Value\r\n" to dst — zero alloc with sufficient capacity.
func AppendHeader(dst []byte, key, value string) []byte {
	dst = append(dst, key...)
	dst = append(dst, ColonSpace...)
	dst = append(dst, value...)
	dst = append(dst, CRLF...)
	return dst
}

// AppendHeaderBytes appends "Key: Value\r\n" to dst for byte slices.
func AppendHeaderBytes(dst []byte, key, value []byte) []byte {
	dst = append(dst, key...)
	dst = append(dst, ColonSpaceBytes...)
	dst = append(dst, value...)
	dst = append(dst, CRLFBytes...)
	return dst
}

// Bytes returns a mutable []byte copy of s.
func Bytes(s string) []byte {
	return []byte(s)
}

// ── Byte aliases ──────────────────────────────────────────────────────────────
// One exported byte-alias per public string constant. Callers should treat them
// as immutable (do not mutate the backing array).

var (
	HeaderAcceptBytes                          = []byte(HeaderAccept)
	HeaderAcceptCharsetBytes                   = []byte(HeaderAcceptCharset)
	HeaderAcceptEncodingBytes                  = []byte(HeaderAcceptEncoding)
	HeaderAcceptLanguageBytes                  = []byte(HeaderAcceptLanguage)
	HeaderAcceptPatchBytes                     = []byte(HeaderAcceptPatch)
	HeaderAcceptPostBytes                      = []byte(HeaderAcceptPost)
	HeaderAcceptRangesBytes                    = []byte(HeaderAcceptRanges)
	HeaderAgeBytes                             = []byte(HeaderAge)
	HeaderAllowBytes                           = []byte(HeaderAllow)
	HeaderAltSvcBytes                          = []byte(HeaderAltSvc)
	HeaderAuthorizationBytes                   = []byte(HeaderAuthorization)
	HeaderCacheControlBytes                    = []byte(HeaderCacheControl)
	HeaderClearSiteDataBytes                   = []byte(HeaderClearSiteData)
	HeaderConnectionBytes                      = []byte(HeaderConnection)
	HeaderContentDispositionBytes              = []byte(HeaderContentDisposition)
	HeaderContentEncodingBytes                 = []byte(HeaderContentEncoding)
	HeaderContentLanguageBytes                 = []byte(HeaderContentLanguage)
	HeaderContentLengthBytes                   = []byte(HeaderContentLength)
	HeaderContentLocationBytes                 = []byte(HeaderContentLocation)
	HeaderContentRangeBytes                    = []byte(HeaderContentRange)
	HeaderContentSecurityPolicyBytes           = []byte(HeaderContentSecurityPolicy)
	HeaderContentSecurityPolicyReportOnlyBytes = []byte(HeaderContentSecurityPolicyReportOnly)
	HeaderContentTypeBytes                     = []byte(HeaderContentType)
	HeaderCookieBytes                          = []byte(HeaderCookie)
	HeaderDateBytes                            = []byte(HeaderDate)
	HeaderDigestBytes                          = []byte(HeaderDigest)
	HeaderDNTBytes                             = []byte(HeaderDNT)
	HeaderEarlyDataBytes                       = []byte(HeaderEarlyData)
	HeaderETagBytes                            = []byte(HeaderETag)
	HeaderExpectBytes                          = []byte(HeaderExpect)
	HeaderExpiresBytes                         = []byte(HeaderExpires)
	HeaderForwardedBytes                       = []byte(HeaderForwarded)
	HeaderFromBytes                            = []byte(HeaderFrom)
	HeaderHostBytes                            = []byte(HeaderHost)
	HeaderIfMatchBytes                         = []byte(HeaderIfMatch)
	HeaderIfModifiedSinceBytes                 = []byte(HeaderIfModifiedSince)
	HeaderIfNoneMatchBytes                     = []byte(HeaderIfNoneMatch)
	HeaderIfRangeBytes                         = []byte(HeaderIfRange)
	HeaderIfUnmodifiedSinceBytes               = []byte(HeaderIfUnmodifiedSince)
	HeaderKeepAliveBytes                       = []byte(HeaderKeepAlive)
	HeaderLastModifiedBytes                    = []byte(HeaderLastModified)
	HeaderLinkBytes                            = []byte(HeaderLink)
	HeaderLocationBytes                        = []byte(HeaderLocation)
	HeaderNELBytes                             = []byte(HeaderNEL)
	HeaderOriginBytes                          = []byte(HeaderOrigin)
	HeaderPragmaBytes                          = []byte(HeaderPragma)
	HeaderPriorityBytes                        = []byte(HeaderPriority)
	HeaderProxyAuthenticateBytes               = []byte(HeaderProxyAuthenticate)
	HeaderProxyAuthorizationBytes              = []byte(HeaderProxyAuthorization)
	HeaderRangeBytes                           = []byte(HeaderRange)
	HeaderRefererBytes                         = []byte(HeaderReferer)
	HeaderReferrerPolicyBytes                  = []byte(HeaderReferrerPolicy)
	HeaderRefreshBytes                         = []byte(HeaderRefresh)
	HeaderRetryAfterBytes                      = []byte(HeaderRetryAfter)
	HeaderServerBytes                          = []byte(HeaderServer)
	HeaderServerTimingBytes                    = []byte(HeaderServerTiming)
	HeaderSetCookieBytes                       = []byte(HeaderSetCookie)
	HeaderSetCookie2Bytes                      = []byte(HeaderSetCookie2)
	HeaderSourceMapBytes                       = []byte(HeaderSourceMap)
	HeaderStrictTransportSecurityBytes         = []byte(HeaderStrictTransportSecurity)
	HeaderTEBytes                              = []byte(HeaderTE)
	HeaderTimingAllowOriginBytes               = []byte(HeaderTimingAllowOrigin)
	HeaderTrailerBytes                         = []byte(HeaderTrailer)
	HeaderTransferEncodingBytes                = []byte(HeaderTransferEncoding)
	HeaderUpgradeBytes                         = []byte(HeaderUpgrade)
	HeaderUpgradeInsecureRequestsBytes         = []byte(HeaderUpgradeInsecureRequests)
	HeaderUserAgentBytes                       = []byte(HeaderUserAgent)
	HeaderVaryBytes                            = []byte(HeaderVary)
	HeaderViaBytes                             = []byte(HeaderVia)
	HeaderWantDigestBytes                      = []byte(HeaderWantDigest)
	HeaderWarningBytes                         = []byte(HeaderWarning)
	HeaderWWWAuthenticateBytes                 = []byte(HeaderWWWAuthenticate)

	HeaderAccessControlAllowCredentialsBytes = []byte(HeaderAccessControlAllowCredentials)
	HeaderAccessControlAllowHeadersBytes     = []byte(HeaderAccessControlAllowHeaders)
	HeaderAccessControlAllowMethodsBytes     = []byte(HeaderAccessControlAllowMethods)
	HeaderAccessControlAllowOriginBytes      = []byte(HeaderAccessControlAllowOrigin)
	HeaderAccessControlExposeHeadersBytes    = []byte(HeaderAccessControlExposeHeaders)
	HeaderAccessControlMaxAgeBytes           = []byte(HeaderAccessControlMaxAge)
	HeaderAccessControlRequestHeadersBytes   = []byte(HeaderAccessControlRequestHeaders)
	HeaderAccessControlRequestMethodBytes    = []byte(HeaderAccessControlRequestMethod)
	HeaderCrossOriginEmbedderPolicyBytes     = []byte(HeaderCrossOriginEmbedderPolicy)
	HeaderCrossOriginOpenerPolicyBytes       = []byte(HeaderCrossOriginOpenerPolicy)
	HeaderCrossOriginResourcePolicyBytes     = []byte(HeaderCrossOriginResourcePolicy)

	HeaderSecFetchDestBytes = []byte(HeaderSecFetchDest)
	HeaderSecFetchModeBytes = []byte(HeaderSecFetchMode)
	HeaderSecFetchSiteBytes = []byte(HeaderSecFetchSite)
	HeaderSecFetchUserBytes = []byte(HeaderSecFetchUser)

	HeaderXForwardedForBytes    = []byte(HeaderXForwardedFor)
	HeaderXForwardedHostBytes   = []byte(HeaderXForwardedHost)
	HeaderXForwardedPortBytes   = []byte(HeaderXForwardedPort)
	HeaderXForwardedProtoBytes  = []byte(HeaderXForwardedProto)
	HeaderXForwardedServerBytes = []byte(HeaderXForwardedServer)
	HeaderXRealIPBytes          = []byte(HeaderXRealIP)
	HeaderXRequestIDBytes       = []byte(HeaderXRequestID)
	HeaderXCorrelationIDBytes   = []byte(HeaderXCorrelationID)
	HeaderXRequestStartBytes    = []byte(HeaderXRequestStart)

	HeaderPermissionsPolicyBytes             = []byte(HeaderPermissionsPolicy)
	HeaderFeaturePolicyBytes                 = []byte(HeaderFeaturePolicy)
	HeaderXContentTypeOptionsBytes           = []byte(HeaderXContentTypeOptions)
	HeaderXDNSPrefetchControlBytes           = []byte(HeaderXDNSPrefetchControl)
	HeaderXDownloadOptionsBytes              = []byte(HeaderXDownloadOptions)
	HeaderXFrameOptionsBytes                 = []byte(HeaderXFrameOptions)
	HeaderXPermittedCrossDomainPoliciesBytes = []byte(HeaderXPermittedCrossDomainPolicies)
	HeaderXPoweredByBytes                    = []byte(HeaderXPoweredBy)
	HeaderXRequestedWithBytes                = []byte(HeaderXRequestedWith)
	HeaderXRobotsTagBytes                    = []byte(HeaderXRobotsTag)
	HeaderXXSSProtectionBytes                = []byte(HeaderXXSSProtection)

	HeaderSecWebSocketAcceptBytes     = []byte(HeaderSecWebSocketAccept)
	HeaderSecWebSocketExtensionsBytes = []byte(HeaderSecWebSocketExtensions)
	HeaderSecWebSocketKeyBytes        = []byte(HeaderSecWebSocketKey)
	HeaderSecWebSocketProtocolBytes   = []byte(HeaderSecWebSocketProtocol)
	HeaderSecWebSocketVersionBytes    = []byte(HeaderSecWebSocketVersion)

	HeaderHTTP2SettingsBytes = []byte(HeaderHTTP2Settings)
	HeaderAltUsedBytes       = []byte(HeaderAltUsed)
	HeaderConnectionIDBytes  = []byte(HeaderConnectionID)
)

var (
	MimeTextPlainBytes               = []byte(MIMETextPlain)
	MimeTextPlainCharsetUTF8Bytes    = []byte(MIMETextPlainCharsetUTF8)
	MimeTextHTMLBytes                = []byte(MIMETextHTML)
	MimeTextHTMLCharsetUTF8Bytes     = []byte(MIMETextHTMLCharsetUTF8)
	MimeTextCSSBytes                 = []byte(MIMETextCSS)
	MimeTextCSSCharsetUTF8Bytes      = []byte(MIMETextCSSCharsetUTF8)
	MimeTextCSVBytes                 = []byte(MIMETextCSV)
	MimeTextCSVCharsetUTF8Bytes      = []byte(MIMETextCSVCharsetUTF8)
	MimeTextXMLBytes                 = []byte(MIMETextXML)
	MimeTextXMLCharsetUTF8Bytes      = []byte(MIMETextXMLCharsetUTF8)
	MimeTextMarkdownBytes            = []byte(MIMETextMarkdown)
	MimeTextMarkdownCharsetUTF8Bytes = []byte(MIMETextMarkdownCharsetUTF8)
	MimeTextEventStreamBytes         = []byte(MIMETextEventStream)

	MimeApplicationJSONBytes            = []byte(MIMEApplicationJSON)
	MimeApplicationJSONCharsetUTF8Bytes = []byte(MIMEApplicationJSONCharsetUTF8)
	MimeApplicationXMLBytes             = []byte(MIMEApplicationXML)
	MimeApplicationXMLCharsetUTF8Bytes  = []byte(MIMEApplicationXMLCharsetUTF8)
	MimeApplicationFormBytes            = []byte(MIMEApplicationForm)
	MimeApplicationMultipartFormBytes   = []byte(MIMEApplicationMultipartForm)
	MimeApplicationOctetStreamBytes     = []byte(MIMEApplicationOctetStream)
	MimeApplicationPDFBytes             = []byte(MIMEApplicationPDF)
	MimeApplicationZipBytes             = []byte(MIMEApplicationZip)
	MimeApplicationGzipBytes            = []byte(MIMEApplicationGzip)
	MimeApplicationTarBytes             = []byte(MIMEApplicationTar)
	MimeApplication7zBytes              = []byte(MIMEApplication7z)
	MimeApplicationWASMBytes            = []byte(MIMEApplicationWASM)
	MimeApplicationJavaScriptBytes      = []byte(MIMEApplicationJavaScript)
	MimeApplicationProtobufBytes        = []byte(MIMEApplicationProtobuf)
	MimeApplicationMsgPackBytes         = []byte(MIMEApplicationMsgPack)
	MimeApplicationNDJSONBytes          = []byte(MIMEApplicationNDJSON)
	MimeApplicationProblemJSONBytes     = []byte(MIMEApplicationProblemJSON)

	MimeImagePNGBytes  = []byte(MIMEImagePNG)
	MimeImageJPEGBytes = []byte(MIMEImageJPEG)
	MimeImageGIFBytes  = []byte(MIMEImageGIF)
	MimeImageWEBPBytes = []byte(MIMEImageWEBP)
	MimeImageSVGBytes  = []byte(MIMEImageSVG)
	MimeImageICOBytes  = []byte(MIMEImageICO)
	MimeImageAVIFBytes = []byte(MIMEImageAVIF)

	MimeAudioMPEGBytes = []byte(MIMEAudioMPEG)
	MimeAudioMP4Bytes  = []byte(MIMEAudioMP4)

	MimeVideoMP4Bytes  = []byte(MIMEVideoMP4)
	MimeVideoMPEGBytes = []byte(MIMEVideoMPEG)

	MimeFontWOFFBytes  = []byte(MIMEFontWOFF)
	MimeFontWOFF2Bytes = []byte(MIMEFontWOFF2)
)

var (
	MethodGETBytes     = []byte(MethodGET)
	MethodHEADBytes    = []byte(MethodHEAD)
	MethodPOSTBytes    = []byte(MethodPOST)
	MethodPUTBytes     = []byte(MethodPUT)
	MethodPATCHBytes   = []byte(MethodPATCH)
	MethodDELETEBytes  = []byte(MethodDELETE)
	MethodCONNECTBytes = []byte(MethodCONNECT)
	MethodOPTIONSBytes = []byte(MethodOPTIONS)
	MethodTRACEBytes   = []byte(MethodTRACE)

	MethodCOPYBytes      = []byte(MethodCOPY)
	MethodLOCKBytes      = []byte(MethodLOCK)
	MethodMKCOLBytes     = []byte(MethodMKCOL)
	MethodMOVEBytes      = []byte(MethodMOVE)
	MethodPROPFINDBytes  = []byte(MethodPROPFIND)
	MethodPROPPATCHBytes = []byte(MethodPROPPATCH)
	MethodUNLOCKBytes    = []byte(MethodUNLOCK)
	MethodPURGEBytes     = []byte(MethodPURGE)
)

var (
	MethodHTTP09Bytes = []byte(HTTP09)
	MethodHTTP10Bytes = []byte(HTTP10)
	MethodHTTP11Bytes = []byte(HTTP11)
	MethodHTTP2Bytes  = []byte(HTTP2)
	MethodHTTP3Bytes  = []byte(HTTP3)

	CRLFBytes           = []byte(CRLF)
	LFBytes             = []byte(LF)
	CRBytes             = []byte(CR)
	ColonBytes          = []byte(Colon)
	ColonSpaceBytes     = []byte(ColonSpace)
	CommaBytes          = []byte(Comma)
	CommaSpaceBytes     = []byte(CommaSpace)
	SemicolonBytes      = []byte(Semicolon)
	SemicolonSpaceBytes = []byte(SemicolonSpace)
	SpaceBytes          = []byte(Space)
	TabBytes            = []byte(Tab)
	SlashBytes          = []byte(Slash)
	QuestionBytes       = []byte(Question)
	AmpersandBytes      = []byte(Ampersand)
	EqualsBytes         = []byte(Equals)
	DashBytes           = []byte(Dash)
	DotBytes            = []byte(Dot)
	QuoteBytes          = []byte(Quote)
)

var (
	ValueKeepAliveBytes      = []byte(ValueKeepAlive)
	ValueCloseBytes          = []byte(ValueClose)
	ValueUpgradeBytes        = []byte(ValueUpgrade)
	ValueChunkedBytes        = []byte(ValueChunked)
	ValueTrailersBytes       = []byte(ValueTrailers)
	ValueGzipBytes           = []byte(ValueGzip)
	ValueDeflateBytes        = []byte(ValueDeflate)
	ValueBrBytes             = []byte(ValueBr)
	ValueCompressBytes       = []byte(ValueCompress)
	ValueIdentityBytes       = []byte(ValueIdentity)
	ValueNoCacheBytes        = []byte(ValueNoCache)
	ValueNoStoreBytes        = []byte(ValueNoStore)
	ValueMaxAge0Bytes        = []byte(ValueMaxAge0)
	ValuePrivateBytes        = []byte(ValuePrivate)
	ValuePublicBytes         = []byte(ValuePublic)
	ValueMustRevalidateBytes = []byte(ValueMustRevalidate)
	ValueNoTransformBytes    = []byte(ValueNoTransform)
	ValueImmutableBytes      = []byte(ValueImmutable)
	ValueBytesBytes          = []byte(ValueBytes)
	ValueNoneBytes           = []byte(ValueNone)
	ValueSameOriginBytes     = []byte(ValueSameOrigin)
	ValueDenyBytes           = []byte(ValueDeny)
	ValueNosniffBytes        = []byte(ValueNosniff)
	ValueWebSocketBytes      = []byte(ValueWebSocket)
	ValueXMLHttpRequestBytes = []byte(ValueXMLHttpRequest)
	ValueBoundaryBytes       = []byte(ValueBoundary)
	ValueBearerBytes         = []byte(ValueBearer)
	ValueBasicBytes          = []byte(ValueBasic)
	ValueDigestBytes         = []byte(ValueDigest)
)
