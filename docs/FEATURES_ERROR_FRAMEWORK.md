# Error Framework Feature List

1. RFC 9457 problem details
2. Typed `HTTPError`
3. Safe public message plus private wrapped cause
4. Error kind classification
5. Error severity classification
6. Retryable flag
7. `Retry-After` handling
8. Field validation errors
9. Panic-to-error conversion
10. Stack capture for panics
11. Environment-aware debug policy
12. Production-safe internal error masking
13. Development/test diagnostics
14. Built-in secret redaction
15. Request ID correlation
16. Problem `instance` URI
17. UTC timestamp extension
18. Stable error catalog for docs/OpenAPI
19. Error metrics counters by code
20. Safe fallback if error response rendering fails
21. Default not-found/method-not-allowed routed through framework
22. Middleware panic recovery package
23. Middleware packages restored instead of deleting existing test expectations
24. Example app and documentation included
25. Tests added for production masking, debug redaction, and validation problem details
