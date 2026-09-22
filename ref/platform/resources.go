package platform

// Resource providers.
//
// Every built-in provider is stdlib-backed: database/sql (so PostgreSQL, MySQL
// and SQLite all work through whichever driver the application imports), fh's
// own kv stores and durable queue, net/http, net/smtp and crypto. This package
// therefore adds no client-library dependency to fh, and a deployment pulls in
// exactly the drivers it actually uses.
//
// Backends that need a real client — Redis, Kafka, NATS, S3, SES — are not
// stubbed out here under a name that cannot honour them. They plug in through
// RegisterResourceDriver against the contracts in ref/platform/spi, which is a
// few lines of host code and keeps the honest property that every registered
// kind does what its name says.

func registerBuiltinResources(r *Registry) {
	registerCacheResources(r)
	registerSessionResources(r)
	registerDatabaseResources(r)
	registerQueueResources(r)
	registerCoordinationResources(r)
	registerAuthResources(r)
	registerServiceResources(r)
	registerStorageResources(r)
	registerMiscResources(r)
	registerProcessStoreResources(r)
}
