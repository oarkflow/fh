package slowloris

import "testing"

func TestTrackConnReleaseIsIdempotent(t *testing.T) {
	before := ActiveConnections()
	release := TrackConn()
	release()
	release()
	if got := ActiveConnections(); got != before {
		t.Fatalf("active connections = %d, want %d", got, before)
	}
}
