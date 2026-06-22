package hpack

// headerFieldTable implements a list of HeaderFields used for both the static
// and dynamic HPACK tables (RFC 7541 §2.3).
//
// For the dynamic table, entries are appended to ents and evicted from ents[0].
// Each entry has a monotonically increasing unique id (evictCount + index + 1)
// that survives evictions and is used for O(1) index lookups.
//
// The static table is a global read-only instance; ents[k] has HPACK index k+1.
type headerFieldTable struct {
	ents       []HeaderField
	evictCount uint64

	// byName and byNameValue map to the unique id of the most-recently-added
	// entry with that name or name+value pair, enabling O(1) search.
	byName      map[string]uint64
	byNameValue map[pairNameValue]uint64
}

type pairNameValue struct{ name, value string }

func (t *headerFieldTable) init() {
	t.byName = make(map[string]uint64, 64)
	t.byNameValue = make(map[pairNameValue]uint64, 64)
}

// reset clears all dynamic entries while retaining allocated map capacity.
func (t *headerFieldTable) reset() {
	for k := range t.byName {
		delete(t.byName, k)
	}
	for k := range t.byNameValue {
		delete(t.byNameValue, k)
	}
	t.ents = t.ents[:0]
	t.evictCount = 0
}

func (t *headerFieldTable) len() int { return len(t.ents) }

func (t *headerFieldTable) addEntry(f HeaderField) {
	id := t.evictCount + uint64(t.len()) + 1
	t.byName[f.Name] = id
	t.byNameValue[pairNameValue{f.Name, f.Value}] = id
	t.ents = append(t.ents, f)
}

// evictOldest removes the n oldest entries.
func (t *headerFieldTable) evictOldest(n int) {
	for k := 0; k < n; k++ {
		f := t.ents[k]
		id := t.evictCount + uint64(k) + 1
		if t.byName[f.Name] == id {
			delete(t.byName, f.Name)
		}
		if p := (pairNameValue{f.Name, f.Value}); t.byNameValue[p] == id {
			delete(t.byNameValue, p)
		}
	}
	// Shift remaining entries to the front and zero trailing slots for GC.
	copy(t.ents, t.ents[n:])
	for k := t.len() - n; k < t.len(); k++ {
		t.ents[k] = HeaderField{}
	}
	t.ents = t.ents[:t.len()-n]
	t.evictCount += uint64(n)
}

// search looks up f in the table.
// Returns (0, false) on no match, (i, false) on name-only match, (i, true) on
// full match. The returned index is HPACK-style (1-based).
func (t *headerFieldTable) search(f HeaderField) (i uint64, nameValueMatch bool) {
	if !f.Sensitive {
		if id := t.byNameValue[pairNameValue{f.Name, f.Value}]; id != 0 {
			return t.idToIndex(id), true
		}
	}
	if id := t.byName[f.Name]; id != 0 {
		return t.idToIndex(id), false
	}
	return 0, false
}

func (t *headerFieldTable) idToIndex(id uint64) uint64 {
	k := id - t.evictCount - 1
	if t == staticTable {
		return k + 1 // static: oldest entry is index 1
	}
	return uint64(t.len()) - k // dynamic: newest entry is lowest index
}

// ── Dynamic table wrapper ────────────────────────────────────────────────────

type dynamicTable struct {
	table          headerFieldTable
	size           uint32
	maxSize        uint32
	allowedMaxSize uint32
}

func (dt *dynamicTable) setMaxSize(v uint32) {
	dt.maxSize = v
	dt.evict()
}

func (dt *dynamicTable) add(f HeaderField) {
	dt.table.addEntry(f)
	dt.size += f.Size()
	dt.evict()
}

func (dt *dynamicTable) evict() {
	n := 0
	for dt.size > dt.maxSize && n < dt.table.len() {
		dt.size -= dt.table.ents[n].Size()
		n++
	}
	if n > 0 {
		dt.table.evictOldest(n)
	}
}

// ── Static table (RFC 7541 Appendix A) ──────────────────────────────────────

var staticTable = newStaticTable()

func newStaticTable() *headerFieldTable {
	t := &headerFieldTable{}
	t.init()
	for _, hf := range staticTableEntries {
		t.addEntry(hf)
	}
	return t
}

// staticTableEntries is the RFC 7541 Appendix A static table.
// Index 1 = ":authority", … Index 61 = "www-authenticate: ".
var staticTableEntries = [...]HeaderField{
	{Name: ":authority"},
	{Name: ":method", Value: "GET"},
	{Name: ":method", Value: "POST"},
	{Name: ":path", Value: "/"},
	{Name: ":path", Value: "/index.html"},
	{Name: ":scheme", Value: "http"},
	{Name: ":scheme", Value: "https"},
	{Name: ":status", Value: "200"},
	{Name: ":status", Value: "204"},
	{Name: ":status", Value: "206"},
	{Name: ":status", Value: "304"},
	{Name: ":status", Value: "400"},
	{Name: ":status", Value: "404"},
	{Name: ":status", Value: "500"},
	{Name: "accept-charset"},
	{Name: "accept-encoding", Value: "gzip, deflate"},
	{Name: "accept-language"},
	{Name: "accept-ranges"},
	{Name: "accept"},
	{Name: "access-control-allow-origin"},
	{Name: "age"},
	{Name: "allow"},
	{Name: "authorization"},
	{Name: "cache-control"},
	{Name: "content-disposition"},
	{Name: "content-encoding"},
	{Name: "content-language"},
	{Name: "content-length"},
	{Name: "content-location"},
	{Name: "content-range"},
	{Name: "content-type"},
	{Name: "cookie"},
	{Name: "date"},
	{Name: "etag"},
	{Name: "expect"},
	{Name: "expires"},
	{Name: "from"},
	{Name: "host"},
	{Name: "if-match"},
	{Name: "if-modified-since"},
	{Name: "if-none-match"},
	{Name: "if-range"},
	{Name: "if-unmodified-since"},
	{Name: "last-modified"},
	{Name: "link"},
	{Name: "location"},
	{Name: "max-forwards"},
	{Name: "proxy-authenticate"},
	{Name: "proxy-authorization"},
	{Name: "range"},
	{Name: "referer"},
	{Name: "refresh"},
	{Name: "retry-after"},
	{Name: "server"},
	{Name: "set-cookie"},
	{Name: "strict-transport-security"},
	{Name: "transfer-encoding"},
	{Name: "user-agent"},
	{Name: "vary"},
	{Name: "via"},
	{Name: "www-authenticate"},
}
