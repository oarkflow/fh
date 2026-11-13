package fh

import (
	"strconv"
	"strings"
	"testing"
)

func rangeList(count int) string {
	var builder strings.Builder
	builder.WriteString("bytes=")
	for i := 0; i < count; i++ {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(strconv.Itoa(i))
		builder.WriteString("-")
		builder.WriteString(strconv.Itoa(i))
	}
	return builder.String()
}

func TestRangeParserStopsAtConfiguredLimit(t *testing.T) {
	if ranges, err := parseRangeHeader(rangeList(defaultMaxStaticRanges), 1000); err != nil || len(ranges) != defaultMaxStaticRanges {
		t.Fatalf("default range parse = %d ranges, %v", len(ranges), err)
	}
	if ranges, err := parseRangeHeader(rangeList(defaultMaxStaticRanges+1), 1000); err == nil || ranges != nil {
		t.Fatalf("default range limit returned %d ranges, %v; want rejection", len(ranges), err)
	}
	customLimit := defaultMaxStaticRanges + 1
	if ranges, err := parseRangeHeaderLimit(rangeList(customLimit), 1000, customLimit); err != nil || len(ranges) != customLimit {
		t.Fatalf("custom range parse = %d ranges, %v", len(ranges), err)
	}
}

func TestRangeParserBoundsLargeRangeLists(t *testing.T) {
	header := rangeList(10000)
	if ranges, err := parseRangeHeader(header, 20000); err == nil || ranges != nil {
		t.Fatalf("large range list returned %d ranges, %v; want rejection", len(ranges), err)
	}
}
