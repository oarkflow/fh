package main

import "testing"

func TestSelectScenarios(t *testing.T) {
	selected, err := selectScenarios("json, params")
	if err != nil || len(selected) != 2 || selected[0].name != "json" || selected[1].name != "params" {
		t.Fatalf("selected = %v, %v", selected, err)
	}
	for _, names := range []string{"json,json", "missing", "json,"} {
		if _, err := selectScenarios(names); err == nil {
			t.Errorf("accepted invalid scenarios %q", names)
		}
	}
}

func TestValidatePorts(t *testing.T) {
	for _, ports := range [][]int{{3001, 3003, 3004}, {1, 65535}} {
		if err := validatePorts(ports...); err != nil {
			t.Errorf("validatePorts(%v): %v", ports, err)
		}
	}
	for _, ports := range [][]int{{0}, {65536}, {3001, 3001}} {
		if err := validatePorts(ports...); err == nil {
			t.Errorf("validatePorts(%v) unexpectedly succeeded", ports)
		}
	}
}

func TestApplyRuntimeStats(t *testing.T) {
	out := result{Requests: 4}
	applyRuntimeStats(&out,
		runtimeSnapshot{TotalAlloc: 100, HeapAlloc: 500, NumGC: 3, GoCPU: 1},
		runtimeSnapshot{TotalAlloc: 500, HeapAlloc: 700, NumGC: 5, GoCPU: 1.2},
	)
	if out.AllocatedBytes != 400 || out.AllocBytesPerReq != 100 || out.HeapAllocBefore != 500 || out.HeapAllocAfter != 700 || out.HeapAllocDelta != 200 || out.GCDelta != 2 || out.GoCPUSeconds < 0.199 || out.GoCPUSeconds > 0.201 || out.GoCPUMSPerReq < 49.9 || out.GoCPUMSPerReq > 50.1 {
		t.Fatalf("runtime metrics = %+v", out)
	}

	decreased := result{Requests: 1}
	applyRuntimeStats(&decreased,
		runtimeSnapshot{TotalAlloc: 500, HeapAlloc: 700},
		runtimeSnapshot{TotalAlloc: 400, HeapAlloc: 600},
	)
	if decreased.ResourceStatsError == "" || decreased.AllocatedBytes != 0 {
		t.Fatalf("decreased allocation counter = %+v", decreased)
	}
}

func TestParseRSSKilobytes(t *testing.T) {
	if got, err := parseRSSKilobytes(" 1234\n"); err != nil || got != 1234 {
		t.Fatalf("parse RSS = %d, %v", got, err)
	}
	for _, raw := range []string{"", "abc", "18446744073709551615"} {
		if _, err := parseRSSKilobytes(raw); err == nil {
			t.Errorf("parseRSSKilobytes(%q) unexpectedly succeeded", raw)
		}
	}
}

func TestValidatePayloadRejectsWrongJSON(t *testing.T) {
	for _, tc := range []struct {
		scenario scenario
		body     string
		wantErr  bool
	}{
		{scenario{name: "json"}, `{"message":"Hello, World!"}`, false},
		{scenario{name: "json"}, `{}`, true},
		{scenario{name: "params"}, `{"id":0,"name":"User 42"}`, false},
		{scenario{name: "params"}, `{"name":"User 42"}`, true},
		{scenario{name: "echo", body: `{"message":"Hello, World!"}`}, `{"message":"Hello, World!"}`, false},
	} {
		err := validatePayload(tc.scenario, []byte(tc.body))
		if (err != nil) != tc.wantErr {
			t.Errorf("%s body %q: error=%v, want error=%v", tc.scenario.name, tc.body, err, tc.wantErr)
		}
	}
}
