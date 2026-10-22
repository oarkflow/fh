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
