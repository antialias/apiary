package main

import "net/http"

// fetchPreferred is a one-off read of the brain's /preferred, for tests only:
// production hears the same document as the data of a `preferred` event on
// the brain's /events (brain_stream.go). Both go through applyPreferred, and a
// failed read flips the view as a dropped stream does.
func fetchPreferred() {
	req, _ := http.NewRequest("GET", lan.brainURL()+"/preferred", nil)
	resp, err := brainClient.Do(req)
	if err != nil {
		lan.brainFail("fetch", err)
		return
	}
	defer resp.Body.Close()
	lan.applyPreferred(resp.Body)
}

// stopPrewarmTimers empties the warm set and stops every prewarm timer, so
// none fires during a later test (prewarmWG's -race lesson, main.go).
func stopPrewarmTimers() {
	lan.setWarmSet(map[string]bool{})
}
