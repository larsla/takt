// Package mocks mirrors go.uber.org/cadence/mocks: testify mocks of
// client.Client, client.DomainClient, client.WorkflowRun,
// client.HistoryEventIterator and encoded.Value. internal/tools/genmocks
// writes them (make generate). Like Cadence's, they work as zero values
// (&mocks.Client{}); NewClient(t) also asserts expectations at cleanup.
package mocks
