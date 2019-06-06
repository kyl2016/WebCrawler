package module

import "net/http"

type Counts struct {
	CalledCount    uint64
	AcceptedCount  uint64
	CompletedCount uint64
	HandlingNumber uint64
}

type SummaryStruct struct {
	ID        MID         `json:"id"`
	Called    uint64      `json:"called"`
	Accepted  uint64      `json:"accepted"`
	Completed uint64      `json:"completed"`
	Handling  uint64      `json:"handling"`
	Extra     interface{} `json:"extra,omitempty"`
}

type Module interface {
	ID() MID
	Addr() string
	Score() uint64
	SetScore(score uint64)
	ScoreCalculator() CalculateScore
	CalledCount() uint64
	AcceptCount() uint64
	CompletedCount() uint64
	HandlingNumber() uint64
	Counts() Counts
	Summary() SummaryStruct
}

type Downloader interface {
	Module
	Download(req *Request) (*Response, error)
}

type Analyzer interface {
	Module
	RespParses() []ParseResponse
	Analyze(resp *Response) ([]Data, []error)
}

type ParseResponse func(httpResp *http.Response, respDepth uint32) ([]Data, []error)

type Pipeline interface {
	Module
	ItemProcessors() []ProcessItem
	Send(item Item) []error
	FailFast() bool
	SetFailFast(failFast bool)
}

type ProcessItem func(item Item) (result Item, err error)
