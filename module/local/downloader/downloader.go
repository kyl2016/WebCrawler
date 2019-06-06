package downloader

import (
	"github.com/Qihoo360/poseidon/builder/docformat/src/github.com/donnie4w/go-logger/logger"
	"github.com/kyl2016/WebCrawler/module"
	"github.com/kyl2016/WebCrawler/module/stub"
	"gosrc/src/math/rand"
	rand3 "math/rand"
	"net/http"
	"time"
)

type myDownloader struct {
	stub.ModuleInternal
	httpClient http.Client
	rand       *rand3.Rand
}

func New(mid module.MID, client *http.Client, scoreCalculator module.CalculateScore) (module.Downloader, error) {
	rand3.Seed(time.Now().UnixNano())
	moduleBase, err := stub.NewModuleInternal(mid, scoreCalculator)
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, genParameterError("nil http client")
	}
	return &myDownloader{
		ModuleInternal: moduleBase,
		httpClient:     *client,
		rand:           rand3.New(rand3.NewSource(1000)),
	}, nil
}

func (downloader *myDownloader) Download(req *module.Request) (*module.Response, error) {
	downloader.ModuleInternal.IncrHandlingNumber()
	defer downloader.ModuleInternal.DecrHandlingNumber()
	downloader.ModuleInternal.IncrCalledCount()
	if req == nil {
		return nil, genParameterError("nil request")
	}
	httpReq := req.HTTPReq()
	if httpReq == nil {
		return nil, genParameterError("nil HTTP request")
	}
	downloader.ModuleInternal.IncrAcceptedCount()
	logger.Infof("Do the request (URL: %s, depth: %d)... \n", httpReq.URL, req.Depth())

	time.Sleep(time.Duration(rand.Intn(10)) * time.Second)

	httpResp, err := downloader.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	downloader.ModuleInternal.IncrCompletedCount()
	return module.NewResponse(httpResp, req.Depth()), nil
}
