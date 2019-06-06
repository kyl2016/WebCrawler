package main

import (
	"flag"
	"fmt"
	lib "github.com/kyl2016/WebCrawler/examples/finder/internal"
	"github.com/kyl2016/WebCrawler/examples/finder/monitor"
	"github.com/kyl2016/WebCrawler/helper/log"
	sched "github.com/kyl2016/WebCrawler/scheduler"
	"net/http"
	"os"
	"strings"
	"time"
)

var logger = log.DLogger()

var (
	//firstURL string = "https://zhihu.sogou.com/zhihu?query=golang&ie=utf8&w=&oq=&ri=0&sourceid=sugg&sut=226&sst0=1559648905150&lkt=1%2C1559648905044%2C1559648905044"
	firstURL string = "http://soso.nipic.com/?q=cat"
	domains  string = "img03.sogoucdn.com,zhuanlan.zhihu.com,http://soso.nipic.com,http://www.nipic.com,static.nipic.com"
	depth    uint   = 6
	dirPath  string = "/tmp/images"
)

func Usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\tfinder [flags] \n")
	fmt.Fprintf(os.Stderr, "Flags:\n")
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage
	flag.Parse()
	scheduler := sched.NewScheduler()
	domainParts := strings.Split(domains, ",")
	acceptedDomains := []string{}
	for _, domain := range domainParts {
		domain = strings.TrimSpace(domain)
		if domain != "" {
			acceptedDomains = append(acceptedDomains, domain)
		}
	}
	requestArgs := sched.RequestArgs{
		AcceptedDomains: acceptedDomains,
		MaxDepth:        uint32(depth),
	}
	dataArgs := sched.DataArgs{
		ReqBufferCap:         50,
		ReqMaxBufferNumber:   1000,
		RespBufferCap:        50,
		RespMaxBufferNumber:  10,
		ItemBufferCap:        50,
		ItemMaxBufferNumber:  100,
		ErrorBufferCap:       50,
		ErrorMaxBufferNumber: 1,
	}
	downloaders, err := lib.GetDownloaders(1)
	if err != nil {
		logger.Fatal("An error occurs when creating downloaders: %s", err)
	}
	analyzers, err := lib.GetAnalyzers(1)
	if err != nil {
		logger.Fatal("An error occurs when creating analyzers: %s", err)
	}
	pipelines, err := lib.GetPipelines(1, dirPath)
	if err != nil {
		logger.Fatalf("An error occurs when creating pipelines: %s", err)
	}
	moduleArgs := sched.ModuleArgs{
		Downloaders: downloaders,
		Analyzers:   analyzers,
		Pipelines:   pipelines,
	}
	err = scheduler.Init(requestArgs, dataArgs, moduleArgs)
	if err != nil {
		logger.Fatalf("An error occurs when initializing scheduler: %s", err)
	}
	checkInterval := time.Second
	summarizeInterval := 100 * time.Millisecond
	maxIdleCount := uint(5)
	checkCountChan := monitor.Monitor(scheduler, checkInterval, summarizeInterval, maxIdleCount, true, lib.Record)
	firstHTTPReq, err := http.NewRequest("GET", firstURL, nil)
	if err != nil {
		logger.Fatalln(err)
		return
	}
	err = scheduler.Start(firstHTTPReq)
	if err != nil {
		logger.Fatalf("An error occurs when starting scheduler: %s", err)
	}
	<-checkCountChan
}
