package internal

import (
	"github.com/kyl2016/WebCrawler/module"
	"github.com/kyl2016/WebCrawler/module/local/analyzer"
	"github.com/kyl2016/WebCrawler/module/local/downloader"
	"github.com/kyl2016/WebCrawler/module/local/pipeline"
)

var snGen = module.NewSNGenerator(1, 0)

func GetDownloaders(number uint8) ([]module.Downloader, error) {
	downloaders := []module.Downloader{}
	if number == 0 {
		return downloaders, nil
	}
	for i := uint8(0); i < number; i++ {
		mid, err := module.GenMID(module.TYPE_DOWNLOADER, snGen.Get(), nil)
		if err != nil {
			return downloaders, err
		}
		d, err := downloader.New(mid, genHTTPClient(), module.CalculateScoreSimple)
		if err != nil {
			return downloaders, err
		}
		downloaders = append(downloaders, d)
	}
	return downloaders, nil
}

func GetAnalyzers(number uint8) ([]module.Analyzer, error) {
	analyzers := []module.Analyzer{}
	if number == 0 {
		return analyzers, nil
	}
	for i := uint8(0); i < number; i++ {
		mid, err := module.GenMID(module.TYPE_ANALYZER, snGen.Get(), nil)
		if err != nil {
			return analyzers, err
		}
		a, err := analyzer.New(mid, genResponseParsers(), module.CalculateScoreSimple)
		if err != nil {
			return analyzers, err
		}
		analyzers = append(analyzers, a)
	}
	return analyzers, nil
}

func GetPipelines(number uint8, dirPath string) ([]module.Pipeline, error) {
	pipelines := []module.Pipeline{}
	if number == 0 {
		return pipelines, nil
	}
	for i := uint8(0); i < number; i++ {
		mid, err := module.GenMID(module.TYPE_PIPELINE, snGen.Get(), nil)
		if err != nil {
			return pipelines, err
		}
		p, err := pipeline.New(mid, genItemProcessors(dirPath), module.CalculateScoreSimple)
		if err != nil {
			return pipelines, err
		}
		p.SetFailFast(true)
		pipelines = append(pipelines, p)
	}
	return pipelines, nil
}
