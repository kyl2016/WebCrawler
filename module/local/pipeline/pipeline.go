package pipeline

import (
	"fmt"
	"github.com/Qihoo360/poseidon/builder/docformat/src/github.com/donnie4w/go-logger/logger"
	"github.com/kyl2016/WebCrawler/module"
	"github.com/kyl2016/WebCrawler/module/stub"
)

type myPipeline struct {
	stub.ModuleInternal
	itemProcessors []module.ProcessItem
	failFast       bool
}

func New(mid module.MID, itemProcessors []module.ProcessItem, scoreCalculator module.CalculateScore) (module.Pipeline, error) {
	moduleBase, err := stub.NewModuleInternal(mid, scoreCalculator)
	if err != nil {
		return nil, err
	}
	if itemProcessors == nil {
		return nil, genParameterError("nil item processor list")
	}
	if len(itemProcessors) == 0 {
		return nil, genParameterError("empty item processor list")
	}
	var innerProcessors []module.ProcessItem
	for i, processor := range itemProcessors {
		if processor == nil {
			err := genParameterError(fmt.Sprintf("nil item processor[%d]", i))
			return nil, err
		}
		innerProcessors = append(innerProcessors, processor)
	}
	return &myPipeline{
		ModuleInternal: moduleBase,
		itemProcessors: innerProcessors,
	}, nil
}

func (pipeline *myPipeline) ItemProcessors() []module.ProcessItem {
	processors := make([]module.ProcessItem, len(pipeline.itemProcessors))
	copy(processors, pipeline.itemProcessors)
	return processors
}

func (pipeline *myPipeline) Send(item module.Item) []error {
	pipeline.ModuleInternal.IncrHandlingNumber()
	defer pipeline.ModuleInternal.DecrHandlingNumber()
	pipeline.ModuleInternal.IncrCalledCount()
	var errs []error
	if item == nil {
		err := genParameterError("nil item")
		errs = append(errs, err)
		return errs
	}
	pipeline.IncrAcceptedCount()
	logger.Infof("Process item %+v... \n", item)
	var currentItem = item
	for _, processor := range pipeline.itemProcessors {
		processedItem, err := processor(currentItem)
		if err != nil {
			errs = append(errs, err)
			if pipeline.failFast {
				break
			}
		}
		if processedItem != nil {
			currentItem = processedItem
		}
	}
	if len(errs) == 0 {
		pipeline.ModuleInternal.IncrCompletedCount()
	}
	return errs
}

func (pipeline *myPipeline) FailFast() bool {
	return pipeline.failFast
}

func (pipeline *myPipeline) SetFailFast(failFast bool) {
	pipeline.failFast = failFast
}

type extraSummaryStruct struct {
	FailFast        bool `json:"fail_fast"`
	ProcessorNumber int  `json:"processor_number"`
}

func (pipeline *myPipeline) Summary() module.SummaryStruct {
	summary := pipeline.ModuleInternal.Summary()
	summary.Extra = extraSummaryStruct{
		FailFast:        pipeline.failFast,
		ProcessorNumber: len(pipeline.itemProcessors),
	}
	return summary
}
