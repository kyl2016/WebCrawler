package internal

import (
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/kyl2016/WebCrawler/module"
	"net/http"
	"net/url"
	"path"
	"strings"
)

func genResponseParsers() []module.ParseResponse {
	parseLink := func(httpResp *http.Response, respDepth uint32) ([]module.Data, []error) {
		dataList := make([]module.Data, 0)
		if httpResp == nil {
			return nil, []error{fmt.Errorf("nil HTTP response")}
		}
		httpReq := httpResp.Request
		if httpReq == nil {
			return nil, []error{fmt.Errorf("nil HTTP request")}
		}
		reqURL := httpReq.URL
		if httpResp.StatusCode != 200 {
			err := fmt.Errorf("unsupported status code %d (requestURL: %s)", httpResp.StatusCode, reqURL)
			return nil, []error{err}
		}
		body := httpResp.Body
		if body == nil {
			err := fmt.Errorf("nil HTTP response body (requestURL: %s)", reqURL)
			return nil, []error{err}
		}
		var matchContentType bool
		if httpResp.Header != nil {
			contentTypes := httpResp.Header["Content-Type"]
			for _, ct := range contentTypes {
				if strings.HasPrefix(ct, "text/html") {
					matchContentType = true
					break
				}
			}
		}
		if !matchContentType {
			return dataList, nil
		}
		doc, err := goquery.NewDocumentFromReader(body)
		if err != nil {
			return dataList, []error{err}
		}
		errs := make([]error, 0)
		doc.Find("a").Each(func(index int, sel *goquery.Selection) {
			href, exists := sel.Attr("href")
			if !exists || href == "" || href == "#" || href == "/" {
				return
			}
			href = strings.TrimSpace(href)
			lowerHref := strings.ToLower(href)
			if href == "" || strings.HasPrefix(lowerHref, "javascript") {
				return
			}
			aURL, err := url.Parse(href)
			if err != nil {
				logger.Warnf("An error occurs when parsing attribute %q in tag %q : %s (href: %s)", err, "href", "a", href)
				return
			}
			if !aURL.IsAbs() {
				aURL = reqURL.ResolveReference(aURL)
			}
			httpReq, err := http.NewRequest("GET", aURL.String(), nil)
			if err != nil {
				errs = append(errs, err)
			} else {
				req := module.NewRequest(httpReq, respDepth)
				dataList = append(dataList, req)
			}
		})
		doc.Find("img").Each(func(index int, sel *goquery.Selection) {
			imgSrc, exists := sel.Attr("src")
			if !exists || imgSrc == "" || imgSrc == "#" || imgSrc == "/" {
				return
			}
			imgSrc = strings.TrimSpace(imgSrc)
			imgURL, err := url.Parse(imgSrc)
			if err != nil {
				errs = append(errs, err)
				return
			}
			if !imgURL.IsAbs() {
				imgURL = reqURL.ResolveReference(imgURL)
			}
			httpReq, err := http.NewRequest("GET", imgURL.String(), nil)
			if err != nil {
				errs = append(errs, err)
			} else {
				req := module.NewRequest(httpReq, respDepth)
				dataList = append(dataList, req)
			}
		})
		return dataList, errs
	}
	parseImg := func(httpResp *http.Response, respDepth uint32) ([]module.Data, []error) {
		if httpResp == nil {
			return nil, []error{fmt.Errorf("nil HTTP response")}
		}
		httpReq := httpResp.Request
		if httpReq == nil {
			return nil, []error{fmt.Errorf("nil HTTP request")}
		}
		reqURL := httpReq.URL
		if httpResp.StatusCode != 200 {
			err := fmt.Errorf("unsupported status code %d (requestURL: %s)", httpResp.StatusCode, reqURL)
			return nil, []error{err}
		}
		httpRespBody := httpResp.Body
		if httpRespBody == nil {
			return nil, []error{fmt.Errorf("nil HTTP response body (requestURL: %s)", reqURL)}
		}
		dataList := make([]module.Data, 0)
		var pictureFormat string
		if httpResp.Header != nil {
			contentTypes := httpResp.Header["Content-Type"]
			var contentType string
			for _, ct := range contentTypes {
				if strings.HasPrefix(ct, "image") {
					contentType = ct
					break
				}
			}
			index1 := strings.Index(contentType, "/")
			index2 := strings.Index(contentType, ";")
			if index1 > 0 {
				if index2 < 0 {
					pictureFormat = contentType[index1+1:]
				} else if index1 < index2 {
					pictureFormat = contentType[index1+1 : index2]
				}
			}
		}
		if pictureFormat == "" {
			return dataList, nil
		}
		item := make(map[string]interface{})
		item["reader"] = httpRespBody
		item["name"] = path.Base(reqURL.Path)
		item["ext"] = pictureFormat
		dataList = append(dataList, module.Item(item))
		return dataList, nil
	}
	return []module.ParseResponse{parseLink, parseImg}
}
