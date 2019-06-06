package module

import "net/http"

type Data interface {
	Valid() bool
}

type Request struct {
	httpReq *http.Request
	depth   uint32
}

func NewRequest(httpReq *http.Request, depth uint32) *Request {
	return &Request{httpReq, depth}
}

func (req *Request) HTTPReq() *http.Request {
	return req.httpReq
}

func (req *Request) Depth() uint32 {
	return req.depth
}

func (req *Request) Valid() bool {
	return req.httpReq != nil && req.httpReq.URL != nil
}

type Response struct {
	httpRes *http.Response
	depth   uint32
}

func NewResponse(httpRes *http.Response, depth uint32) *Response {
	return &Response{httpRes, depth}
}

func (res *Response) HTTPRes() *http.Response {
	return res.httpRes
}

func (res *Response) Depth() uint32 {
	return res.depth
}

func (res *Response) Valid() bool {
	return res.httpRes != nil && res.httpRes.Body != nil
}

type Item map[string]interface{}

func (item Item) Valid() bool {
	return item != nil
}
