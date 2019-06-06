package buffer

import "errors"

var ErrClosedBufferPool = errors.New("closed buffer pool")

var ErrClosedBuffer = errors.New("closed buffer")
