package buffer

import (
	"fmt"
	"github.com/kyl2016/WebCrawler/errors"
	"sync"
	"sync/atomic"
)

type Pool interface {
	BufferCap() uint32
	MaxBufferNumber() uint32
	BufferNumber() uint32
	Total() uint64
	Put(datum interface{}) error
	Get() (datum interface{}, err error)
	Close() bool
	Closed() bool
}

type myPool struct {
	bufferCap       uint32
	maxBufferNumber uint32
	bufferNumber    uint32
	total           uint64
	bufCh           chan Buffer
	closed          uint32
	rwlock          sync.RWMutex
}

func NewPool(bufferCap uint32, maxBufferNumber uint32) (Pool, error) {
	if bufferCap == 0 {
		errMsg := fmt.Sprintf("illegal buffer cap for buffer pool: %d", bufferCap)
		return nil, errors.NewIllegalParameterError(errMsg)
	}
	if maxBufferNumber == 0 {
		errMsg := fmt.Sprintf("illegal max buffer number for buffer pool: %d", maxBufferNumber)
		return nil, errors.NewIllegalParameterError(errMsg)
	}
	bufCh := make(chan Buffer, maxBufferNumber)
	buf, _ := NewBuffer(bufferCap)
	bufCh <- buf
	return &myPool{
		bufferCap:       bufferCap,
		maxBufferNumber: maxBufferNumber,
		bufferNumber:    1,
		bufCh:           bufCh,
	}, nil
}

func (p *myPool) BufferCap() uint32 {
	return p.bufferCap
}

func (p *myPool) MaxBufferNumber() uint32 {
	return p.maxBufferNumber
}

func (p *myPool) BufferNumber() uint32 {
	return atomic.LoadUint32(&p.bufferNumber)
}

func (p *myPool) Total() uint64 {
	return atomic.LoadUint64(&p.total)
}

func (p *myPool) Put(datum interface{}) (err error) {
	if p.Closed() {
		return ErrClosedBufferPool
	}
	var count uint32
	maxCount := p.BufferNumber() * 5
	var ok bool
	for buf := range p.bufCh {
		ok, err = p.putData(buf, datum, &count, maxCount)
		if ok || err != nil {
			break
		}
	}
	return
}

func (p *myPool) putData(buf Buffer, datum interface{}, count *uint32, maxCount uint32) (ok bool, err error) {
	if p.Closed() {
		return false, ErrClosedBufferPool
	}
	defer func() {
		p.rwlock.RLock()
		if p.Closed() {
			atomic.AddUint32(&p.bufferNumber, ^uint32(0))
			err = ErrClosedBufferPool
		} else {
			p.bufCh <- buf
		}
		p.rwlock.RUnlock()
	}()
	ok, err = buf.Put(datum)
	if ok {
		atomic.AddUint64(&p.total, 1)
		return
	}
	if err != nil {
		return
	}
	(*count)++
	if *count >= maxCount && p.BufferNumber() < p.MaxBufferNumber() {
		p.rwlock.Lock()
		if p.BufferNumber() < p.MaxBufferNumber() {
			if p.Closed() {
				p.rwlock.Unlock()
				return
			}
			newBuf, _ := NewBuffer(p.bufferCap)
			newBuf.Put(datum)
			p.bufCh <- newBuf
			atomic.AddUint32(&p.bufferNumber, 1)
			atomic.AddUint64(&p.total, 1)
			ok = true
		}
		p.rwlock.Unlock()
		*count = 0
	}
	return
}

func (p *myPool) Get() (datum interface{}, err error) {
	if p.Closed() {
		return nil, ErrClosedBufferPool
	}
	var count uint32
	maxCount := p.BufferNumber() * 10
	for buf := range p.bufCh {
		datum, err = p.getData(buf, &count, maxCount)
		if datum != nil || err != nil {
			break
		}
	}
	return
}

func (p *myPool) getData(buf Buffer, count *uint32, maxCount uint32) (datum interface{}, err error) {
	if p.Closed() {
		return nil, ErrClosedBufferPool
	}
	defer func() {
		if *count >= maxCount && buf.Len() == 0 && p.BufferNumber() > 1 {
			buf.Close()
			atomic.AddUint32(&p.bufferNumber, ^uint32(0))
			*count = 0
			return
		}
		p.rwlock.RLock()
		if p.Closed() {
			atomic.AddUint32(&p.bufferNumber, ^uint32(0))
			err = ErrClosedBufferPool
		} else {
			p.bufCh <- buf
		}
		p.rwlock.RUnlock()
	}()
	datum, err = buf.Get()
	if datum != nil {
		atomic.AddUint64(&p.total, ^uint64(0))
		return
	}
	if err != nil {
		return
	}
	(*count)++
	return
}

func (p *myPool) Close() bool {
	if !atomic.CompareAndSwapUint32(&p.closed, 0, 1) {
		return false
	}
	p.rwlock.Lock()
	defer p.rwlock.Unlock()
	close(p.bufCh)
	for buf := range p.bufCh {
		buf.Close()
	}
	return true
}
func (p *myPool) Closed() bool {
	if atomic.LoadUint32(&p.closed) == 1 {
		return true
	}
	return false
}
