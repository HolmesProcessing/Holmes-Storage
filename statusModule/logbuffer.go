package Status

import (
	"sync"
)

// A simple log buffer, discards old messages if to make space for new ones.
// This is only a temporary solution, as in the end the database
// will be responsible for saving logs and tombstones in Cassandra
// make it a lot easier.
func NewDefaultLogBuffer() *LogBuffer {
	return NewLogBuffer(0x400) // 1024 lines
}
func NewLogBuffer(size int) *LogBuffer {
	return &LogBuffer{
		Buffer:   make([]string, size),
		Capacity: size,
		Last:     -1,
		Length:   0,
	}
}

type LogBuffer struct {
	sync.Mutex
	Buffer   []string
	Capacity int
	Last     int
	Length   int
}

func (this *LogBuffer) Append(msgs []string) {
	this.Lock()
	defer this.Unlock()
	for _, msg := range msgs {
		this.Last = (this.Last + 1) % this.Capacity
		this.Buffer[this.Last] = msg
	}
	this.Length += len(msgs)
	if this.Length > this.Capacity {
		this.Length = this.Capacity
	}
}

func (this *LogBuffer) GetLastN(n int) []string {
	if n > this.Length {
		n = this.Length
	}
	if n > this.Last {
		// we have to copy ...
		// [xxxxxx|------xx]
		// 0      l      n
		// -> remainder = l-n
		// -> Buffer[(Capacity+remainder):]
		r := make([]string, n)
		oflen := this.Capacity + (this.Last - n)
		copy(r, this.Buffer[oflen:])
		copy(r[oflen:], this.Buffer[:this.Last])
		return r
	}
	return this.Buffer[this.Last-n : this.Last]
}
