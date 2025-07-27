package atomic

import (
	"encoding/json"
	"math"
	"sync/atomic"
)

func DefaultValue[T any]() T {
	var defaultValue T
	return defaultValue
}

type TypedValue[T any] struct {
	_     noCopy
	value atomic.Value
}

// tValue is a struct with determined type to resolve atomic.Value usages with interface types
// https://github.com/golang/go/issues/22550
//
// The intention to have an atomic value store for errors. However, running this code panics:
// panic: sync/atomic: store of inconsistently typed value into Value
// This is because atomic.Value requires that the underlying concrete type be the same (which is a reasonable expectation for its implementation).
// When going through the atomic.Value.Store method call, the fact that both these are of the error interface is lost.
type tValue[T any] struct {
	value T
}

func (t *TypedValue[T]) Load() T {
	value, _ := t.LoadOk()
	return value
}

func (t *TypedValue[T]) LoadOk() (_ T, ok bool) {
	value := t.value.Load()
	if value == nil {
		return DefaultValue[T](), false
	}
	return value.(tValue[T]).value, true
}

func (t *TypedValue[T]) Store(value T) {
	t.value.Store(tValue[T]{value})
}

func (t *TypedValue[T]) Swap(new T) T {
	old := t.value.Swap(tValue[T]{new})
	if old == nil {
		return DefaultValue[T]()
	}
	return old.(tValue[T]).value
}

func (t *TypedValue[T]) CompareAndSwap(old, new T) bool {
	return t.value.CompareAndSwap(tValue[T]{old}, tValue[T]{new}) ||
		// In the edge-case where [atomic.Value.Store] is uninitialized
		// and trying to compare with the zero value of T,
		// then compare-and-swap with the nil any value.
		(any(old) == any(DefaultValue[T]()) && t.value.CompareAndSwap(any(nil), tValue[T]{new}))
}

func (t *TypedValue[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Load())
}

func (t *TypedValue[T]) UnmarshalJSON(b []byte) error {
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	t.Store(v)
	return nil
}

func (t *TypedValue[T]) MarshalYAML() (any, error) {
	return t.Load(), nil
}

func (t *TypedValue[T]) UnmarshalYAML(unmarshal func(any) error) error {
	var v T
	if err := unmarshal(&v); err != nil {
		return err
	}
	t.Store(v)
	return nil
}

func NewTypedValue[T any](t T) (v TypedValue[T]) {
	v.Store(t)
	return
}

type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// Update TypedValue[map[K]V]
func (t *TypedValue[T]) Update(f func(old T) (new T)) {
	switch any(DefaultValue[T]()).(type) {
	case map[string]float64:
		old := t.Load()
		n := f(old)
		t.Store(n)
		return
	}
	for {
		old := t.Load()
		n := f(old)
		if t.CompareAndSwap(old, n) {
			return
		}
	}
}

func CloneMap[K comparable, V any](m map[K]V) map[K]V {
	if m == nil {
		return make(map[K]V)
	}
	newMap := make(map[K]V, len(m))
	for k, v := range m {
		newMap[k] = v
	}
	return newMap
}

// Float64 atomic.Float64
type Float64 struct {
	value uint64
}

func (f *Float64) Store(val float64) {
	atomic.StoreUint64(&f.value, math.Float64bits(val))
}

func (f *Float64) Load() float64 {
	return math.Float64frombits(atomic.LoadUint64(&f.value))
}

func (f *Float64) Add(delta float64) float64 {
	for {
		oldBits := atomic.LoadUint64(&f.value)
		old := math.Float64frombits(oldBits)
		n := old + delta
		newBits := math.Float64bits(n)
		if atomic.CompareAndSwapUint64(&f.value, oldBits, newBits) {
			return n
		}
	}
}

func (f *Float64) Swap(new float64) float64 {
	for {
		oldBits := atomic.LoadUint64(&f.value)
		newBits := math.Float64bits(new)
		if atomic.CompareAndSwapUint64(&f.value, oldBits, newBits) {
			return math.Float64frombits(oldBits)
		}
	}
}

func (f *Float64) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.Load())
}

func (f *Float64) UnmarshalJSON(b []byte) error {
	var v float64
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	f.Store(v)
	return nil
}
