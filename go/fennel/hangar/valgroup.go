package hangar

import (
	"fmt"
	"sync"
)

var stringSet = sync.Pool{New: func() interface{} {
	return make(map[string]struct{}, 128)
}}
var stringToByteSlice = sync.Pool{New: func() interface{} {
	return make(map[string][]byte, 128)
}}

func freeStringToByteSlice(m map[string][]byte) {
	for k := range m {
		delete(m, k)
	}
	stringToByteSlice.Put(m)
}

func freeStringSet(m map[string]struct{}) {
	for k := range m {
		delete(m, k)
	}
	stringSet.Put(m)
}

func (vg ValGroup) Valid() bool {
	return len(vg.Fields) == len(vg.Values)
}

func (vg *ValGroup) Update(other ValGroup) error {
	if !vg.Valid() || !other.Valid() {
		return fmt.Errorf("ValGroup.Update: invalid ValGroup")
	}
	newData := stringToByteSlice.Get().(map[string][]byte)
	defer freeStringToByteSlice(newData)
	for i := range other.Fields {
		newData[string(other.Fields[i])] = other.Values[i]
	}
	written := stringSet.Get().(map[string]struct{})
	defer freeStringSet(written)
	for i, field := range vg.Fields {
		if nv, ok := newData[string(field)]; ok {
			written[string(field)] = struct{}{}
			vg.Values[i] = nv
		}
	}
	// extend the size of fields/values by the needed amount
	tobeWritten := len(other.Fields) - len(written)
	if tobeWritten > 0 {
		idx := len(vg.Fields)
		vg.Fields = append(vg.Fields, make(Fields, tobeWritten)...)
		vg.Values = append(vg.Values, make(Values, tobeWritten)...)
		for i, field := range other.Fields {
			if _, ok := written[string(field)]; !ok {
				vg.Fields[idx] = field
				vg.Values[idx] = other.Values[i]
				idx += 1
			}
		}
	}
	// update the expires unless other's expires is negative, in which case
	// leave it unchanged
	if other.Expiry >= 0 {
		vg.Expiry = other.Expiry
	}
	return nil
}

func (vg *ValGroup) Select(fields Fields) {
	data := stringSet.Get().(map[string]struct{})
	defer freeStringSet(data)
	for i := range fields {
		data[string(fields[i])] = struct{}{}
	}
	oldFields := vg.Fields
	oldValues := vg.Values
	vg.Fields = vg.Fields[:0]
	vg.Values = vg.Values[:0]
	for i, field := range oldFields {
		if _, ok := data[string(field)]; ok {
			vg.Fields = append(vg.Fields, field)
			vg.Values = append(vg.Values, oldValues[i])
		}
	}
}

func (vg *ValGroup) Del(fields Fields) {
	del := make(map[string]struct{}, len(fields))
	for i := range fields {
		del[string(fields[i])] = struct{}{}
	}
	oldFields := vg.Fields
	oldValues := vg.Values
	vg.Fields = vg.Fields[:0]
	vg.Values = vg.Values[:0]
	write := 0
	for i, field := range oldFields {
		if _, ok := del[string(field)]; !ok {
			vg.Fields = append(vg.Fields, field)
			vg.Values = append(vg.Values, oldValues[i])
			write++
		}
	}
}
