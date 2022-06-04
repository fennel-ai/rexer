package store

import (
	"fmt"
)

func (vg ValGroup) Valid() bool {
	return len(vg.Fields) == len(vg.Values)
}

func (vg *ValGroup) Update(other ValGroup) error {
	if !vg.Valid() || !other.Valid() {
		return fmt.Errorf("ValGroup.Update: invalid ValGroup")
	}
	oldData := make(map[string][]byte, len(vg.Fields))
	for i := range vg.Fields {
		oldData[string(vg.Fields[i])] = vg.Values[i]
	}
	newData := make(map[string][]byte, len(other.Fields))
	for i := range other.Fields {
		newData[string(other.Fields[i])] = other.Values[i]
	}
	oldFields := vg.Fields
	oldValues := vg.Values
	vg.Fields = vg.Fields[:0]
	vg.Values = vg.Values[:0]
	for i, field := range oldFields {
		v := oldValues[i]
		if nv, ok := newData[string(field)]; ok {
			v = nv
		}
		vg.Fields = append(vg.Fields, field)
		vg.Values = append(vg.Values, v)
	}
	for _, field := range other.Fields {
		if _, ok := oldData[string(field)]; !ok {
			vg.Fields = append(vg.Fields, field)
			vg.Values = append(vg.Values, newData[string(field)])
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
	data := make(map[string]struct{}, len(fields))
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
