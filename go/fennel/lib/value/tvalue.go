package value

type tag struct {
	label string
	value string
}

type tagset struct {
	parents map[*tagset]struct{}
	tag     *tag
}

func newTagset() *tagset {
	return &tagset{
		parents: make(map[*tagset]struct{}),
		tag:     nil,
	}
}

func fuseTagsets(tagsets ...*tagset) *tagset {
	var ret *tagset = nil
	for _, ts := range tagsets {
		if ts == nil {
			continue
		}
		if ret == nil {
			ret = newTagset()
		}
		if _, ok := ret.parents[ts]; !ok {
			ret.parents[ts] = struct{}{}
		}
	}
	return ret
}

func (ts *tagset) addTag(label, val string) *tagset {
	return &tagset{
		parents: map[*tagset]struct{}{ts: {}},
		tag: &tag{
			label: label,
			value: val,
		},
	}
}

func (ts *tagset) Tags() map[string][]string {
	seen := make(map[string]map[string]struct{}, 0)
	tagsets := []*tagset{ts}
	start := 0
	for start < len(tagsets) {
		ts := tagsets[start]
		start += 1
		tag := ts.tag
		if tag != nil {
			if _, ok := seen[tag.label]; !ok {
				seen[tag.label] = make(map[string]struct{})
			}
			if _, ok := seen[tag.label][tag.value]; !ok {
				seen[tag.label][tag.value] = struct{}{}
			}
		}
		for p, _ := range ts.parents {
			if p != nil {
				tagsets = append(tagsets, p)
			}
		}
	}
	ret := make(map[string][]string, len(seen))
	for k, vs := range seen {
		ret[k] = make([]string, 0, len(seen[k]))
		for v, _ := range vs {
			ret[k] = append(ret[k], v)
		}
	}
	return ret
}

func (ts *tagset) clone() *tagset {
	return ts
}

type TValue struct {
	Value
	*tagset
}

func NewTValue(v Value, tvs ...TValue) TValue {
	ret := TValue{
		Value:  v,
		tagset: nil,
	}
	switch len(tvs) {
	case 0:
		ret.tagset = nil
	case 1:
		ret.tagset = tvs[0].tagset
	default:
		ret.InheritTags(tvs...)
	}

	return ret
}

func (tv *TValue) Tag(label, val string) {
	tv.tagset = tv.tagset.addTag(label, val)
}

func (tv *TValue) SelfTag(label string) {
	tv.Tag(label, tv.Value.String())
}

func (tv *TValue) InheritTags(tvs ...TValue) {
	tagsets := make([]*tagset, 0, 1+len(tvs))
	for i := range tvs {
		tagsets = append(tagsets, tvs[i].tagset)
	}
	tagsets = append(tagsets, tv.tagset)
	tv.tagset = fuseTagsets(tagsets...)
}

func (tv *TValue) Tags() map[string][]string {
	if tv.tagset == nil {
		return map[string][]string{}
	}
	return tv.tagset.Tags()
}
