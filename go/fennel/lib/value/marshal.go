package value

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

type marshalType byte

const (
	m_int    marshalType = 1
	m_double             = 2
	m_string             = 3
	m_bool               = 4
	m_list               = 5
	m_dict               = 6
	m_table              = 7
	m_nil                = 8
)

func intToBytes(n int) []byte {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(n))
	return bs
}

func bytesToInt(bytes []byte) int {
	return int(binary.LittleEndian.Uint32(bytes))
}

func UnmarshalJSON(bytes []byte) (Value, error) {
	if len(bytes) < 1 {
		return Nil, fmt.Errorf("valid value marshal should at least be 1 byte long")
	}
	v, n_read, err := decode(bytes)
	if err != nil {
		return Nil, err
	}
	if n_read < len(bytes) {
		return Nil, fmt.Errorf("not all bytes consumed in unmarshalling")
	}
	return v, nil
}

func decode(bytes []byte) (Value, int, error) {
	var v Value
	var n int
	var err error
	switch marshalType(bytes[0]) {
	case m_int:
		v, n, err = decodeInt(bytes[1:])
	case m_list:
		v, n, err = decodeList(bytes[1:])
	case m_string:
		v, n, err = decodeString(bytes[1:])
	case m_double:
		v, n, err = decodeDouble(bytes[1:])
	case m_bool:
		v, n, err = decodeBool(bytes[1:])
	case m_nil:
		v, n, err = decodeNil(bytes[1:])
	case m_dict:
		v, n, err = decodeDict(bytes[1:])
	case m_table:
		v, n, err = decodeTable(bytes[1:])
	default:
		return Nil, 1, fmt.Errorf("invalid marshal code %v", bytes[0])
	}
	return v, n + 1, err
}

func marshal(inner interface{}, code marshalType) ([]byte, error) {
	m, err := json.Marshal(inner)
	if err != nil {
		return nil, err
	}
	sz := 1 + 4 + len(m)
	buf := make([]byte, 0, sz)
	buf = append(buf, byte(code))
	buf = append(buf, intToBytes(len(m))...)
	buf = append(buf, m...)
	return buf, nil
}

func unmarshal(b []byte, holder interface{}) (int, error) {
	sz := bytesToInt(b[:4])
	b = b[4:]
	err := json.Unmarshal(b[:sz], holder)
	if err == nil {
		return 4 + sz, nil
	} else {
		return 4 + sz, err
	}
}

func (I Int) MarshalJSON() ([]byte, error) {
	return marshal(int(I), m_int)
}

func decodeInt(b []byte) (Value, int, error) {
	var i Int
	n, err := unmarshal(b, &i)
	if err != nil {
		return Nil, n, err
	} else {
		return i, n, nil
	}
}

func (d Double) MarshalJSON() ([]byte, error) {
	return marshal(float64(d), m_double)
}

func decodeDouble(b []byte) (Value, int, error) {
	var d Double
	n, err := unmarshal(b, &d)
	if err != nil {
		return Nil, n, err
	} else {
		return d, n, nil
	}
}

func (s String) MarshalJSON() ([]byte, error) {
	return marshal(string(s), m_string)
}

func decodeString(b []byte) (Value, int, error) {
	var s String
	n, err := unmarshal(b, &s)
	if err != nil {
		return Nil, n, err
	} else {
		return s, n, nil
	}
}

func (b Bool) MarshalJSON() ([]byte, error) {
	return marshal(bool(b), m_bool)
}

func decodeBool(b []byte) (Value, int, error) {
	var s Bool
	n, err := unmarshal(b, &s)
	if err != nil {
		return Nil, n, err
	} else {
		return Bool(s), n, nil
	}
}

func (l List) MarshalJSON() ([]byte, error) {
	// since every value's encoding takes at least 2 bytes, this list
	// will take at least 2*l bytes (though likely more)
	m := make([]byte, 0, 2*len(l))
	for _, v := range l {
		vm, err := v.MarshalJSON()
		if err != nil {
			return nil, err
		}
		m = append(m, vm...)
	}
	buf := make([]byte, 0, 5+len(m))
	buf = append(buf, byte(m_list))
	buf = append(buf, intToBytes(len(m))...)
	buf = append(buf, m...)
	return buf, nil
}

func decodeList(b []byte) (Value, int, error) {
	values := make([]Value, 0)
	sz := bytesToInt(b[:4])
	b = b[4:]
	n_read := 0
	for n_read < sz {
		v, n, err := decode(b)
		if err != nil {
			return Nil, n_read, err
		}
		values = append(values, v)
		n_read += n
		b = b[n:]
	}
	if n_read > sz {
		return Nil, n_read, fmt.Errorf("read too many bytes, ill formated list")
	}
	l, _ := NewList(values)
	return l, 4 + sz, nil
}

func (n nil_) MarshalJSON() ([]byte, error) {
	return []byte{byte(m_nil)}, nil
}

func decodeNil(b []byte) (Value, int, error) {
	return Nil, 0, nil
}

func (d Dict) MarshalJSON() ([]byte, error) {
	// every value takes >= 2 bytes and for each element of dict
	// we need to marshal both key/value, hence at least 4 bytes
	m := make([]byte, 0, 4*len(d))
	for k, v := range d {
		ks := String(k)
		km, err := ks.MarshalJSON()
		if err != nil {
			return nil, err
		}
		m = append(m, km...)
		vm, err := v.MarshalJSON()
		if err != nil {
			return nil, err
		}
		m = append(m, vm...)
	}
	buf := make([]byte, 0, 5+len(m))
	buf = append(buf, byte(m_dict))
	buf = append(buf, intToBytes(len(m))...)
	buf = append(buf, m...)
	return buf, nil
}

func decodeDict(b []byte) (Value, int, error) {
	values := make(map[string]Value, 0)
	sz := bytesToInt(b[:4])
	b = b[4:]
	n_read := 0
	for n_read < sz {
		// first read a key
		str, n, err := decode(b)
		if err != nil {
			return Nil, n_read, err
		}
		n_read += n
		b = b[n:]
		s := string(str.(String))
		//now read its value
		v, n, err := decode(b)
		if err != nil {
			return Nil, n_read, err
		}
		n_read += n
		b = b[n:]
		values[s] = v
	}
	if n_read > sz {
		return Nil, n_read, fmt.Errorf("read too many bytes, ill formated list")
	}
	d, _ := NewDict(values)
	return d, 4 + sz, nil
}

func (t Table) MarshalJSON() ([]byte, error) {
	// each list takes at least 6 bytes to encode
	// so table will take at least 6R, if not more
	m := make([]byte, 0, 6*len(t.rows))
	for _, v := range t.rows {
		vm, err := v.MarshalJSON()
		if err != nil {
			return nil, err
		}
		m = append(m, vm...)
	}
	buf := make([]byte, 0, 5+len(m))
	buf = append(buf, byte(m_table))
	buf = append(buf, intToBytes(len(m))...)
	buf = append(buf, m...)
	return buf, nil
}

func decodeTable(b []byte) (Value, int, error) {
	table := NewTable()
	sz := bytesToInt(b[:4])
	b = b[4:]
	n_read := 0
	for n_read < sz {
		ld, n, err := decode(b)
		if err != nil {
			return Nil, n, err
		}
		n_read += n
		b = b[:n]
		var d Dict
		var ok bool
		if d, ok = ld.(Dict); !ok {
			return Nil, n, fmt.Errorf("ill formed json: expected dict")
		}
		table.Append(d)
	}
	if n_read != sz {
		return Nil, n_read, fmt.Errorf("ill formed json")
	}
	return table, 4 + sz, nil
}
