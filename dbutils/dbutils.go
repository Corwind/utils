package dbutils

import (
	"bytes"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

func DbSaveEntity(db fdb.Database, t tuple.Tuple, buffer *bytes.Buffer) (interface{}, error) {
	return db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(t, buffer.Bytes())
		return tr.Get(t).Get()
	})
}

func DbFetchEntity(db fdb.Database, t tuple.Tuple) (interface{}, error) {
	return db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		ret, err := tr.Get(t).Get()
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, fmt.Errorf("entity not found")
		}
		return ret, err
	})
}

func DbFetchRange(db fdb.Database, range_ tuple.Tuple) ([]interface{}, error) {
	values := make([]interface{}, 0)

	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		rangePrefix, err := fdb.PrefixRange(range_.Pack())
		if err != nil {
			return nil, err
		}
		rangeIterator, err := tr.GetRange(rangePrefix, fdb.RangeOptions{}).Iterator(), nil
		if err != nil {
			return nil, err
		}

		for rangeIterator.Advance() {
			kv := rangeIterator.MustGet()
			values = append(values, kv.Value)
		}

		return nil, nil
	})

	if err != nil {
		return nil, err
	}
	return values, nil
}
