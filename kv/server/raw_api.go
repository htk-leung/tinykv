package server

import (
	"context" // go package

	// "github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/errors"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

var (
	ErrEmptyReqCF       = errors.New("Missing CF in request")
	ErrEmptyReqKey      = errors.New("Missing Key in request")
	ErrEmptyReqVal      = errors.New("Missing Val in request")
	ErrEmptyReqStartKey = errors.New("Missing StartKey in request")
	ErrEmptyReqLimit    = errors.New("Missing iteration limit in request")
	ErrEmptyReqContext  = errors.New("Missing context in request")
)

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).

	// new response
	resp := &kvrpcpb.RawGetResponse{}

	// get vars
	reqContext := req.GetContext()
	// if reqContext == nil {
	// 	return resp, ErrEmptyReqContext
	// }
	reqCF := req.GetCf()
	reqKey := req.GetKey()
	if reqCF == "" || reqKey == nil {
		resp.NotFound = true
		return resp, nil
	}

	// get reader
	reader, _ := server.storage.Reader(reqContext)
	defer reader.Close() // delay discarding txn

	// get value and error
	resp.Value, _ = reader.GetCF(reqCF, reqKey)

	// if not found set bool
	if resp.Value == nil {
		resp.NotFound = true
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	resp := &kvrpcpb.RawPutResponse{}

	// get vars
	reqContext := req.GetContext()
	// if reqContext == nil {
	// 	return resp, ErrEmptyReqContext
	// }
	reqCF := req.GetCf()
	if reqCF == "" {
		return resp, ErrEmptyReqCF
	}
	reqKey := req.GetKey()
	if reqKey == nil {
		return resp, ErrEmptyReqKey
	}
	reqVal := req.GetValue()
	if reqVal == nil {
		return resp, ErrEmptyReqVal
	}

	// create batch
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    reqCF,
				Key:   reqKey,
				Value: reqVal,
			},
		},
	}

	// acquire latch
	keys := [][]byte{reqKey}
	server.Latches.WaitForLatches(keys)

	// write to storage
	err := server.storage.Write(reqContext, batch)
	if err != nil {
		resp.Error = err.Error()
	}

	// release latch
	server.Latches.ReleaseLatches(keys)

	// return
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted

	// create response
	resp := &kvrpcpb.RawDeleteResponse{}

	// get vars
	reqContext := req.GetContext()
	// if reqContext == nil {
	// 	return resp, ErrEmptyReqContext
	// }
	reqCF := req.GetCf()
	if reqCF == "" {
		return resp, ErrEmptyReqCF
	}
	reqKey := req.GetKey()
	if reqKey == nil {
		return resp, ErrEmptyReqKey
	}

	// create batch
	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  reqCF,
				Key: reqKey,
			},
		},
	}

	// acquire latch
	keys := [][]byte{reqKey}
	server.Latches.WaitForLatches(keys)

	// write to storage
	err := server.storage.Write(reqContext, batch)
	if err != nil {
		resp.Error = err.Error()
	}

	// release latch
	server.Latches.ReleaseLatches(keys)

	// return
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	// create response
	resp := &kvrpcpb.RawScanResponse{}

	// get vars
	reqContext := req.GetContext()
	// if reqContext == nil {
	// 	resp.Error = ErrEmptyReqContext.Error()
	// 	return resp, nil
	// }
	reqSK := req.GetStartKey()
	if reqSK == nil {
		resp.Error = ErrEmptyReqStartKey.Error()
		return resp, nil
	}
	reqLimit := req.GetLimit()
	if reqLimit == 0 {
		resp.Error = ErrEmptyReqLimit.Error()
		return resp, nil
	}
	reqCF := req.GetCf()
	if reqCF == "" {
		resp.Error = ErrEmptyReqCF.Error()
		return resp, nil
	}

	// get reader
	reader, _ := server.storage.Reader(reqContext)
	defer reader.Close()

	// get it
	it := reader.IterCF(reqCF)
	defer it.Close()

	// scan
	pairs := make([]*kvrpcpb.KvPair, 0, reqLimit)
	it.Seek(reqSK)

	for i := uint32(0); i < reqLimit && it.Valid(); i++ {

		item := it.Item()
		val, _ := item.ValueCopy(nil)
		key := item.KeyCopy(nil)

		pairs = append(pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		})

		it.Next()
	}

	resp.Kvs = pairs
	return resp, nil
}
