// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package rpcbench

import (
	"io"
	"net/rpc"
	"reflect"
	"sync"

	"git.apache.org/thrift.git/lib/go/thrift"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// Context ...
type Context struct {
}

type method struct {
	handler func(Messager, func(Messager, error))
	reqType reflect.Type
	public  bool
}

type serverResponse struct {
	req   rpc.Request
	reply Messager
	err   error
}

type syncAdapter func(Messager) (Messager, error)

func (s syncAdapter) exec(args Messager, callback func(Messager, error)) {
	go func() {
		callback(s(args))
	}()
}

// Server is a Cockroach-specific RPC server. By default it handles a simple
// heartbeat protocol to measure link health. It also supports close callbacks.
//
// TODO(spencer): heartbeat protocol should also measure link latency.
type ThriftServer struct {
	insecure bool
	quit     chan struct{}

	serverTransport thrift.TServerTransport
	protocolFactory thrift.TProtocolFactory
	// decorate the accepted connection
	transportFactory thrift.TTransportFactory

	mu             sync.RWMutex
	activeConns    map[thrift.TTransport]struct{}
	openCallbacks  []func(conn thrift.TTransport)
	closeCallbacks []func(conn thrift.TTransport)
	methods        map[string]method
}

// NewThriftServer creates a new instance of ThriftServer.
func NewThriftServer(context *Context, t thrift.TServerTransport, tf thrift.TTransportFactory, pf thrift.TProtocolFactory) *ThriftServer {
	s := &ThriftServer{
		insecure:         false,
		activeConns:      make(map[thrift.TTransport]struct{}),
		methods:          map[string]method{},
		serverTransport:  t,
		transportFactory: tf,
		protocolFactory:  pf,
		quit:             make(chan struct{}, 1),
	}
	// heartbeat := &HeartbeatService{
	// }
	// if err := heartbeat.Register(s); err != nil {
	// 	log.Fatalf("unable to register heartbeat service with RPC server: %s", err)
	// }
	return s
}

// Register a new method handler. `name` is a qualified name of the
// form "Service.Name". `handler` is a function that takes an
// argument of the same type as `reqPrototype`. Both the argument and
// return value of 'handler' should be a pointer to a protocol message
// type. The handler function will be executed in a new goroutine.
// Only the "node" system user is allowed to use these endpoints.
func (s *ThriftServer) Register(name string,
	handler func(Messager) (Messager, error),
	reqPrototype Messager) error {
	return s.RegisterAsync(name, false /*not public*/, syncAdapter(handler).exec, reqPrototype)
}

// RegisterPublic is similar to Register, but allows non-system users.
func (s *ThriftServer) RegisterPublic(name string,
	handler func(Messager) (Messager, error),
	reqPrototype Messager) error {
	return s.RegisterAsync(name, true /*public*/, syncAdapter(handler).exec, reqPrototype)
}

// RegisterAsync registers an asynchronous method handler. Instead of
// returning a (Messager, error) tuple, an asynchronous handler
// receives a callback which it must execute when it is complete. Note
// that async handlers are started in the RPC server's goroutine and
// must not block (i.e. they must start a goroutine or write to a
// channel promptly). However, the fact that they are started in the
// RPC server's goroutine guarantees that the order of requests as
// they were read from the connection is preserved.
// If 'public' is true, all users may call this method, otherwise
// "node" users only.
func (s *ThriftServer) RegisterAsync(name string, public bool,
	handler func(Messager, func(Messager, error)),
	reqPrototype Messager) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.methods[name]; ok {
		return util.Errorf("method %s already registered", name)
	}
	reqType := reflect.TypeOf(reqPrototype)
	if reqType.Kind() != reflect.Ptr {
		// net/rpc supports non-pointer requests, but we always use pointers
		// and things are a little simpler this way.
		return util.Errorf("request type not a pointer")
	}
	s.methods[name] = method{
		handler: handler,
		reqType: reqType,
		public:  public,
	}
	return nil
}

// AddOpenCallback adds a callback to the openCallbacks slice to
// be invoked when a connection is opened.
func (s *ThriftServer) AddOpenCallback(cb func(conn thrift.TTransport)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.openCallbacks = append(s.openCallbacks, cb)
}

func (s *ThriftServer) runOpenCallbacks(conn thrift.TTransport) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, cb := range s.openCallbacks {
		cb(conn)
	}
}

// AddCloseCallback adds a callback to the closeCallbacks slice to
// be invoked when a connection is closed.
func (s *ThriftServer) AddCloseCallback(cb func(conn thrift.TTransport)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCallbacks = append(s.closeCallbacks, cb)
}

func (s *ThriftServer) runCloseCallbacks(conn thrift.TTransport) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, cb := range s.closeCallbacks {
		cb(conn)
	}
}

func (s *ThriftServer) Listen() error {
	return s.serverTransport.Listen()
}

func (s *ThriftServer) AcceptLoop() error {
	for {
		client, err := s.serverTransport.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return nil
			default:
			}
			return err
		}
		if client != nil {
			client = s.transportFactory.GetTransport(client)
			go s.serveThrift(client)
		}
	}
}

func (s *ThriftServer) Serve() error {
	err := s.Listen()
	if err != nil {
		return err
	}
	s.AcceptLoop()
	return nil
}

func (s *ThriftServer) Stop() error {
	s.quit <- struct{}{}
	s.serverTransport.Interrupt()
	return nil
}

// ServeHTTP implements an http.Handler that answers RPC requests.
func (s *ThriftServer) serveThrift(conn thrift.TTransport) {
	// Run open callbacks.
	s.runOpenCallbacks(conn)

	codec := NewThriftServerCodec(conn, s.protocolFactory)
	responses := make(chan serverResponse)
	go func() {
		s.sendResponses(codec, responses)
	}()
	go func() {
		s.readRequests(conn, codec, nil, responses)
		codec.Close()
		conn.Close()
	}()
}

// readRequests synchronously reads a stream of requests from a
// connection. Each request is handled in a new background goroutine;
// when the handler finishes the response is written to the responses
// channel. When the connection is closed (and any pending requests
// have finished), we close the responses channel.
func (s *ThriftServer) readRequests(conn thrift.TTransport, codec rpc.ServerCodec, authHook func(Messager, bool) error, responses chan<- serverResponse) {
	var wg sync.WaitGroup
	var closed bool
	defer func() {
		wg.Wait()
		if !closed {
			s.runCloseCallbacks(conn)
		}
		close(responses)
	}()

	for {
		req, meth, args, err := s.readRequest(codec)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF || util.IsClosedConnection(err) {
				closed = true
				s.runCloseCallbacks(conn)
				return
			}
			//			log.Warningf("rpc: server cannot decode request: %s", err)
			return
		}

		if meth.handler == nil {
			responses <- serverResponse{
				req: req,
				err: util.Errorf("rpc: couldn't find method: %s", req.ServiceMethod),
			}
			continue
		}

		if authHook != nil {
			if err := authHook(args, meth.public); err != nil {
				responses <- serverResponse{
					req: req,
					err: err,
				}
				// We got an unauthorized request. For now, leave the connection
				// open. We may want to close it in the future because security.
				continue
			}
		}

		wg.Add(1)
		meth.handler(args, func(reply Messager, err error) {
			responses <- serverResponse{
				req:   req,
				reply: reply,
				err:   err,
			}
			wg.Done()
		})
	}
}

// readRequest reads a single request from a connection.
func (s *ThriftServer) readRequest(codec rpc.ServerCodec) (rpc.Request, method, Messager, error) {
	var req rpc.Request
	if err := codec.ReadRequestHeader(&req); err != nil {
		//		log.Warningf("rpc: read requestHeader failed: %s", err)
		return req, method{}, nil, err
	}

	s.mu.RLock()
	m, ok := s.methods[req.ServiceMethod]
	s.mu.RUnlock()

	// If we found the method, construct a request protobuf, parse into
	// it, and authenticate it.
	if ok {
		args := reflect.New(m.reqType.Elem()).Interface().(Messager)

		return req, m, args, codec.ReadRequestBody(args)
	}

	// If not, consume and discard the input by passing nil to ReadRequestBody.
	return req, m, nil, codec.ReadRequestBody(nil)
}

// sendResponses sends a stream of responses on a connection, and
// exits when the channel is closed.
func (s *ThriftServer) sendResponses(codec rpc.ServerCodec, responses <-chan serverResponse) {
	for resp := range responses {
		rpcResp := rpc.Response{
			ServiceMethod: resp.req.ServiceMethod,
			Seq:           resp.req.Seq,
		}
		if resp.err != nil {
			rpcResp.Error = resp.err.Error()
		}
		if err := codec.WriteResponse(&rpcResp, resp.reply); err != nil {
			log.Warningf("rpc: write response failed: %s", err)
			// TODO(bdarnell): what to do at this point? close the connection?
			// net/rpc just swallows the error.
		}
	}
}
