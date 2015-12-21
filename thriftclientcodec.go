// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// Copyright 2013 <chaishushan{AT}gmail.com>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpcbench

import (
	"fmt"
	"net/rpc"

	"git.apache.org/thrift.git/lib/go/thrift"
)

// Messager ...
type Messager interface {
	String() string
}

// Writer ...
type Writer interface {
	Write(thrift.TProtocol) error
}

// Reader ...
type Reader interface {
	Read(thrift.TProtocol) error
}

type thriftClientCodec struct {
	transport       thrift.TTransport
	protocolFactory thrift.TProtocolFactory
	outputProtocol  thrift.TProtocol
	inputProtocol   thrift.TProtocol
}

// NewThriftClientCodec returns a new rpc.ClientCodec using Thrift-RPC on conn.
func NewThriftClientCodec(t thrift.TTransport, f thrift.TProtocolFactory) rpc.ClientCodec {
	c := &thriftClientCodec{
		transport:       t,
		protocolFactory: f,
	}
	c.outputProtocol = c.protocolFactory.GetProtocol(c.transport)
	c.inputProtocol = c.protocolFactory.GetProtocol(c.transport)
	return c
}

func (c *thriftClientCodec) WriteRequest(r *rpc.Request, param interface{}) error {
	var request Writer
	if param != nil {
		var ok bool
		if request, ok = param.(Writer); !ok {
			return fmt.Errorf(
				"protorpc.ClientCodec.WriteRequest: %T does not implement proto.Message",
				param,
			)
		}
	}

	oprot := c.outputProtocol
	if err := oprot.WriteMessageBegin(r.ServiceMethod, thrift.CALL, int32(r.Seq)); err != nil {
		fmt.Printf("WriteReqest begin error %s\n", err)
		return err
	}
	if err := request.Write(oprot); err != nil {
		fmt.Printf("WriteReqest error %s\n", err)
		return err
	}
	if err := oprot.WriteMessageEnd(); err != nil {
		fmt.Printf("WriteReqest end error %s\n", err)
		return err
	}
	return oprot.Flush()
}

func (c *thriftClientCodec) ReadResponseHeader(r *rpc.Response) error {
	iprot := c.inputProtocol
	method, mTypeID, seqID, err := iprot.ReadMessageBegin()
	if err != nil {
		//	fmt.Printf("ReadResponseHeader begin error %s\n", err)
		return err
	}
	if mTypeID == thrift.EXCEPTION {
		fmt.Printf("ReadResponseHeader server return exception\n")
		error0 := thrift.NewTApplicationException(thrift.UNKNOWN_APPLICATION_EXCEPTION, "Unknown Exception")
		var error1 error
		error1, err = error0.Read(iprot)
		if err != nil {
			return err
		}
		if err = iprot.ReadMessageEnd(); err != nil {
			return err
		}
		err = error1
		return err
	}
	if mTypeID != thrift.REPLY {
		err = thrift.NewTApplicationException(thrift.INVALID_MESSAGE_TYPE_EXCEPTION, "Echo failed: invalid message type")
		fmt.Printf("ReadResponseHeader not reply %s\n", err)
		return err
	}

	r.Seq = uint64(seqID)
	r.ServiceMethod = method
	return nil
}

func (c *thriftClientCodec) ReadResponseBody(x interface{}) error {
	var response Reader
	if x != nil {
		var ok bool
		response, ok = x.(Reader)
		if !ok {
			return fmt.Errorf(
				"protorpc.ClientCodec.ReadResponseBody: %T does not implement proto.Message",
				x,
			)
		}
	}

	if err := response.Read(c.inputProtocol); err != nil {
		fmt.Printf("ReadResponseBody begin error %s\n", err)
		return err
	}
	if err := c.inputProtocol.ReadMessageEnd(); err != nil {
		fmt.Printf("ReadResponseBody end error %s\n", err)
		return err
	}

	return nil
}

func (c *thriftClientCodec) Close() error {
	return c.transport.Close()
}
