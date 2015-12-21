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

type thriftServerCodec struct {
	transport       thrift.TTransport
	protocolFactory thrift.TProtocolFactory
	outputProtocol  thrift.TProtocol
	inputProtocol   thrift.TProtocol

	// temporary work space
	reqMethod string
	reqSeqID  int32
}

// NewThriftServerCodec returns a serverCodec that communicates with the ClientCodec
// on the other end of the given conn.
func NewThriftServerCodec(t thrift.TTransport, f thrift.TProtocolFactory) rpc.ServerCodec {
	codec := &thriftServerCodec{
		transport:       t,
		protocolFactory: f,
	}
	codec.outputProtocol = codec.protocolFactory.GetProtocol(codec.transport)
	codec.inputProtocol = codec.protocolFactory.GetProtocol(codec.transport)
	return codec
}

func (c *thriftServerCodec) ReadRequestHeader(r *rpc.Request) error {
	method, _, seqID, err := c.inputProtocol.ReadMessageBegin()
	if err != nil {
		//fmt.Printf("ReadRequestHeader err2 %s\n", err)
		return err
	}

	r.Seq = uint64(seqID)
	r.ServiceMethod = method
	// Temporarily hold
	c.reqSeqID = seqID
	c.reqMethod = method

	return nil
}

func (c *thriftServerCodec) ReadRequestBody(x interface{}) error {
	if x == nil {
		return nil
	}
	request, ok := x.(Reader)
	if !ok {
		return fmt.Errorf(
			"protorpc.ServerCodec.ReadRequestBody: %T does not implement Messager",
			x,
		)
	}

	if err := request.Read(c.inputProtocol); err != nil {
		c.inputProtocol.ReadMessageEnd()
		ex := thrift.NewTApplicationException(thrift.PROTOCOL_ERROR, err.Error())
		c.outputProtocol.WriteMessageBegin(c.reqMethod, thrift.EXCEPTION, c.reqSeqID)
		ex.Write(c.outputProtocol)
		c.outputProtocol.WriteMessageEnd()
		c.outputProtocol.Flush()
		fmt.Printf("ReadRequestBody err %s\n", err)
		return err
	}
	c.reqSeqID = 0
	if err := c.inputProtocol.ReadMessageEnd(); err != nil {
		fmt.Printf("ReadRequestBody err2 %s\n", err)
		return err
	}

	return nil
}

func (c *thriftServerCodec) WriteResponse(r *rpc.Response, x interface{}) error {
	var response Writer
	if x != nil {
		var ok bool
		if response, ok = x.(Writer); !ok {
			if _, ok = x.(struct{}); !ok {
				return fmt.Errorf(
					"protorpc.ServerCodec.WriteResponse: %T does not implement Writer",
					x,
				)
			}
		}
	}

	// clear response if error
	if r.Error != "" {
		response = nil
	}
	if err := c.outputProtocol.WriteMessageBegin(r.ServiceMethod, thrift.REPLY, int32(r.Seq)); err != nil {
		return err
	}
	if err := response.Write(c.outputProtocol); err != nil {
		return err
	}
	if err := c.outputProtocol.WriteMessageEnd(); err != nil {
		return err
	}
	return c.outputProtocol.Flush()
}

func (c *thriftServerCodec) Close() error {
	return c.transport.Close()
}
