// Copyright 2013 <chaishushan{AT}gmail.com>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

namespace go rpcbench

struct EchoRequest {
	1: string msg,
}

struct EchoResponse {
	1: string msg,
}

service Echo {
  EchoResponse Echo(1:EchoRequest e),
}

