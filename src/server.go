package geerpc

import gee-rpc/codec

const MagicNumber = 0x3bef5c

type Option struct {
	MagicNumber int
	CodecType codec.Type
}