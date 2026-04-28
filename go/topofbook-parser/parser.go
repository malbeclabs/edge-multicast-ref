package main

import "github.com/malbeclabs/edge-multicast-ref/go/topofbook-parser/tob"

type Record = tob.Record
type PacketMeta = tob.PacketMeta
type Parser = tob.Parser

// ParserFactory creates a new Parser instance.
type ParserFactory func() Parser

var parserRegistry = map[string]ParserFactory{
	"topofbook": func() Parser { return tob.NewTopOfBookParser() },
}

// NewParser creates a parser by name from the registry.
func NewParser(name string) (Parser, bool) {
	factory, ok := parserRegistry[name]
	if !ok {
		return nil, false
	}
	return factory(), true
}

// RegisteredParsers returns the names of all registered parsers.
func RegisteredParsers() []string {
	names := make([]string, 0, len(parserRegistry))
	for name := range parserRegistry {
		names = append(names, name)
	}
	return names
}
