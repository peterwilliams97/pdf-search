package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/registry"
	"github.com/peterwilliams97/pdf-search/utils"
)

var store = "store.simple"

func main() {
	flag.StringVar(&store, "s", store, "Bleve store name. This is a directory.")
	utils.MakeUsage(`Usage: go run simple_search.go [OPTIONS] Adobe PDF
Performs a full text search for "Adobe PDF" in Bleve index "store.simple" that was created with
simple_index.go`)
	utils.SetLogging()
	flag.Parse()
	if utils.ShowHelp {
		flag.Usage()
		os.Exit(0)
	}
	if len(flag.Args()) < 1 {
		flag.Usage()
		os.Exit(1)
	}

	term := strings.Join(flag.Args(), " ")
	fmt.Printf("term=%q\n", term)

	// Open existing index.
	index, err := bleve.Open(store)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not open Bleve index %q.\n", store)
		panic(err)
	}

	query := bleve.NewMatchQuery(term)
	search := bleve.NewSearchRequest(query)
	types, _ := registry.HighlighterTypesAndInstances()
	fmt.Printf("Higlighters=%+v\n", types)
	// search.Highlight = bleve.NewHighlightWithStyle("html")
	search.Highlight = bleve.NewHighlight()
	search.Fields = []string{"Contents"}
	search.Highlight.Fields = search.Fields

	searchResults, err := index.Search(search)
	if err != nil {
		panic(err)
	}

	fmt.Println("=================!!!=====================")
	fmt.Printf("searchResults=%s\n", searchResults)
	fmt.Println("=================***=====================")
	if len(searchResults.Hits) == 0 {
		fmt.Println("No matches")
		os.Exit(0)
	}
	for i, hit := range searchResults.Hits {
		fmt.Printf("%2d: %s\n", i, hit)
	}
	fmt.Println("=================@@@=====================")
}
