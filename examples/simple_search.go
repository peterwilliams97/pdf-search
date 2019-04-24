package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/registry"
	"github.com/peterwilliams97/pdf-search/doclib"
)

const usage = `Usage: go run simple_search.go [OPTIONS] Adobe PDF
Performs a full text search for "Adobe PDF" in Bleve index "store.simple" that was created with
simple_index.go`

var persistDir = "store.simple"

func main() {
	flag.StringVar(&persistDir, "s", persistDir, "Index store directory name.")
	doclib.MakeUsage(usage)
	flag.Parse()
	doclib.SetLogging()
	if len(flag.Args()) < 1 {
		flag.Usage()
		os.Exit(1)
	}
	indexPath := filepath.Join(persistDir, "bleve")

	term := strings.Join(flag.Args(), " ")
	fmt.Printf("term=%q\n", term)
	fmt.Printf("indexPath=%q\n", indexPath)

	// Open existing index.
	index, err := bleve.Open(indexPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not open Bleve index %q.\n", indexPath)
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
		text := hit.Fields["Contents"].(string)
		locations := hit.Locations
		contents := locations["Contents"]

		// fmt.Printf("%2d: %s\n\tLocations=%T\n\tcontents=%T\n\tencoding=%#v\n",
		// 	i, hit, hit.Locations, hit.Locations["Contents"], hit.Locations["Contents"]["encoding"])
		fmt.Printf("%2d: %s Hit=%T Locations=%d %T contents=%d %T  text=%d %T\n",
			i, hit, hit,
			len(locations), locations,
			len(contents), contents,
			len(text), text)

		k := 0
		for term, termLocations := range contents {
			fmt.Printf("%6d: term=%q matches=%d\n", k, term, len(termLocations))
			k++
			for j, loc := range termLocations {
				l := *loc
				snip := text[l.Start:l.End]
				fmt.Printf("%9d: %d [%d:%d] %q\n", j, l.Pos, l.Start, l.End, snip)
			}
		}
	}
	fmt.Println("=================@@@=====================")
	fmt.Printf("term=%q\n", term)
	fmt.Printf("indexPath=%q\n", indexPath)
}
