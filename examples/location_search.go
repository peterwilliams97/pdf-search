package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/registry"
	"github.com/peterwilliams97/pdf-search/utils"
)

var basePath = "store.xxx"

func main() {
	flag.StringVar(&basePath, "s", basePath, "Bleve store name. This is a directory.")
	indexPath := filepath.Join(basePath, "bleve")
	// locationsPath := filepath.Join(basePath, "locations")
	// hashPath := filepath.Join(basePath, "file_hash.json")
	utils.MakeUsage(`Usage: go run location_search.go [OPTIONS] Adobe PDF
Performs a full text search for "Adobe PDF" in Bleve index "store.location" that was created with
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
	index, err := bleve.Open(indexPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not open Bleve index %q.\n", indexPath)
		panic(err)
	}
	b, err := ioutil.ReadAll(locationsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not open locations %q.\n", locationsPath)
		panic(err)
	}

	hs, err := utils.OpenLocationsState(basePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not oopen hash file %q. err=%v\n", basePath, err)
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
}
