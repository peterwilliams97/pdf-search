package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/registry"
	"github.com/peterwilliams97/pdf-search/utils"
)

const usage = `Usage: go run location_search.go [OPTIONS] Adobe PDF
Performs a full text search for "Adobe PDF" in Bleve index "store.location" that was created with
simple_index.go`

var basePath = "store.xxx"

func main() {
	flag.StringVar(&basePath, "s", basePath, "Bleve store name. This is a directory.")
	indexPath := filepath.Join(basePath, "bleve")
	utils.MakeUsage(usage)
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

	lState, err := utils.OpenPositionsState(basePath, false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not open positions file %q. err=%v\n", basePath, err)
		panic(err)
	}

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
	search.Fields = []string{"Text"}
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
		id := hit.Fields["ID"].(string)
		text := hit.Fields["Text"].(string)
		locations := hit.Locations
		contents := locations["Text"]

		docIdx, pageIdx, err := decodeID(id)
		if err != nil {
			panic(err)
		}
		dpl, err := lState.ReadDocPagePositions(docIdx, pageIdx)
		if err != nil {
			panic(err)
		}

		positions := dpl.Locations
		positions = positions

		fmt.Printf("%2d: %s Hit=%T Locations=%d %T text=%d %T\n",
			i, hit, hit,
			len(locations), locations,
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

// id := fmt.Sprintf("%04X.%d", l.DocIdx, l.PageIdx)
func decodeID(id string) (uint64, uint32, error) {
	parts := strings.Split(id, ".")
	if len(parts) != 2 {
		return 0, 0, errors.New("bad format")
	}
	docIdx, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return 0, 0, err
	}
	pageIdx, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, 0, err
	}
	return uint64(docIdx), uint32(pageIdx), nil
}
