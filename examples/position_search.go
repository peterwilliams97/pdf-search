package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/registry"
	"github.com/peterwilliams97/pdf-search/serial"
	"github.com/peterwilliams97/pdf-search/utils"
	"github.com/unidoc/unidoc/common"
)

const usage = `Usage: go run location_search.go [OPTIONS] Adobe PDF
Performs a full text search for "Adobe PDF" in Bleve index "store.location" that was created with
simple_index.go`

var basePath = "store.position"

func main() {
	flag.StringVar(&basePath, "s", basePath, "Bleve store name. This is a directory.")
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
	indexPath := filepath.Join(basePath, "bleve")

	term := strings.Join(flag.Args(), " ")
	fmt.Printf("term=%q\n", term)
	fmt.Printf("indexPath=%q\n", indexPath)

	lState, err := utils.OpenPositionsState(basePath, false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not open positions store %q. err=%v\n", basePath, err)
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

	extractions := utils.CreateExtractList(10)
	for i, hit := range searchResults.Hits {
		// if i > 10 {
		// 	break
		// }
		id := hit.ID
		text := hit.Fields["Text"].(string)
		locations := hit.Locations
		contents := locations["Text"]

		docIdx, pageIdx, err := decodeID(id)
		if err != nil {
			panic(err)
		}
		inPath, pageNum, dpl, err := lState.ReadDocPagePositions(docIdx, pageIdx)
		if err != nil {
			panic(err)
		}

		positions := dpl.Locations

		fmt.Printf("--->>> %2d: pageNum=%d id=%q hit=%s Locations=%d text=%d  positions=%d\n",
			i, pageNum, id, hit,
			len(locations), len(text), len(positions))

		// for j, pos := range positions {
		// 	fmt.Printf("%6d: %v\n", j, pos)
		// }

		k := 0
		for term, termLocations := range contents {
			fmt.Printf("%6d: term=%q matches=%d\n", k, term, len(termLocations))
			k++
			for j, loc := range termLocations {
				l := *loc
				snip := text[l.Start:l.End]
				pos := getPosition(positions, uint32(l.Start))
				fmt.Printf("%9d: %d [%d:%d] %q %v\n", j, l.Pos, l.Start, l.End, snip, pos)
				extractions.AddRect(inPath, int(pageNum),
					float64(pos.Llx), float64(pos.Lly), float64(pos.Urx), float64(pos.Ury))
			}
		}
	}
	if err := extractions.SaveOutputPdf("XXXXX.pdf"); err != nil {
		panic(err)
	}
	fmt.Println("=================@@@=====================")
	fmt.Printf("term=%q\n", term)
	fmt.Printf("indexPath=%q\n", indexPath)
}

func getPosition(positions []serial.TextLocation, offset uint32) serial.TextLocation {
	i := sort.Search(len(positions), func(i int) bool { return positions[i].Offset >= offset })
	if !(0 <= i && i < len(positions)) {
		common.Log.Error("getPosition: offset=%d i=%d len=%d %v==%v", offset, i, len(positions),
			positions[0], positions[len(positions)-1])
		return serial.TextLocation{}
	}
	return positions[i]
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
	pageIdx, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, 0, err
	}
	fmt.Printf("$$$ %+q -> %+q %d.%d\n", id, parts, docIdx, pageIdx)
	return uint64(docIdx), uint32(pageIdx), nil
}
