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
	"github.com/blevesearch/bleve/search"
	"github.com/peterwilliams97/pdf-search/utils"
	"github.com/unidoc/unidoc/common"
)

const usage = `Usage: go run position_search.go [OPTIONS] Adobe PDF
Performs a full text search for "Adobe PDF" in Bleve index "store.position" that was created with
simple_index.go`

var basePath = "store.position"

func main() {
	maxResults := 10
	flag.StringVar(&basePath, "s", basePath, "Bleve store name. This is a directory.")
	flag.IntVar(&maxResults, "n", maxResults, "Max number of results to return.")
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
	fmt.Printf("maxResults=%d\n", maxResults)
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
	search.Fields = []string{"Text"}
	search.Highlight.Fields = search.Fields
	search.Size = maxResults
	// search.Explain = true

	searchResults, err := index.Search(search)
	if err != nil {
		panic(err)
	}

	lState, err := utils.OpenPositionsState(basePath, false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not open positions store %q. err=%v\n", basePath, err)
		panic(err)
	}
	if lState.Len() == 0 {
		fmt.Fprintf(os.Stderr, "Empty positions store %s\n", lState)
		panic("Empty positions store")
	}

	fmt.Println("=================!!!=====================")
	fmt.Printf("searchResults=%T\n", searchResults)
	// fmt.Printf("searchResults=%s\n", searchResults)
	// // fmt.Println("=================---=====================")
	// // fmt.Printf("searchResults.Fields=%s\n", searchResults.Fields)
	// fmt.Println("=================***=====================")
	fmt.Printf("   getResults=%s\n", getResults(lState, searchResults))
	fmt.Println("=================+++=====================")
	if len(searchResults.Hits) == 0 {
		fmt.Println("No matches")
		os.Exit(0)
	}

	fmt.Println("=================@@@=====================")
	fmt.Printf("term=%q\n", term)
	fmt.Printf("indexPath=%q\n", indexPath)
}

func getResults(lState *utils.PositionsState, sr *bleve.SearchResult) string {
	rv := ""
	if sr.Total > 0 {
		if sr.Request.Size > 0 {
			rv = fmt.Sprintf("%d matches, showing %d through %d, took %s\n",
				sr.Total, sr.Request.From+1, sr.Request.From+len(sr.Hits), sr.Took)
			for i, hit := range sr.Hits {
				rv += "--------------------------------------------------\n"
				rv += fmt.Sprintf("%d: ", i+sr.Request.From+1)
				rv += getHit(lState, i, hit)
			}
		} else {
			rv = fmt.Sprintf("%d matches, took %s\n", sr.Total, sr.Took)
		}
	} else {
		rv = "No matches"
	}
	return rv
}

func getHit(lState *utils.PositionsState, i int, hit *search.DocumentMatch) string {
	m := getMatch(hit)
	inPath, pageNum, _, err := lState.ReadDocPagePositions(m.docIdx, m.pageIdx)
	if err != nil {
		panic(err)
	}
	text, err := lState.ReadDocPageText(m.docIdx, m.pageIdx)
	if err != nil {
		panic(err)
	}
	lineNumber, line, ok := getLineNumber(text, m.start)
	if !ok {
		panic("No line number")
	}
	rv := fmt.Sprintf("path=%q pageNum=%d line=%d (score=%.3f) match=%q\n"+
		"^^^^^^^^ Marked up Text ^^^^^^^^\n"+
		"%s\n",
		inPath, pageNum, lineNumber, m.score, line, m.fragment)

	return fmt.Sprintf("%d: %s -- %s", i, hit.ID, rv)
}

type match struct {
	docIdx   uint64
	pageIdx  uint32
	score    float64
	fragment string
	start    int
	end      int
}

func (m match) String() string {
	return fmt.Sprintf("docIdx=%d pageIdx=%d (score=%.3f)\n%s", m.docIdx, m.pageIdx, m.score, m.fragment)
}

func getMatch(hit *search.DocumentMatch) match {

	docIdx, pageIdx, err := decodeID(hit.ID)
	if err != nil {
		panic(err)
	}

	start, end := -1, -1
	frags := ""
	common.Log.Debug("------------------------")
	for k, fragments := range hit.Fragments {
		for _, fragment := range fragments {
			frags += fragment
		}
		loc := hit.Locations[k]
		common.Log.Debug("%q: %v", k, frags)
		for kk, v := range loc {
			for i, l := range v {
				common.Log.Debug("\t%q: %d: %#v", kk, i, l)
				if start < 0 {
					start = int(l.Start)
					end = int(l.End)
				}
			}
		}
	}
	return match{
		docIdx:   docIdx,
		pageIdx:  pageIdx,
		score:    hit.Score,
		fragment: frags,
		start:    start,
		end:      end,
	}
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
	// fmt.Printf("$$$ %+q -> %+q %d.%d\n", id, parts, docIdx, pageIdx)
	return uint64(docIdx), uint32(pageIdx), nil
}

func getLineNumber(text string, offset int) (int, string, bool) {
	endings := lineEndings(text)
	n := len(endings)
	i := sort.Search(len(endings), func(i int) bool { return endings[i] > offset })
	ok := 0 <= i && i < n
	if !ok {
		common.Log.Error("getLineNumber: offset=%d text=%d i=%d endings=%d %+v\n%s",
			offset, len(text), i, n, endings, text)
	}
	common.Log.Debug("offset=%d i=%d endings=%+v", offset, i, endings)
	ofs0 := endings[i-1]
	ofs1 := endings[i+0]
	line := text[ofs0:ofs1]
	runes := []rune(line)
	if len(runes) >= 1 && runes[0] == '\n' {
		line = string(runes[1:])
	}
	return i, line, ok
}

func lineEndings(text string) []int {
	if len(text) == 0 || (len(text) > 0 && text[len(text)-1] != '\n') {
		text += "\n"
	}
	endings := []int{0}
	for ofs := 0; ofs < len(text); {
		o := strings.Index(text[ofs:], "\n")
		if o < 0 {
			break
		}
		endings = append(endings, ofs+o)
		ofs = ofs + o + 1
	}
	// fmt.Println("==================================")
	// fmt.Printf("%s\n", text)
	// common.Log.Info("++++ text=%d endings=%d %+v", len(text), len(endings), endings)

	return endings
}
