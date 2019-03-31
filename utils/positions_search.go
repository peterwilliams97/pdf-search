package utils

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/registry"
	"github.com/blevesearch/bleve/search"
	"github.com/unidoc/unidoc/common"
)

func SearchPdfIndex(persistDir, term string, maxResults int) (string, error) {

	indexPath := filepath.Join(persistDir, "bleve")

	common.Log.Info("term=%q", term)
	common.Log.Info("maxResults=%d", maxResults)
	common.Log.Info("indexPath=%q", indexPath)

	// Open existing index.
	index, err := bleve.Open(indexPath)
	if err != nil {
		return "", fmt.Errorf("Could not open Bleve index %q", indexPath)
	}
	common.Log.Debug("index=%s", index)

	lState, err := OpenPositionsState(persistDir, false)
	if err != nil {
		return "", fmt.Errorf("Could not open positions store %q. err=%v", persistDir, err)
	}
	common.Log.Debug("lState=%s", *lState)

	results, err := SearchIndex(lState, index, term, maxResults)
	if err != nil {
		return "", fmt.Errorf("Could not find term=%q %q. err=%v", term, persistDir, err)
	}

	common.Log.Info("=================@@@=====================")
	common.Log.Info("term=%q", term)
	common.Log.Info("indexPath=%q", indexPath)
	return results, nil
}

func SearchIndex(lState *PositionsState, index bleve.Index, term string, maxResults int) (
	string, error) {

	common.Log.Debug("SearchIndex: term=%q maxResults=%d", term, maxResults)

	if lState.Len() == 0 {
		return "", fmt.Errorf("Empty positions store %s", lState)
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
		return "", err
	}

	common.Log.Info("=================!!!=====================")
	common.Log.Info("searchResults=%T", searchResults)

	if len(searchResults.Hits) == 0 {
		common.Log.Info("No matches")
		return "", nil
	}

	return getResults(lState, searchResults), nil
}

func getResults(lState *PositionsState, sr *bleve.SearchResult) string {
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

func getHit(lState *PositionsState, i int, hit *search.DocumentMatch) string {
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
