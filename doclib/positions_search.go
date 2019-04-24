package doclib

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/registry"
	"github.com/blevesearch/bleve/search"
	"github.com/peterwilliams97/pdf-search/serial"
	"github.com/unidoc/unidoc/common"
)

type PdfMatchSet struct {
	TotalMatches   int
	SearchDuration time.Duration
	Matches        []PdfMatch
}

// PdfMatch describes a single search match in a PDF document.
// It is the analog of a bleve search.DocumentMatch
type PdfMatch struct {
	InPath  string
	PageNum uint32
	LineNum int
	Line    string
	serial.DocPageLocations
	match
}

type match struct {
	docIdx   uint64
	pageIdx  uint32
	Score    float64
	Fragment string
	Start    uint32
	End      uint32
}

func SearchPdfIndex(persistDir, term string, maxResults int) (PdfMatchSet, error) {
	p := PdfMatchSet{}

	indexPath := filepath.Join(persistDir, "bleve")

	common.Log.Debug("term=%q", term)
	common.Log.Debug("maxResults=%d", maxResults)
	common.Log.Debug("indexPath=%q", indexPath)

	// Open existing index.
	index, err := bleve.Open(indexPath)
	if err != nil {
		return p, fmt.Errorf("Could not open Bleve index %q", indexPath)
	}
	common.Log.Debug("index=%s", index)

	lState, err := OpenPositionsState(persistDir, false)
	if err != nil {
		return p, fmt.Errorf("Could not open positions store %q. err=%v", persistDir, err)
	}
	common.Log.Debug("lState=%s", *lState)

	results, err := SearchIndex(lState, index, term, maxResults)
	if err != nil {
		return p, fmt.Errorf("Could not find term=%q %q. err=%v", term, persistDir, err)
	}

	common.Log.Debug("=================@@@=====================")
	common.Log.Debug("term=%q", term)
	common.Log.Debug("indexPath=%q", indexPath)
	return results, nil
}

func SearchIndex(lState *PositionsState, index bleve.Index, term string, maxResults int) (
	PdfMatchSet, error) {
	p := PdfMatchSet{}

	common.Log.Debug("SearchIndex: term=%q maxResults=%d", term, maxResults)

	if lState.Len() == 0 {
		return p, fmt.Errorf("Empty positions store %s", lState)
	}

	query := bleve.NewMatchQuery(term)
	search := bleve.NewSearchRequest(query)
	types, _ := registry.HighlighterTypesAndInstances()
	common.Log.Debug("Higlighters=%+v", types)
	search.Highlight = bleve.NewHighlight()
	search.Fields = []string{"Text"}
	search.Highlight.Fields = search.Fields
	search.Size = maxResults

	searchResults, err := index.Search(search)
	if err != nil {
		return p, err
	}

	common.Log.Debug("=================!!!=====================")
	common.Log.Debug("searchResults=%T", searchResults)

	if len(searchResults.Hits) == 0 {
		common.Log.Info("No matches")
		return p, nil
	}

	return lState.getPdfMatches(searchResults)
}

func (lState *PositionsState) getResults(sr *bleve.SearchResult) (string, error) {
	matchSet, err := lState.getPdfMatches(sr)
	if err != nil {
		return "", err
	}
	return matchSet.String(), nil
}

func (lState *PositionsState) getPdfMatches(sr *bleve.SearchResult) (PdfMatchSet, error) {
	var matches []PdfMatch
	if sr.Total > 0 && sr.Request.Size > 0 {
		for _, hit := range sr.Hits {
			m, err := lState.getPdfMatch(hit)
			if err != nil {
				if err == ErrNoMatch {
					continue
				}
				return PdfMatchSet{}, err
			}
			matches = append(matches, m)
		}
	}

	return PdfMatchSet{
		TotalMatches:   int(sr.Total),
		SearchDuration: sr.Took,
		Matches:        matches,
	}, nil
}

func (lState *PositionsState) getHit(i int, hit *search.DocumentMatch) (string, error) {
	p, err := lState.getPdfMatch(hit)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d: %s -- %s", i, hit.ID, p), nil
}

func (s PdfMatchSet) String() string {
	if s.TotalMatches <= 0 {
		return "No matches"
	}
	if len(s.Matches) == 0 {
		return fmt.Sprintf("%d matches, SearchDuration %s\n", s.TotalMatches, s.SearchDuration)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d matches, showing %d, SearchDuration %s\n",
		s.TotalMatches, len(s.Matches), s.SearchDuration)
	for i, m := range s.Matches {
		fmt.Fprintln(&b, "--------------------------------------------------")
		fmt.Fprintf(&b, "%d: %s\n", i+1, m)
	}
	return b.String()
}

// Filter returns a filtered list of results is `s` as a PdfMatchSet.
func (s PdfMatchSet) Filter(maxResultsPerFile int) PdfMatchSet {
	fileCounts := map[string]int{}
	var matches []PdfMatch
	for _, m := range s.Matches {
		fileCounts[m.InPath]++
		if fileCounts[m.InPath] <= maxResultsPerFile {
			matches = append(matches, m)
		}
	}
	return PdfMatchSet{
		TotalMatches:   s.TotalMatches,
		SearchDuration: s.SearchDuration, // !@#$ IndexDuration
		Matches:        matches,
	}
}

// Files returns the unique file names in `s`.
func (s PdfMatchSet) Files() []string {
	fileSet := map[string]bool{}
	var files []string
	for _, m := range s.Matches {
		if _, ok := fileSet[m.InPath]; ok {
			continue
		}
		files = append(files, m.InPath)
		fileSet[m.InPath] = true
	}
	return files
}

func (p PdfMatch) String() string {
	return fmt.Sprintf("path=%q pageNum=%d line=%d (score=%.3f) match=%q\n"+
		"^^^^^^^^ Marked up Text ^^^^^^^^\n"+
		"%s",
		p.InPath, p.PageNum, p.LineNum, p.Score, p.Line, p.Fragment)
}

// getPdfMatch returns the PdfMatch corresponding the bleve DocumentMatch `hit`.
// The returned PdfMatch contains information that is not in `hit` that is looked up in `lState`.
// We purposely try to keep `hit` small to improve bleve indexing performance and to reduce the
// index size.
func (lState *PositionsState) getPdfMatch(hit *search.DocumentMatch) (PdfMatch, error) {
	m, err := getMatch(hit)
	if err != nil {
		return PdfMatch{}, err
	}
	inPath, pageNum, dpl, err := lState.ReadDocPagePositions(m.docIdx, m.pageIdx)
	if err != nil {
		return PdfMatch{}, err
	}
	common.Log.Error("dpl=%#v", dpl)
	text, err := lState.ReadDocPageText(m.docIdx, m.pageIdx)
	if err != nil {
		return PdfMatch{}, err
	}
	lineNum, line, ok := getLineNumber(text, m.Start)
	if !ok {
		return PdfMatch{}, fmt.Errorf("No line number. m=%s", m)
	}
	return PdfMatch{
		InPath:           inPath,
		PageNum:          pageNum,
		LineNum:          lineNum,
		Line:             line,
		DocPageLocations: dpl,
		match:            m,
	}, nil
}

func (m match) String() string {
	return fmt.Sprintf("docIdx=%d pageIdx=%d (score=%.3f)\n%s",
		m.docIdx, m.pageIdx, m.Score, m.Fragment)
}

var ErrNoMatch = errors.New("no match for hit")

func getMatch(hit *search.DocumentMatch) (match, error) {

	docIdx, pageIdx, err := decodeID(hit.ID)
	if err != nil {
		return match{}, err
	}

	start, end := -1, -1
	frags := ""
	common.Log.Debug("------------------------")
	for k, fragments := range hit.Fragments {
		for _, fragment := range fragments {
			frags += fragment
		}
		loc := hit.Locations[k]
		common.Log.Info("%q: %v", k, frags)
		for kk, v := range loc {
			for i, l := range v {
				common.Log.Info("\t%q: %d: %#v", kk, i, l)
				if start < 0 {
					start = int(l.Start)
					end = int(l.End)
				}
			}
		}
	}
	if start < 0 {
		common.Log.Error("Fragments=%d", len(hit.Fragments))
		for k := range hit.Fragments {
			loc := hit.Locations[k]
			common.Log.Error("%q: %v", k, frags)
			for kk, v := range loc {
				for i, l := range v {
					common.Log.Error("\t%q: %d: %#v", kk, i, l)
				}
			}
		}
		err := ErrNoMatch
		common.Log.Error("hit=%s err=%v", hit, err)
		return match{}, err
	}
	return match{
		docIdx:   docIdx,
		pageIdx:  pageIdx,
		Score:    hit.Score,
		Fragment: frags,
		Start:    uint32(start),
		End:      uint32(end),
	}, nil
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

func getLineNumber(text string, offset uint32) (int, string, bool) {
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

func lineEndings(text string) []uint32 {
	if len(text) == 0 || (len(text) > 0 && text[len(text)-1] != '\n') {
		text += "\n"
	}
	endings := []uint32{0}
	for ofs := 0; ofs < len(text); {
		o := strings.Index(text[ofs:], "\n")
		if o < 0 {
			break
		}
		endings = append(endings, uint32(ofs+o))
		ofs = ofs + o + 1
	}
	// fmt.Println("==================================")
	// fmt.Printf("%s\n", text)
	// common.Log.Info("++++ text=%d endings=%d %+v", len(text), len(endings), endings)

	return endings
}

func GetPosition(positions []serial.TextLocation, start, end uint32) serial.TextLocation {
	i0, ok0 := getPositionIndex(positions, end)
	i1, ok1 := getPositionIndex(positions, start)
	if !(ok0 && ok1) {
		return serial.TextLocation{}
	}
	p0, p1 := positions[i0], positions[i1]
	return serial.TextLocation{
		Start: start,
		End:   end,
		Llx:   min(p0.Llx, p1.Llx),
		Lly:   min(p0.Lly, p1.Lly),
		Urx:   max(p0.Urx, p1.Urx),
		Ury:   max(p0.Ury, p1.Ury),
	}
}

func getPositionIndex(positions []serial.TextLocation, offset uint32) (int, bool) {
	i := sort.Search(len(positions), func(i int) bool { return positions[i].Start >= offset })
	ok := 0 <= i && i < len(positions)
	if !ok {
		common.Log.Error("getPositionIndex: offset=%d i=%d len=%d %v==%v", offset, i, len(positions),
			positions[0], positions[len(positions)-1])
	}
	return i, ok
}

func min(x, y float32) float32 {
	if x < y {
		return x
	}
	return y
}

func max(x, y float32) float32 {
	if x > y {
		return x
	}
	return y
}
