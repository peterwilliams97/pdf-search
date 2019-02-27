Programs for Searching PDF Files.
================================

Uses UniDoc for PDF parsing and Bleve for full text search.

Installation (UniDoc)
---------------------
	cd $GOPATH/src/github.com
	mkdir -p github.com/peterwilliams97
	cd github.com/peterwilliams97
	git clone https://github.com/peterwilliams97/unidoc
	git checkout v3.imagemark

Installation (Bleve)
--------------------
	go get github.com/blevesearch/bleve/...
	go get github.com/blevesearch/snowballstem
	go get github.com/kljensen/snowball
	go get github.com/willf/bitset
	go get github.com/couchbase/moss
	go get github.com/syndtr/goleveldb/leveldb
	go get github.com/rcrowley/go-metrics

Programs
========
Basic search
------------
	simple_index.go        Index some PDFs
	simple_search.go       Full text search the PDFs indexed by `simple_index.go`

e.g.

	go run simple_index.go ~/testdata/adobe/PDF32000_2008.pdf
	go run simple_search.go Adobe PDF

gives

searchResults=755 matches, showing 1 through 10, took 5.396772ms
1. 1faa31928e.284 (0.414407)
  Contents
    <bold>PDF</bold> 32000-1:2008
Table 119 –  Character collections for predefined CMaps, by <bold>PDF</bold> version  (continued)
CMAP <bold>PDF</bold> 1.2 <bold>PDF</bold> 1.3 <bold>PDF</bold> 1.4 <bold>PDF</bold> 1.5
GBK2K-H/V — — <bold>Adobe</bold>-GB1-4 <bold>Adobe</bold>-GB1-4
UniGB-UCS2-H/V — <bold>Adobe</bold>-…
2. 1faa31928e.6 (0.322457)
  Contents
    <bold>PDF</bold> 32000-1:2008
    Foreword
On January 29, 2007, <bold>Adobe</bold> Systems Incorporated announced it’s intention to release the full Portable
Document Format (<bold>PDF</bold>) 1.7 specification to the American National Standa…
...

or

	go run simple_index.go ~/testdata/adobe/*.pdf
	go run simple_search.go Type1
