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

		searchResults=1518 matches, showing 1 through 10, took 8.704385ms
	    1. 1faa31928e.284 (0.545815)
		Contents
			<mark>PDF</mark> 32000-1:2008
	Table 119 –  Character collections for predefined CMaps, by <mark>PDF</mark> version  (continued)
	CMAP <mark>PDF</mark> 1.2 <mark>PDF</mark> 1.3 <mark>PDF</mark> 1.4 <mark>PDF</mark> 1.5
	GBK2K-H/V — — <mark>Adobe</mark>-GB1-4 <mark>Adobe</mark>-GB1-4
	UniGB-UCS2-H/V — <mark>Adobe</mark>-…
		Name
			PDF32000_2008.<mark>pdf</mark>
	    2. 50d0bbc960.5 (0.482355)
		Contents
			…00 is published will
	reference specifications in ISO 32000.
	The extensions are to the <mark>PDF</mark> document format. <mark>Adobe</mark> plans to submit these extensions to ISO as
	candidates for inclusion into the next ver…
		Name
			adobe_supplement_iso32000.<mark>pdf</mark>

or

	go run simple_index.go ~/testdata/adobe/*.pdf
	go run simple_search.go Type1
