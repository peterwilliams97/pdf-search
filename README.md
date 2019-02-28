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

<strong>searchResults=755 matches, showing 1 through 10, took 5.396772ms</strong>

<strong>1. 1faa31928e.284 (0.414407) </strong>
<code>
<strong><em>PDF</em></strong> 32000-1:2008
Table 119 –  Character collections for predefined CMaps, by <strong><em>PDF</em></strong> version  (continued)
CMAP <strong><em>PDF</em></strong> 1.2 <strong><em>PDF</em></strong> 1.3 <strong><em>PDF</em></strong> 1.4 <strong><em>PDF</em></strong> 1.5
GBK2K-H/V — — <strong><em>Adobe</em></strong>-GB1-4 <strong><em>Adobe</em></strong>-GB1-4
UniGB-UCS2-H/V — <strong><em>Adobe</em></strong>-…
</code>

<strong>2. 1faa31928e.6 (0.322457) </strong>
<code>
<strong><em>PDF</em></strong> 32000-1:2008
Foreword
On January 29, 2007, <strong><em>Adobe</em></strong> Systems Incorporated announced it’s intention to release the full Portable
Document Format (<strong><em>PDF</em></strong>) 1.7 specification to the American National Standa…
...
</code>

or

	go run simple_index.go ~/testdata/adobe/*.pdf
	go run simple_search.go Type1

gives

<strong>searchResults=220 matches, showing 1 through 10, took 12.175059ms</strong>

<strong>1. 1faa31928e.710 (0.721218)</strong>
<code>
…bj

7  0  obj
<<  /Type  /Font
    /Subtype  /<strong><em>Type1</em></strong>
    /Name  /F1
    /BaseFont  /Helvetica
    /<strong><em>Encoding</em></strong> /<strong><em>MacRomanEncoding</em></strong>
>>
endobj
</code>

<strong>2. 1faa31928e.271 (0.427241)</strong>
<code>
…ut this <strong><em>encoding</em></strong>does play a role as a default <strong><em>encoding</em></strong>(as shown in Table 114). The regular encodings
used for Lat
</code>

References
==========
https://en.wikipedia.org/wiki/Full-text_search
https://en.wikipedia.org/wiki/Information_retrieval

TODO
====
Move to unidoc:v3
Make go gettable
Add tests
Markup PDF files
