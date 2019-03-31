package utils

import (
	"path/filepath"

	"github.com/blevesearch/bleve"
	"github.com/unidoc/unidoc/common"
)

// CreateBleveIndex creates a new persistent Bleve index at `indexPath`.
// If `forceCreate` is true then an existing index will be deleted.
// If `allowAppend` is true then an existing index will be appended to.
func CreateBleveIndex(indexPath string, forceCreate, allowAppend bool) (bleve.Index, error) {
	// Create a new index.
	mapping := bleve.NewIndexMapping()
	index, err := bleve.New(indexPath, mapping)
	if err == bleve.ErrorIndexPathExists {
		common.Log.Error("Bleve index %q exists.", indexPath)
		if forceCreate {
			common.Log.Info("Removing %q.", indexPath)
			removeIndex(indexPath)
			index, err = bleve.New(indexPath, mapping)
		} else if allowAppend {
			common.Log.Info("Opening existing %q.", indexPath)
			index, err = bleve.Open(indexPath)
		}
	}
	return index, err
}

// CreateBleveMemIndex creates a new in-memory (unpersisted) Bleve index.
func CreateBleveMemIndex() (bleve.Index, error) {
	// Create a new index.
	mapping := bleve.NewIndexMapping()
	index, err := bleve.NewMemOnly(mapping)
	return index, err
}

// removeIndex removes the Bleve index persistent data in `indexPath` from disk.
func removeIndex(indexPath string) {
	metaPath := filepath.Join(indexPath, "index_meta.json")
	if !Exists(metaPath) {
		common.Log.Error("%q doesn't appear to a be a Bleve index. %q doesn't exist.",
			indexPath, metaPath)
		return
	}
	if err := RemoveDirectory(indexPath); err != nil {
		common.Log.Error("RemoveDirectory(%q) failed. err=%v", indexPath, err)
	}
}
