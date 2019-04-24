package doclib

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

// MkParentDir creates the parent directory for `filename` if it doesn't already exist.
func MkParentDir(filename string) error {
	dir, _ := filepath.Split(filename)
	return MkDir(dir)
}

// MkDir creates a directory called `dir` if it doesn't already exist.
func MkDir(dir string) error {
	_, err := os.Stat(dir)
	if !os.IsNotExist(err) {
		if err != nil {
			fmt.Fprintf(os.Stderr, "MkDir: Stat failed. dir=%q err=%v\n", dir, err)
			return err
		}
		return nil
	}
	err = os.Mkdir(dir, 0777)
	if err != nil {
		fmt.Fprintf(os.Stderr, "MkDir: Mkdir failed. dir=%q err=%v\n", dir, err)
	}
	return err
}

// Exists returns true if `filename` exists.
func Exists(filename string) bool {
	_, err := os.Stat(filename)
	if err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Exists: Stat failed. filename=%q err=%v\n", filename, err)
	}
	return err == nil
}

// WriteJsonSlice writes slice `vals` to json file `filename`, one line per string.
// NOTE: We write this json file in a human readable way because we will be using it in development
func WriteJsonSlice(filename string, vals []string) error {
	body := strings.Join(vals, ",\n")
	parts := []string{"[", body, "]"}
	whole := strings.Join(parts, "\n")
	err := ioutil.WriteFile(filename, []byte(whole), 0777)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WriteJsonSlice: WriteFile failed filename=%q err=%v\n", filename, err)
		return err
	}
	fmt.Printf("WriteJsonSlice: Saved %d entries to %q\n", len(vals), filename)
	return nil
}

// ChangePathDir returns `inPath` with its ancestor directory `inDir` replaced with `outDir` and its
// extension replaced with `outExt`.
func ChangePathDirExt(inDir, inPath, outDir, outExt string) (string, error) {
	outPath, err := ChangePathDir(inDir, inPath, outDir)
	if err != nil {
		return "", err
	}
	return ChangePathExt(outPath, outExt), nil
}

// ChangePathDir returns `inPath` with its ancestor directory `inDir` replaced with `outDir`.
func ChangePathDir(inDir, inPath, outDir string) (string, error) {
	rel, err := filepath.Rel(inDir, inPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ChangePathDir: Rel failed. inPath=%q inDir=%q err=%v\n",
			inPath, inDir, err)
		return "", err
	}
	return filepath.Join(outDir, rel), nil
}

// ChangePathExt returns `inPath` with its extension replaced with `outExt`.
func ChangePathExt(inPath, outExt string) string {
	return inPath[:len(inPath)-len(filepath.Ext(inPath))] + outExt
}

// RemoveDirectory recursively removes directory `dir` and its contents from disk.
func RemoveDirectory(dir string) error {
	if dir == "" || strings.HasPrefix(dir, ".") || strings.HasPrefix(dir, "/") {
		full, _ := filepath.Abs(dir)
		panic(fmt.Errorf("RemoveDirectory: Suspicious dir=%q (%q)", dir, full))
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return os.Remove(dir)
}
