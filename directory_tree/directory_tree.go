// This is borrowed heavily from github.com/marcinwyszynski/directory_tree
//
// Some minor edits were needed to add the Gdrive ID field for our specialized use-case.
package directory_tree

import (
	"os"
	"path/filepath"
	"time"
)

// FileInfo is a struct created from os.FileInfo interface for serialization.
type FileInfo struct {
	Name    string      `json:"name"`
	Size    int64       `json:"size"`
	Mode    os.FileMode `json:"mode"`
	ModTime time.Time   `json:"mod_time"`
	IsDir   bool        `json:"is_dir"`
}

// Helper function to create a local FileInfo struct from os.FileInfo interface.
func fileInfoFromInterface(v os.FileInfo) *FileInfo {
	return &FileInfo{
		Name:    v.Name(),
		Size:    v.Size(),
		Mode:    v.Mode(),
		ModTime: v.ModTime(),
		IsDir:   v.IsDir()}
}

// Node represents a node in a directory tree.
type Node struct {
	FullPath string    `json:"path"`
	Info     *FileInfo `json:"info"`
	Children []*Node   `json:"children"`
	Parent   *Node     `json:"-"`
	DriveID  string
}

// Create directory hierarchy.
func NewTree(root string) (result *Node, err error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return
	}
	parents := make(map[string]*Node)
	walkFunc := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		parents[path] = &Node{
			FullPath: path,
			Info:     fileInfoFromInterface(info),
			Children: make([]*Node, 0),
		}
		return nil
	}
	if err = filepath.Walk(absRoot, walkFunc); err != nil {
		return
	}
	for path, node := range parents {
		parentPath := filepath.Dir(path)
		parent, exists := parents[parentPath]
		if !exists { // If a parent does not exist, this is the root.
			result = node
		} else {
			node.Parent = parent
			parent.Children = append(parent.Children, node)
		}
	}
	return
}
