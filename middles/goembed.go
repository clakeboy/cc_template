package middles

import (
	"embed"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"path"
	"path/filepath"
	"strings"
)

type EmbedFiles struct {
	Embed embed.FS
	Path  string
}

func (e *EmbedFiles) Open(name string) (http.File, error) {
	if filepath.Separator != '/' && strings.ContainsRune(name, filepath.Separator) {
		return nil, errors.New("http: invalid character in file path")
	}
	fullName := filepath.Join(e.Path, filepath.FromSlash(path.Clean("/"+name)))
	f, err := e.Embed.Open(strings.ReplaceAll(fullName, "\\", "/"))
	wf := &EmbedFile{
		File: f,
	}
	return wf, err
}

type EmbedFile struct {
	io.Seeker
	fs.File
}

func (*EmbedFile) Readdir(count int) ([]fs.FileInfo, error) {
	return nil, nil
}
