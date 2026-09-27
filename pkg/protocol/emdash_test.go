package protocol

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNoEmDashes(t *testing.T) {
	root := filepath.Join("..", "..")
	emDash := "\u2014"

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == ".git" || info.Name() == "dist" {
				return filepath.SkipDir
			}
			return nil
		}

		ext := filepath.Ext(path)
		if ext != ".go" && ext != ".md" && ext != ".ps1" && info.Name() != "Makefile" {
			return nil
		}

		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}

		// Avoid flagging the test file definition itself
		if filepath.Base(path) == "emdash_test.go" {
			return nil
		}

		if strings.Contains(string(content), emDash) {
			t.Errorf("File %s contains em dash (\\u2014)!", path)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Walk failed: %v", err)
	}
}
