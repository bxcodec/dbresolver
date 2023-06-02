//go:build generate

//go:generate go install github.com/mfridman/tparse@latest
//go:generate go install gotest.tools/gotestsum@latest
//go:generate tparse -v
//go:generate gotestsum  --version

/*
Installs test deps
*/

package dbresolver_test
