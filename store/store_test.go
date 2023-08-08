package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"log"
	"testing"

	"github.com/cbergoon/merkletree"
	"gotest.tools/assert"
)

func TestStore(t *testing.T) {

	setValue(1, "key1", "value2")

	value, _, _ := getValue(1, "key1")

	assert.Equal(t, value, "value2", "Both should be SetMessage")

}

// TestContent implements the Content interface provided by merkletree and represents the content stored in the tree.
type TestContent struct {
	x string
}

// CalculateHash hashes the values of a TestContent
func (t TestContent) CalculateHash() ([]byte, error) {
	h := sha256.New()
	if _, err := h.Write([]byte(t.x)); err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// Equals tests for equality of two Contents
func (t TestContent) Equals(other merkletree.Content) (bool, error) {
	otherTC, ok := other.(TestContent)
	if !ok {
		return false, errors.New("value is not of type TestContent")
	}
	return t.x == otherTC.x, nil
}

func TestMerkleTree(t *testing.T) {
	var list []merkletree.Content
	list = append(list, TestContent{x: "Hello"})
	list = append(list, TestContent{x: "Hi"})
	list = append(list, TestContent{x: "Hey"})
	list = append(list, TestContent{x: "Hola"})

	//Create a new Merkle Tree from the list of Content
	tree, err := merkletree.NewTree(list)
	if err != nil {
		log.Fatal(err)
	}

	//Get the Merkle Root of the tree
	mr := tree.MerkleRoot()
	log.Println(mr)

	//Verify the entire tree (hashes for each node) is valid
	vt, err := tree.VerifyTree()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Verify Tree: ", vt)

	//Verify a specific content in in the tree
	vc, err := tree.VerifyContent(list[0])
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Verify Content: ", vc)

	log.Println("Verify Hash: ", bytes.Equal(tree.Root.Hash, tree.MerkleRoot()))

	//String representation
	log.Println(t)
}
