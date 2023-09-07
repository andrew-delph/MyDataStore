package storage

import (
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
)

type Column interface {
	GetValue() string
}

type UnorderedColumn struct {
	Name string
}

func CreateUnorderedColumn(name string) Column {
	return UnorderedColumn{Name: name}
}

func (uc UnorderedColumn) GetValue() string {
	return uc.Name
}

type OrderedColumn struct {
	Value  uint32
	Length int
}

func CreateOrderedColumn(value uint32, length int) Column {
	if len(string(value)) > length {
		logrus.Fatal("Cannot create collumn: name > length")
	}
	return OrderedColumn{Value: value, Length: length}
}

func (oc OrderedColumn) GetValue() string {
	valueStr := fmt.Sprintf("%d", oc.Value)
	return fmt.Sprintf("%s%s", strings.Repeat("0", oc.Length-len(valueStr)), valueStr)
}

type Index struct {
	Name    string
	Columns []Column
}

func NewIndex(name string) *Index {
	return &Index{
		Name:    name,
		Columns: make([]Column, 0),
	}
}

func (idx *Index) AddColumn(column Column) *Index {
	idx.Columns = append(idx.Columns, column)
	return idx
}

func (idx *Index) Build() string {
	index := fmt.Sprintf("%s", idx.Name)
	for _, column := range idx.Columns {
		index += "_" + column.GetValue()
	}
	return index
}
