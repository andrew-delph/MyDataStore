package storage

import (
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
)

type Column interface {
	GetValue() (string, error)
	GetName() string
	Parse(string) string
}

type UnorderedColumn struct {
	Name  string
	Value string
}

func CreateUnorderedColumn(name, value string) Column {
	return UnorderedColumn{Name: name, Value: value}
}

func (uc UnorderedColumn) GetName() string {
	return uc.Name
}

func (uc UnorderedColumn) GetValue() (string, error) {
	return uc.Value, nil
}

func (uc UnorderedColumn) Parse(value string) string {
	return value
}

type OrderedColumn struct {
	Name   string
	Value  string
	Length int
}

func CreateOrderedColumn(name, value string, length int) Column {
	return OrderedColumn{Name: name, Value: value, Length: length}
}

func (uc OrderedColumn) GetName() string {
	return uc.Name
}

func (oc OrderedColumn) GetValue() (string, error) {
	var valueStr string = oc.Value

	if len(string(oc.Value)) > oc.Length || oc.Length-len(valueStr) < 0 {
		return "", fmt.Errorf("index value invalid length %d", len(string(oc.Value)))
	}
	return fmt.Sprintf("%s%s", strings.Repeat("0", oc.Length-len(valueStr)), valueStr), nil
}

func (uc OrderedColumn) Parse(value string) string {
	parsedValue := strings.TrimLeft(value, "0")
	if parsedValue == "" { // if it is 0
		parsedValue = "0"
	}
	return parsedValue
}

type Index struct {
	Columns []Column
}

func NewIndex(name string) *Index {
	columns := make([]Column, 0)
	columns = append(columns, CreateUnorderedColumn("name", name))
	return &Index{
		Columns: columns,
	}
}

func (idx *Index) AddColumn(column Column) *Index {
	idx.Columns = append(idx.Columns, column)
	return idx
}

func (idx *Index) Build() (string, error) {
	index := ""
	for i, column := range idx.Columns {
		value, err := column.GetValue()
		if err != nil {
			return "", err
		}
		if i != 0 {
			index += "_"
		}
		index += value

	}
	return index, nil
}

func (idx *Index) Parse(index string) (map[string]string, error) {
	parts := strings.Split(index, "_")
	if len(idx.Columns) != len(parts) {
		return nil, fmt.Errorf("index columns #%d and parts #%d not equal", len(idx.Columns)+1, len(parts))
	}
	m := map[string]string{}
	for i := len(idx.Columns) - 1; i >= 0; i-- {
		column := idx.Columns[i]
		logrus.Warnf("i %d parts[i] %s", i, parts[i])
		m[column.GetName()] = column.Parse(parts[i])
	}
	return m, nil
}
