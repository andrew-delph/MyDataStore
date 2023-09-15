package utils

type IntSet map[int]struct{}

func NewIntSet() IntSet {
	set := make(map[int]struct{})
	return set
}

func (s IntSet) From(values []int) IntSet {
	for _, value := range values {
		s.Add(value)
	}
	return s
}

func (s IntSet) Add(value int) IntSet {
	s[value] = struct{}{}
	return s
}

func (s IntSet) Has(value int) bool {
	_, exists := s[value]
	return exists
}

func (s IntSet) Remove(value int) IntSet {
	delete(s, value)
	return s
}

func (s IntSet) List() []int {
	var values []int
	for value := range s {
		values = append(values, value)
	}
	return values
}

func (setA IntSet) Difference(setB IntSet) IntSet {
	result := NewIntSet()
	for value := range setA {
		if _, exists := setB[value]; !exists {
			result.Add(value)
		}
	}
	return result
}

type Int32Set map[int32]struct{}

func NewInt32Set() Int32Set {
	set := make(map[int32]struct{})
	return set
}

func (s Int32Set) From(values []int32) Int32Set {
	for _, value := range values {
		s.Add(value)
	}
	return s
}

func (s Int32Set) Add(value int32) Int32Set {
	s[value] = struct{}{}
	return s
}

func (s Int32Set) Has(value int32) bool {
	_, exists := s[value]
	return exists
}

func (s Int32Set) Remove(value int32) Int32Set {
	delete(s, value)
	return s
}

func (s Int32Set) List() []int32 {
	var values []int32
	for value := range s {
		values = append(values, value)
	}
	return values
}

func (setA Int32Set) Difference(setB Int32Set) Int32Set {
	result := NewInt32Set()
	for value := range setA {
		if _, exists := setB[value]; !exists {
			result.Add(value)
		}
	}
	return result
}
