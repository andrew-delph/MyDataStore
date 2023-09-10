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
