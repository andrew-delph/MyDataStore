package utils

type IntSet map[int]struct{}

func NewIntSet() IntSet {
	return make(map[int]struct{})
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
