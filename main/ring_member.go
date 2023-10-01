package main

type RingMember struct {
	Name      string
}

func CreateRingMember(name string) RingMember {
	return RingMember{Name: name}
}

func (m RingMember) String() string {
	return string(m.Name)
}
