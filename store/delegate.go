package main

type MyDelegate struct {
	msgCh chan []byte
}

func GetMyDelegate() *MyDelegate {
	delegate := new(MyDelegate)

	delegate.msgCh = make(chan []byte)

	return delegate
}

func (d *MyDelegate) NotifyMsg(msg []byte) {
	d.msgCh <- msg
}
func (d *MyDelegate) NodeMeta(limit int) []byte {
	// not use, noop
	return []byte("")
}
func (d *MyDelegate) LocalState(join bool) []byte {
	// not use, noop
	return []byte("")
}
func (d *MyDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	// not use, noop
	return nil
}
func (d *MyDelegate) MergeRemoteState(buf []byte, join bool) {
	// not use
}
