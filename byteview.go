package kamacache

// ByteView 只读的字节视图，用于缓存数据
// 保证缓存里的数据只能被读不能被修改
type ByteView struct {
	b []byte // 小写，让字段不能被外部读取，防止修改
}

func (b ByteView) Len() int { // 返回缓存数据的长度
	return len(b.b)
}

func (b ByteView) ByteSLice() []byte {
	return cloneBytes(b.b) //只能拿到副本，防止修改数据
}

func (b ByteView) String() string {
	//string是不可变的，所以肯定安全
	return string(b.b) // 复制 byte → string。方便直接println
}

func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b) // 把 b 拷贝到 c
	return c
}
