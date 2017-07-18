package batchconsumer

type batcherSync struct {
	tag    string
	writer *batchedWriter
}

func (b *batcherSync) SendBatch(batch [][]byte) {
	b.writer.SendBatch(batch, b.tag)
	b.writer.CheckPointBatch(b.tag)
}
