package batchconsumer

type BatcherSync struct {
	tag    string
	writer *BatchedWriter
}

func (b *BatcherSync) SendBatch(batch [][]byte) {
	b.writer.SendBatch(batch, b.tag)
	b.writer.CheckPointBatch(b.tag)
}
