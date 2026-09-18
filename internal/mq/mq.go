package mq

import (
	"context"
	"encoding/json"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string) *Producer {
	return &Producer{writer: &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Balancer:               &kafka.Hash{}, // 按key哈希分区——同一个symbol的下单/撤单事件要落到同一分区，
		AllowAutoTopicCreation: true,          // 保证engine端消费时的相对顺序不乱掉
	}}
}

func (p *Producer) Publish(ctx context.Context, topic, key string, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{Topic: topic, Key: []byte(key), Value: body})
}

func (p *Producer) Close() error { return p.writer.Close() }

// 简单的单topic消费封装：MVP阶段一个engine进程订阅全部symbol，不做分片消费，
// 单实例部署——按symbol分片是后续"横向扩展"阶段要做的事，不是这次范围
type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{reader: kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})}
}

// Message 一条Kafka消息交给handler处理时需要的全部信息，包含Topic/Partition/Offset——
// 这三个字段合起来是这条消息在Kafka里的全局唯一坐标，是WithDedup做消息级去重的依据
type Message struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int
	Offset    int64
}

// 阻塞读取，handler返回error只记日志不重投——MVP阶段简化处理，不做死信队列/重试
func (c *Consumer) Consume(ctx context.Context, handler func(msg Message) error) {
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("mq: read message error: %v", err)
			continue
		}
		m := Message{Key: msg.Key, Value: msg.Value, Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset}
		if err := handler(m); err != nil {
			log.Printf("mq: handle message error (topic=%s partition=%d offset=%d key=%s): %v",
				msg.Topic, msg.Partition, msg.Offset, msg.Key, err)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

// DedupChecker 由调用方提供"这条消息是不是第一次处理"的判断，mq包不关心具体用什么存储
// 实现——真正的实现在internal/repo.ProcessedMessageRepo(按topic+partition+offset落MySQL
// 表)，这里只依赖一个小接口，避免mq包直接依赖DB/repo包
type DedupChecker interface {
	TryMark(ctx context.Context, topic string, partition int, offset int64) (bool, error)
}

// WithDedup 包一层消息级去重：处理前先标记这条消息的(topic,partition,offset)有没有处理过，
// 已经处理过的直接跳过、不重复执行handler，防御Kafka at-least-once语义下的重复投递被业务
// 逻辑处理第二遍——这是完整的消息级方案，不依赖具体业务字段(比如order_id)，见
// docs/message-dedup.md。标记本身失败(比如DB抖动)保守选择继续执行handler而不是拒绝处理，
// 去重层是锦上添花的正确性加固，不能变成消息完全消费不了的新单点故障
func WithDedup(ctx context.Context, checker DedupChecker, handler func(msg Message) error) func(msg Message) error {
	return func(msg Message) error {
		first, err := checker.TryMark(ctx, msg.Topic, msg.Partition, msg.Offset)
		if err != nil {
			log.Printf("[WARN] 消息去重标记失败(topic=%s partition=%d offset=%d)，按未去重处理: %v",
				msg.Topic, msg.Partition, msg.Offset, err)
			return handler(msg)
		}
		if !first {
			log.Printf("[WARN] 检测到重复投递，跳过(topic=%s partition=%d offset=%d)",
				msg.Topic, msg.Partition, msg.Offset)
			return nil
		}
		return handler(msg)
	}
}
