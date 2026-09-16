// Package mq 封装Kafka生产者/消费者——用segmentio/kafka-go(纯Go实现，无cgo依赖，本地开发
// 部署更简单，跟confluent-kafka-go相比是明确取舍，见plan文件)。
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
		AllowAutoTopicCreation: true,          // 保证engine端消费时的相对顺序不乱掉(照抄Java版按symbol分区路由的思路)
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

// Consumer 简单的单topic消费封装：MVP阶段一个engine进程订阅全部symbol，不做分片消费，
// 跟Java版contract-perp-engine现在的部署形态(单实例)对齐——按symbol分片是阶段六"横向
// 扩展"要做的事，不是这次范围
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

// Consume 阻塞读取，handler返回error只记日志不重投——MVP阶段简化处理，不做死信队列/重试
func (c *Consumer) Consume(ctx context.Context, handler func(key, value []byte) error) {
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("mq: read message error: %v", err)
			continue
		}
		if err := handler(msg.Key, msg.Value); err != nil {
			log.Printf("mq: handle message error (topic=%s key=%s): %v", msg.Topic, msg.Key, err)
		}
	}
}

func (c *Consumer) Close() error { return c.reader.Close() }
