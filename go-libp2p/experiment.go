package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

func CalcID(msg []byte) string {
	// The first 8 bytes of the message are the message ID
	return string(msg[0:8])
}

type HostConnector interface {
	ConnectTo(ctx context.Context, h host.Host, targetNodeId int) error
}

type scriptedNode struct {
	nodeID    int
	h         host.Host
	logger    *log.Logger
	slogger   *slog.Logger
	connector HostConnector
	pubsub    *pubsub.PubSub
	topics    map[string]*pubsub.Topic
	startTime time.Time
	subCtx    context.Context
}

func newScriptedNode(
	ctx context.Context,
	startTime time.Time,
	nodeID int,
	logger *log.Logger,
	slogger *slog.Logger,
	h host.Host,
	connector HostConnector,
	ps *pubsub.PubSub,
) (*scriptedNode, error) {
	slogger.Info("PeerID", "id", h.ID(), "node_id", nodeID)

	n := &scriptedNode{
		nodeID:    nodeID,
		h:         h,
		logger:    logger,
		slogger:   slogger,
		connector: connector,
		pubsub:    ps,
		startTime: startTime,
		subCtx:    ctx,
	}
	return n, nil
}

func (n *scriptedNode) runScriptInstruction(ctx context.Context, si ScriptInstruction) error {
	targetTime := n.startTime.Add(time.Duration(si.ElapsedMillis) * time.Millisecond)
	waitTime := time.Until(targetTime)
	if waitTime > 0 {
		n.logger.Printf("Waiting %s (until elapsed: %dms)", waitTime, si.ElapsedMillis)
		time.Sleep(waitTime)
	}

	for _, instruction := range si.Instructions {
		if err := n.runInstruction(ctx, instruction); err != nil {
			return err
		}
	}
	return nil
}

func (n *scriptedNode) runInstruction(ctx context.Context, instruction Instruction) error {
	switch a := instruction.(type) {
	case ConnectInstruction:
		for _, targetNodeId := range a.ConnectTo {
			err := n.connector.ConnectTo(ctx, n.h, targetNodeId)
			if err != nil {
				n.slogger.Error(err.Error())
			}
		}
		n.logger.Printf("Node %d connected to %d peers", n.nodeID, len(n.h.Network().Peers()))
	case PublishInstruction:
		topic, err := n.getTopic(a.TopicID)
		if err != nil {
			return fmt.Errorf("failed to get topic %s: %w", a.TopicID, err)
		}

		n.logger.Printf("Publishing message %d\n", a.MessageID)
		msg := make([]byte, a.MessageSizeBytes)
		binary.BigEndian.PutUint64(msg, uint64(a.MessageID))

		if err := topic.Publish(ctx, msg); err != nil {
			return fmt.Errorf("failed to publish message %d: %w", a.MessageID, err)
		}
		n.logger.Printf("Published message %d\n", a.MessageID)
	case SubscribeToTopicInstruction:
		topic, err := n.getTopic(a.TopicID)
		if err != nil {
			return fmt.Errorf("failed to get topic %s: %w", a.TopicID, err)
		}
		sub, err := topic.Subscribe()
		if err != nil {
			return fmt.Errorf("failed to subscribe to topic %s: %w", a.TopicID, err)
		}
		go func() {
			for {
				n.logger.Printf("Waiting to receive message\n")
				msg, err := sub.Next(n.subCtx)
				if errors.Is(err, context.Canceled) {
					return
				}

				if err != nil {
					n.logger.Printf("Failed to receive message: %v", err)
					return
				}
				msgID := binary.BigEndian.Uint64(msg.Data)
				n.logger.Printf("Received message %d\n", msgID)
			}
		}()
	case SetTopicValidationDelayInstruction:
		err := n.pubsub.RegisterTopicValidator(a.TopicID, func(context.Context, peer.ID, *pubsub.Message) pubsub.ValidationResult {
			duration := time.Duration(a.DelaySeconds * float64(time.Second))
			time.Sleep(duration)
			return pubsub.ValidationAccept
		})
		if err != nil {
			return err
		}

	default:
		return fmt.Errorf("unknown instruction type: %T", instruction)
	}

	return nil
}

func (n *scriptedNode) getTopic(topicStr string) (*pubsub.Topic, error) {
	if n.topics == nil {
		n.topics = make(map[string]*pubsub.Topic)
	}
	t, ok := n.topics[topicStr]
	if !ok {
		var err error
		t, err = n.pubsub.Join(topicStr)
		if err != nil {
			return nil, err
		}
		n.topics[topicStr] = t
	}
	return t, nil
}

func RunExperiment(ctx context.Context, startTime time.Time, logger *log.Logger, slogger *slog.Logger, h host.Host, nodeId int, connector HostConnector, params ExperimentParams, ps *pubsub.PubSub) error {
	n, err := newScriptedNode(ctx, startTime, nodeId, logger, slogger, h, connector, ps)
	if err != nil {
		return err
	}

	for _, si := range params.Script {
		if err := n.runScriptInstruction(ctx, si); err != nil {
			return fmt.Errorf("failed to run instruction: %w", err)
		}
	}

	return nil
}
