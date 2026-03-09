package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// workaround to get the message size from tracing events without having to 
// modify the actual libp2p library code
type gossipTracer struct {
	logger   *slog.Logger
	msgSizes sync.Map // message ID (string) -> data length (int)
}

func (g *gossipTracer) registerSize(data []byte) {
	if len(data) >= 8 {
		id := CalcID(data)
		g.msgSizes.Store(id, len(data))
	}
}

func (g *gossipTracer) getMessageSize(msgID []byte) int {
	if v, ok := g.msgSizes.Load(string(msgID)); ok {
		return v.(int)
	}
	return 0
}

func formatMessageID[S ~string | ~[]byte](msgID S) string {
	// Interpret the message ID as a big endian u64int
	return fmt.Sprintf("%d", binary.BigEndian.Uint64([]byte(msgID)))
}

func messageIDsToStr(msgIDs [][]byte) string {
	if len(msgIDs) == 0 {
		return ""
	}
	msgIDStrs := make([]string, len(msgIDs))
	for i, id := range msgIDs {
		msgIDStrs[i] = formatMessageID(id)
	}
	return "[" + strings.Join(msgIDStrs, ", ") + "]"
}

func (g *gossipTracer) logMeta(action string, logger *slog.Logger, meta *pubsub_pb.TraceEvent_RPCMeta) {
	if meta == nil {
		return
	}

	for _, msg := range meta.Messages {
		logger.LogAttrs(
			context.Background(),
			slog.LevelInfo,
			action+" Message",
			slog.String("topic", msg.GetTopic()),
			slog.String("id", formatMessageID(msg.GetMessageID())),
			slog.Int("size", g.getMessageSize(msg.GetMessageID())),
		)
	}

	for _, subs := range meta.Subscription {
		size := 1 + len(subs.GetTopic())
		logger.LogAttrs(context.Background(), slog.LevelInfo, action+" Subscription", slog.String("topic", subs.GetTopic()), slog.Int("size", size))
	}

	if meta.Control != nil {
		for _, graft := range meta.Control.GetGraft() {
			size := len(graft.GetTopic())
			logger.LogAttrs(context.Background(), slog.LevelInfo, action+" Graft", slog.String("topic", graft.GetTopic()), slog.Int("size", size))
		}
		for _, prune := range meta.Control.GetPrune() {
			size := len(prune.GetTopic())
			for _, peerID := range prune.GetPeers() {
				size += len(peerID)
			}
			logger.LogAttrs(context.Background(), slog.LevelInfo, action+" Prune", slog.String("topic", prune.GetTopic()), slog.Int("size", size))
		}
		for _, idontwant := range meta.Control.GetIdontwant() {
			size := 0
			for _, msgID := range idontwant.GetMessageIDs() {
				size += len(msgID)
			}
			msgIDs := messageIDsToStr(idontwant.GetMessageIDs())
			if msgIDs != "" {
				logger.LogAttrs(context.Background(), slog.LevelInfo, action+" Idontwant", slog.String("ids", msgIDs), slog.Int("size", size))
			}
		}
		for _, iwant := range meta.Control.GetIwant() {
			size := 0
			for _, msgID := range iwant.GetMessageIDs() {
				size += len(msgID)
			}
			msgIDs := messageIDsToStr(iwant.GetMessageIDs())
			if msgIDs != "" {
				logger.LogAttrs(context.Background(), slog.LevelInfo, action+" Iwant", slog.String("ids", msgIDs), slog.Int("size", size))
			}
		}
		for _, ihave := range meta.Control.GetIhave() {
			size := len(ihave.GetTopic())
			for _, msgID := range ihave.GetMessageIDs() {
				size += len(msgID)
			}
			msgIDs := messageIDsToStr(ihave.GetMessageIDs())
			if msgIDs != "" {
				logger.LogAttrs(context.Background(), slog.LevelInfo, action+" Ihave", slog.String("ids", msgIDs), slog.Int("size", size))
			}
		}
	}
}

// Trace implements pubsub.EventTracer.
func (g *gossipTracer) Trace(evt *pubsub_pb.TraceEvent) {
	switch *evt.Type {
	case pubsub_pb.TraceEvent_RECV_RPC:
		recv := evt.GetRecvRPC()
		from := peer.ID(recv.ReceivedFrom)
		logger := g.logger.With(
			slog.Time("event_time", time.Unix(0, evt.GetTimestamp())),
			slog.String("from", from.String()),
		)
		g.logMeta("Received", logger, recv.Meta)
	case pubsub_pb.TraceEvent_SEND_RPC:
		send := evt.GetSendRPC()
		to := peer.ID(send.GetSendTo())
		logger := g.logger.With(
			slog.Time("event_time", time.Unix(0, evt.GetTimestamp())),
			slog.String("to", to.String()),
		)
		g.logMeta("Sent", logger, send.GetMeta())
	/*case pubsub_pb.TraceEvent_GRAFT:
		graft := evt.GetGraft()
		g.logger.LogAttrs(
			context.Background(),
			slog.LevelInfo,
			"Graft",
			slog.String("topic", graft.GetTopic()),
			slog.String("peerId", peer.ID(graft.GetPeerID()).String()),
		)
	case pubsub_pb.TraceEvent_PRUNE:
		prune := evt.GetPrune()
		g.logger.LogAttrs(
			context.Background(),
			slog.LevelInfo,
			"Prune",
			slog.String("topic", prune.GetTopic()),
			slog.String("peerId", peer.ID(prune.GetPeerID()).String()),
		)*/
	case pubsub_pb.TraceEvent_PUBLISH_MESSAGE:
		publish := evt.GetPublishMessage()
		g.logger.LogAttrs(
			context.Background(),
			slog.LevelInfo,
			"Publish",
			slog.Time("event_time", time.Unix(0, evt.GetTimestamp())),
			slog.String("topic", publish.GetTopic()),
			slog.String("id", formatMessageID(publish.GetMessageID())),
			slog.Int("size", g.getMessageSize(publish.GetMessageID())),
		)
	case pubsub_pb.TraceEvent_DELIVER_MESSAGE:
		deliver := evt.GetDeliverMessage()
		g.logger.LogAttrs(
			context.Background(),
			slog.LevelInfo,
			"Deliver",
			slog.Time("event_time", time.Unix(0, evt.GetTimestamp())),
			slog.String("topic", deliver.GetTopic()),
			slog.String("id", formatMessageID(deliver.GetMessageID())),
			slog.String("from", peer.ID(deliver.GetReceivedFrom()).String()),
			slog.Int("size", g.getMessageSize(deliver.GetMessageID())),
		)
	case pubsub_pb.TraceEvent_ADD_PEER:
		addPeer := evt.GetAddPeer()
		g.logger.LogAttrs(
			context.Background(),
			slog.LevelInfo,
			"Added Peer",
			slog.Time("event_time", time.Unix(0, evt.GetTimestamp())),
			slog.String("id", peer.ID(addPeer.PeerID).String()),
		)
	case pubsub_pb.TraceEvent_REMOVE_PEER:
		removePeer := evt.GetRemovePeer()
		g.logger.LogAttrs(
			context.Background(),
			slog.LevelInfo,
			"Removed Peer",
			slog.Time("event_time", time.Unix(0, evt.GetTimestamp())),
			slog.String("id", peer.ID(removePeer.PeerID).String()),
		)
	}
}

// RawTracer implementation — captures actual message sizes before EventTracer logs them.

func (g *gossipTracer) RecvRPC(rpc *pubsub.RPC) {
	for _, msg := range rpc.Publish {
		g.registerSize(msg.Data)
	}
}

func (g *gossipTracer) SendRPC(rpc *pubsub.RPC, p peer.ID) {
	for _, msg := range rpc.Publish {
		g.registerSize(msg.Data)
	}
}

func (g *gossipTracer) DeliverMessage(msg *pubsub.Message) {
	g.registerSize(msg.Data)
}

func (g *gossipTracer) AddPeer(peer.ID, protocol.ID)              {}
func (g *gossipTracer) RemovePeer(peer.ID)                        {}
func (g *gossipTracer) Join(string)                               {}
func (g *gossipTracer) Leave(string)                              {}
func (g *gossipTracer) Graft(peer.ID, string)                     {}
func (g *gossipTracer) Prune(peer.ID, string)                     {}
func (g *gossipTracer) ValidateMessage(*pubsub.Message)           {}
func (g *gossipTracer) RejectMessage(*pubsub.Message, string)     {}
func (g *gossipTracer) DuplicateMessage(*pubsub.Message)          {}
func (g *gossipTracer) ThrottlePeer(peer.ID)                      {}
func (g *gossipTracer) DropRPC(*pubsub.RPC, peer.ID)              {}
func (g *gossipTracer) UndeliverableMessage(*pubsub.Message)      {}
