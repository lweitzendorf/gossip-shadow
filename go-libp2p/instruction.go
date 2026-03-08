package main

import (
	"encoding/json"
	"fmt"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// Instruction is an interface that represents any instruction in the script
type Instruction interface {
	isInstruction()
}

// ConnectInstruction represents a connect instruction in the script
type ConnectInstruction struct {
	Type      string `json:"type"`
	ConnectTo []int  `json:"connectTo"`
}

func (ConnectInstruction) isInstruction() {}

// PublishInstruction represents a publish instruction in the script
type PublishInstruction struct {
	Type             string `json:"type"`
	MessageID        int    `json:"messageID"`
	MessageSizeBytes int    `json:"messageSizeBytes"`
	TopicID          string `json:"topicID"`
}

func (PublishInstruction) isInstruction() {}

// SubscribeToTopicInstruction represents a subscribe instruction in the script
type SubscribeToTopicInstruction struct {
	Type    string `json:"type"`
	TopicID string `json:"topicID"`
}

func (SubscribeToTopicInstruction) isInstruction() {}

// SetTopicValidationDelayInstruction represents a set topic validation delay instruction in the script
type SetTopicValidationDelayInstruction struct {
	Type         string  `json:"type"`
	TopicID      string  `json:"topicID"`
	DelaySeconds float64 `json:"delaySeconds"`
}

func (SetTopicValidationDelayInstruction) isInstruction() {}

// InitGossipSubInstruction represents an instruction to initialize GossipSub with specific parameters
type InitGossipSubInstruction struct {
	Type            string                 `json:"type"`
	GossipSubParams pubsub.GossipSubParams `json:"params"`
}

func (InitGossipSubInstruction) isInstruction() {}

// unmarshalInstruction unmarshals a JSON object into the appropriate Instruction type
func unmarshalInstruction(data []byte) (Instruction, error) {
	var temp struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &temp); err != nil {
		return nil, err
	}

	switch temp.Type {
	case "connect":
		var instruction ConnectInstruction
		if err := json.Unmarshal(data, &instruction); err != nil {
			return nil, err
		}
		return instruction, nil

	case "publish":
		var instruction PublishInstruction
		if err := json.Unmarshal(data, &instruction); err != nil {
			return nil, err
		}
		return instruction, nil

	case "subscribeToTopic":
		var instruction SubscribeToTopicInstruction
		if err := json.Unmarshal(data, &instruction); err != nil {
			return nil, err
		}
		return instruction, nil

	case "setTopicValidationDelay":
		var instruction SetTopicValidationDelayInstruction
		if err := json.Unmarshal(data, &instruction); err != nil {
			return nil, err
		}
		return instruction, nil

	case "initGossipSub":
		var tempInstruction struct {
			Type   string          `json:"type"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(data, &tempInstruction); err != nil {
			return nil, err
		}

		params := pubsub.DefaultGossipSubParams()
		if err := json.Unmarshal(tempInstruction.Params, &params); err != nil {
			return nil, err
		}
		return InitGossipSubInstruction{
			Type:            tempInstruction.Type,
			GossipSubParams: params,
		}, nil

	default:
		return nil, fmt.Errorf("unknown instruction type: %s", temp.Type)
	}
}

// ScriptInstruction groups instructions to execute at a specific elapsed time
type ScriptInstruction struct {
	ElapsedMillis int           `json:"elapsedMillis"`
	Instructions  []Instruction `json:"-"`
}

// UnmarshalJSON implements json.Unmarshaler for ScriptInstruction
func (si *ScriptInstruction) UnmarshalJSON(data []byte) error {
	var raw struct {
		ElapsedMillis int               `json:"elapsedMillis"`
		Instructions  []json.RawMessage `json:"instructions"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	si.ElapsedMillis = raw.ElapsedMillis
	si.Instructions = make([]Instruction, len(raw.Instructions))
	for i, rawInstr := range raw.Instructions {
		instr, err := unmarshalInstruction(rawInstr)
		if err != nil {
			return err
		}
		si.Instructions[i] = instr
	}
	return nil
}
