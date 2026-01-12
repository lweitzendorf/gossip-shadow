package main

import (
	"encoding/json"
	"fmt"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// ScriptInstruction is an interface that represents any instruction in the script
type ScriptInstruction interface {
	isInstruction()
}

// ConnectInstruction represents a connect instruction in the script
type ConnectInstruction struct {
	Type      string `json:"type"`
	ConnectTo []int  `json:"connectTo"`
}

// isInstruction implements the ScriptInstruction interface
func (ConnectInstruction) isInstruction() {}

// IfNodeIDInInstruction represents conditional instructions based on node ID
type IfNodeIDInInstruction struct {
	Type         string              `json:"type"`
	NodeIDs      []int               `json:"nodeIDs"`
	Instructions []ScriptInstruction `json:"instructions"`
}

// isInstruction implements the ScriptInstruction interface
func (IfNodeIDInInstruction) isInstruction() {}

// WaitUntilInstruction represents a wait until instruction in the script
type WaitUntilInstruction struct {
	Type          string `json:"type"`
	ElapsedMillis int    `json:"elapsedMillis"`
}

// isInstruction implements the ScriptInstruction interface
func (WaitUntilInstruction) isInstruction() {}

// ShutDownInstruction represents a shutdown instruction in the script
type ShutDownInstruction struct {
	Type string `json:"type"`
}

// isInstruction implements the ScriptInstruction interface
func (ShutDownInstruction) isInstruction() {}

// PublishInstruction represents a publish instruction in the script
type PublishInstruction struct {
	Type             string `json:"type"`
	MessageID        int    `json:"messageID"`
	MessageSizeBytes int    `json:"messageSizeBytes"`
	TopicID          string `json:"topicID"`
}

// isInstruction implements the ScriptInstruction interface
func (PublishInstruction) isInstruction() {}

// SubscribeToTopicInstruction represents a subscribe instruction in the script
type SubscribeToTopicInstruction struct {
	Type    string `json:"type"`
	TopicID string `json:"topicID"`
}

// isInstruction implements the ScriptInstruction interface
func (SubscribeToTopicInstruction) isInstruction() {}

// SetTopicValidationDelayInstruction represents a set topic validation delay instruction in the script
type SetTopicValidationDelayInstruction struct {
	Type         string  `json:"type"`
	TopicID      string  `json:"topicID"`
	DelaySeconds float64 `json:"delaySeconds"`
}

// isInstruction implements the ScriptInstruction interface
func (SetTopicValidationDelayInstruction) isInstruction() {}

// InitGossipSubInstruction represents an instruction to initialize GossipSub with specific parameters
type InitGossipSubInstruction struct {
	Type            string                 `json:"type"`
	GossipSubParams pubsub.GossipSubParams `json:"gossipSubParams"`
}

// isInstruction implements the ScriptInstruction interface
func (InitGossipSubInstruction) isInstruction() {}

// UnmarshalScriptInstruction unmarshals a JSON object into the appropriate ScriptInstruction type
func UnmarshalScriptInstruction(data []byte) (ScriptInstruction, error) {
	// Unmarshal just the type field to determine which concrete type to use
	var temp struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &temp); err != nil {
		return nil, err
	}

	// Unmarshal to the appropriate concrete type based on the instruction type
	switch temp.Type {
	case "connect":
		var instruction ConnectInstruction
		if err := json.Unmarshal(data, &instruction); err != nil {
			return nil, err
		}
		return instruction, nil

	case "ifNodeIDIn":
		var tempInstruction struct {
			Type         string            `json:"type"`
			NodeIDs      []int             `json:"nodeIDs"`
			Instructions []json.RawMessage `json:"instructions"`
		}
		if err := json.Unmarshal(data, &tempInstruction); err != nil {
			return nil, err
		}

		// Recursively unmarshal the nested instructions
		var nestedInstructions = make([]ScriptInstruction, len(tempInstruction.Instructions))

		for i, nestedTempInstruction := range tempInstruction.Instructions {
			nestedInstruction, err := UnmarshalScriptInstruction(nestedTempInstruction)
			if err != nil {
				return nil, err
			}
			nestedInstructions[i] = nestedInstruction
		}

		return IfNodeIDInInstruction{
			Type:         tempInstruction.Type,
			NodeIDs:      tempInstruction.NodeIDs,
			Instructions: nestedInstructions,
		}, nil

	case "waitUntil":
		var instruction WaitUntilInstruction
		if err := json.Unmarshal(data, &instruction); err != nil {
			return nil, err
		}
		return instruction, nil

	case "shutDown":
		var instruction ShutDownInstruction
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
			Type            string          `json:"type"`
			GossipSubParams json.RawMessage `json:"gossipSubParams"`
		}
		if err := json.Unmarshal(data, &tempInstruction); err != nil {
			return nil, err
		}

		// Start with default parameters
		params := pubsub.DefaultGossipSubParams()

		// Only override values that are specified in the JSON
		if err := json.Unmarshal(tempInstruction.GossipSubParams, &params); err != nil {
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

// ScriptInstructions is a slice of ScriptInstruction that can be unmarshaled from JSON
type ScriptInstructions []ScriptInstruction

// UnmarshalJSON implements json.Unmarshaler for ScriptInstructions
func (si *ScriptInstructions) UnmarshalJSON(data []byte) error {
	var rawInstructions []json.RawMessage
	if err := json.Unmarshal(data, &rawInstructions); err != nil {
		return err
	}

	instructions := make([]ScriptInstruction, len(rawInstructions))
	for i, raw := range rawInstructions {
		instruction, err := UnmarshalScriptInstruction(raw)
		if err != nil {
			return err
		}
		instructions[i] = instruction
	}

	*si = instructions
	return nil
}
