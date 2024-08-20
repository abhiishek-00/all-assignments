package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"gopkg.in/yaml.v2"
	"github.com/google/uuid"
)

type NodeDetail struct {
	NodeName   string                 `yaml:"node_name"`
	DependsOn  string                 `yaml:"depends_on"`
	IsCritical bool                   `yaml:"is_critical"`
	IPC1       map[string]interface{} `yaml:"ipc1"`
	IPC2       map[string]interface{} `yaml:"ipc2"`
	IPC2R2     map[string]interface{} `yaml:"ipc2r2"`
	NodeID     string                 `yaml:"node_id"`
	ParentID   string                 `yaml:"parent_id"`
}

type InputYAML struct {
	ApplicationName string      `yaml:"application_name"`
	NodeDetails     []NodeDetail `yaml:"node_details"`
}

func main() {
	// Read the input YAML file
	inputFile, err := os.ReadFile("/mnt/data/input.yaml")
	if err != nil {
		log.Fatalf("Failed to read input file: %v", err)
	}

	// Unmarshal the input YAML
	var inputData InputYAML
	err = yaml.Unmarshal(inputFile, &inputData)
	if err != nil {
		log.Fatalf("Failed to unmarshal input YAML: %v", err)
	}

	// Create a map to store node_name to node_id mapping
	nodeIDMap := make(map[string]string)

	// Iterate over each node and generate UUIDs for node_id and parent_id
	for i, node := range inputData.NodeDetails {
		// Generate node_id UUID
		nodeUUID := uuid.New().String()
		inputData.NodeDetails[i].NodeID = nodeUUID
		nodeIDMap[node.NodeName] = nodeUUID

		// Generate parent_id UUID based on depends_on field
		if parentID, exists := nodeIDMap[node.DependsOn]; exists {
			inputData.NodeDetails[i].ParentID = parentID
		} else {
			inputData.NodeDetails[i].ParentID = "" // No parent, or parent not yet processed
		}
	}

	// Marshal the modified data back into YAML
	outputYAML, err := yaml.Marshal(&inputData)
	if err != nil {
		log.Fatalf("Failed to marshal output YAML: %v", err)
	}

	// Write the output YAML to a file
	err = ioutil.WriteFile("/mnt/data/output.yaml", outputYAML, 0644)
	if err != nil {
		log.Fatalf("Failed to write output file: %v", err)
	}

	fmt.Println("YAML transformation complete. Output written to /mnt/data/output.yaml")
}
