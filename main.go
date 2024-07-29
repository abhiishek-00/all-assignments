package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

type InputNode struct {
	ApplicationName string       `json:"application_name"`
	CarID           string       `json:"car_id"`
	NodeDetails     []NodeDetail `json:"node_details"`
}

type NodeDetail struct {
	NodeName   string `json:"node_name"`
	NodeID     string `json:"node_id"`
	ParentID   string `json:"parent_id"`
	IsCritical bool   `json:"is_critical"`
	IPC1       IPC    `json:"ipc1"`
	IPC2       IPC    `json:"ipc2"`
	IPC2R2     IPC    `json:"ipc2r2"`
}

type IPC struct {
	F5   string `json:"f5"`
	GTM  string `json:"gtm"`
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

type OutputNode struct {
	ID       string       `json:"id"`
	Name     string       `json:"name"`
	State    NodeState    `json:"state"`
	Children []OutputNode `json:"children,omitempty"`
}

type NodeState struct {
	IPC1   bool   `json:"ipc1"`
	IPC2   bool   `json:"ipc2"`
	IPC2R2 string `json:"ipc2r2"`
}

type Output struct {
	Status  int          `json:"status"`
	Message string       `json:"message"`
	Data    []OutputNode `json:"data"`
}

func main() {
	inputJSON := `{
		"application_name": "Enterprise Login",
		"car_id": "123456789",
		"node_details": [
			{
				"node_name": "Web",
				"node_id": "web_123",
				"parent_id": "123456789",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.8.141.125",
					"port": 443
				},
				"ipc2": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2r2": {}
			},
			{
				"node_name": "MFA POD",
				"node_id": "mfapod_123",
				"parent_id": "web_123",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2": {},
				"ipc2r2": {}
			},
			{
				"node_name": "CBIS-23",
				"node_id": "cbis23_123",
				"parent_id": "mfapod_123",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2": {},
				"ipc2r2": {}
			},
			{
				"node_name": "Mobile",
				"node_id": "mobile_123",
				"parent_id": "123456789",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2": {},
				"ipc2r2": {}
			},
			{
				"node_name": "MFA POA",
				"node_id": "mfapoa_123",
				"parent_id": "mobile_123",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2": {},
				"ipc2r2": {}
			},
			{
				"node_name": "Redis",
				"node_id": "redis_123",
				"parent_id": "mfapoa_123",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2": {},
				"ipc2r2": {}
			},
			{
				"node_name": "MFA PPP",
				"node_id": "mfappp_123",
				"parent_id": "mobile_123",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2": {},
				"ipc2r2": {}
			},
			{
				"node_name": "Tokenizer",
				"node_id": "tokenizer_123",
				"parent_id": "mfappp_123",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2": {},
				"ipc2r2": {}
			},
			{
				"node_name": "Redis",
				"node_id": "redis_123",
				"parent_id": "tokenizer_123",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2": {},
				"ipc2r2": {}
			},
			{
				"node_name": "MFA POD",
				"node_id": "mfapod_123",
				"parent_id": "mobile_123",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2": {},
				"ipc2r2": {}
			},
			{
				"node_name": "CBIS",
				"node_id": "cbis_123",
				"parent_id": "123456789",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2": {},
				"ipc2r2": {}
			},
			{
				"node_name": "Verf",
				"node_id": "verf_123",
				"parent_id": "cbis_123",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2": {},
				"ipc2r2": {}
			},
			{
				"node_name": "Risk Dispatcher",
				"node_id": "risk_123",
				"parent_id": "cbis_123",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2": {},
				"ipc2r2": {}
			},
			{
				"node_name": "Push AuthN",
				"node_id": "push_123",
				"parent_id": "cbis_123",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2": {},
				"ipc2r2": {}
			},
			{
				"node_name": "Agent Binding",
				"node_id": "agentb123_123",
				"parent_id": "push_123",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2": {},
				"ipc2r2": {}
			},
			{
				"node_name": "DSS",
				"node_id": "dss_123",
				"parent_id": "push_123",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2": {},
				"ipc2r2": {}
			},
			{
				"node_name": "MFA DB",
				"node_id": "mfadb_123",
				"parent_id": "cbis_123",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2": {},
				"ipc2r2": {}
			},
			{
				"node_name": "Risk Dispatcher",
				"node_id": "risk_123",
				"parent_id": "mfadb_123",
				"is_critical": true,
				"ipc1": {
					"f5": "phx-dc1ecpgtm1a.phx.aexp.com",
					"gtm": "enterpriseidentitysreportal.igdha-ecp.aexp.com",
					"ip": "10.34.131.77",
					"port": 443
				},
				"ipc2": {},
				"ipc2r2": {}
			}
		]
	}`

	var input InputNode
	err := json.Unmarshal([]byte(inputJSON), &input)
	if err != nil {
		log.Fatalf("Error unmarshaling input JSON: %v", err)
	}

	output := Output{
		Status:  200,
		Message: "Success",
		Data: []OutputNode{
			{
				ID:    input.CarID,
				Name:  input.ApplicationName,
				State: NodeState{},
			},
		},
	}

	nodeMap := make(map[string]*OutputNode)
	nodeMap[input.CarID] = &output.Data[0] // Adding a placeholder for the root node

	for _, inputNode := range input.NodeDetails {
		processNode(&inputNode, nodeMap)
	}

	outputJSON, err := json.MarshalIndent(output, "", "    ")
	if err != nil {
		log.Fatalf("Error marshaling output JSON: %v", err)
	}

	fmt.Println(string(outputJSON))
}

func processNode(inputNode *NodeDetail, nodeMap map[string]*OutputNode) {
	parentID := strings.TrimSpace(inputNode.ParentID)

	state := NodeState{
		IPC1:   inputNode.IPC1.IP != "",
		IPC2:   inputNode.IPC2.IP != "",
		IPC2R2: "NA",
	}

	outputNode := OutputNode{
		ID:    inputNode.NodeID,
		Name:  inputNode.NodeName,
		State: state,
	}

	parentNode, exists := nodeMap[parentID]
	if exists {
		parentNode.Children = append(parentNode.Children, outputNode)
		nodeMap[outputNode.ID] = &parentNode.Children[len(parentNode.Children)-1]    // Update nodeMap with the new child node reference
		nodeMap[inputNode.NodeID] = &parentNode.Children[len(parentNode.Children)-1] // Update nodeMap with the new child node ID
	} else {
		fmt.Printf("Parent node with ID %s not found\n", parentID)
		return
	}
}
