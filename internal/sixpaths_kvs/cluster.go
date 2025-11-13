package sixpaths_kvs

import "fmt"

type NodeConfig struct {
	ID         string
	ClientAddr string // HTTP port for clients
	RaftAddr   string
	DataDir    string
}

// Static 6-node cluster config.
var staticCluster = []NodeConfig{
	{ID: "n1", ClientAddr: ":8090", RaftAddr: ":9001", DataDir: "./data1"},
	{ID: "n2", ClientAddr: ":8091", RaftAddr: ":9002", DataDir: "./data2"},
	{ID: "n3", ClientAddr: ":8092", RaftAddr: ":9003", DataDir: "./data3"},
	{ID: "n4", ClientAddr: ":8093", RaftAddr: ":9004", DataDir: "./data4"},
	{ID: "n5", ClientAddr: ":8094", RaftAddr: ":9005", DataDir: "./data5"},
	{ID: "n6", ClientAddr: ":8095", RaftAddr: ":9006", DataDir: "./data6"},
}

// returns (thisNode, allNodes, error).
func ConfigForID(id string) (NodeConfig, []NodeConfig, error) {
	all := make([]NodeConfig, len(staticCluster))
	copy(all, staticCluster)

	for _, c := range all {
		if c.ID == id {
			return c, all, nil
		}
	}
	return NodeConfig{}, nil, fmt.Errorf("unknown node id %q", id)
}

// returns copy of our slice of NodeCOnfigs
func ClusterConfig() []NodeConfig {
	out := make([]NodeConfig, len(staticCluster))
	copy(out, staticCluster)
	return out
}
