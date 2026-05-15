package capability

import "strings"

// NodeInfo holds the scoring inputs for a node.
type NodeInfo struct {
	ID            string
	Capabilities  []string
	Load          float64 // 0.0 - 1.0, lower is better
	HeartbeatAge  float64 // seconds since last heartbeat, lower is better
	ActiveTasks   int     // number of tasks currently assigned (lower is better)
	MaxCapacity   int     // max concurrent tasks this node supports (0 = unlimited)
	AvgCompletion float64 // average task completion time in seconds (lower is better)
}

// Score computes a composite score for a node relative to task requirements.
// Weights: capability 0.4, load 0.25, heartbeat 0.15, active-tasks 0.2.
// Returns -1 if the node doesn't match capabilities; otherwise 0.0 - 1.0 (higher is better).
func Score(node NodeInfo, requirements []string) float64 {
	if !Match(node.Capabilities, requirements) {
		return -1
	}

	// Capability score: fraction of requirements matched (1.0 if all match)
	capScore := 1.0
	if len(requirements) > 0 {
		matchCount := 0
		capSet := make(map[string]bool)
		for _, c := range node.Capabilities {
			capSet[strings.ToLower(c)] = true
		}
		for _, r := range requirements {
			if capSet[strings.ToLower(r)] {
				matchCount++
			}
		}
		capScore = float64(matchCount) / float64(len(requirements))
	}

	// Load score: lower load = higher score
	loadScore := 1.0 - node.Load

	// Heartbeat score: more recent = higher score (cap at 60s)
	hbScore := 1.0
	if node.HeartbeatAge > 60 {
		hbScore = 0.0
	} else {
		hbScore = 1.0 - (node.HeartbeatAge / 60.0)
	}

	// Active tasks score: fewer active tasks relative to capacity = higher score
	activeScore := 1.0
	if node.MaxCapacity > 0 {
		ratio := float64(node.ActiveTasks) / float64(node.MaxCapacity)
		if ratio >= 1.0 {
			activeScore = 0.0 // node is at capacity
		} else {
			activeScore = 1.0 - ratio
		}
	} else if node.ActiveTasks > 0 {
		// No explicit capacity: penalize based on absolute count (dimin returns)
		activeScore = 1.0 / (1.0 + float64(node.ActiveTasks))
	}

	return capScore*0.4 + loadScore*0.25 + hbScore*0.15 + activeScore*0.2
}

// RankNodes ranks nodes by score (descending). Returns only nodes with score >= 0.
func RankNodes(nodes []NodeInfo, requirements []string) []NodeInfo {
	type scored struct {
		node  NodeInfo
		score float64
	}
	var candidates []scored
	for _, n := range nodes {
		s := Score(n, requirements)
		if s >= 0 {
			candidates = append(candidates, scored{node: n, score: s})
		}
	}
	// Simple selection sort (small N)
	for i := 0; i < len(candidates); i++ {
		maxIdx := i
		for j := i + 1; j < len(candidates); j++ {
			if candidates[j].score > candidates[maxIdx].score {
				maxIdx = j
			}
		}
		candidates[i], candidates[maxIdx] = candidates[maxIdx], candidates[i]
	}
	result := make([]NodeInfo, len(candidates))
	for i, c := range candidates {
		result[i] = c.node
	}
	return result
}
