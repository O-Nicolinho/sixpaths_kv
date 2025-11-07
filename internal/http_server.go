package sixpaths_kvs

// Models

type putReq struct {
	Client string `json:"client"`
	Seq    uint64 `json:"seq"`
	Key    string `json:"key"`
	Value  string `json:"value"`
}

type delReq struct {
	Client string `json:"client"`
	Seq    uint64 `json:"seq"`
	Key    string `json:"key"`
}

type putDelResp struct {
	Success   bool   `json:"success"`
	PrevValue string `json:"prevValue"`
	LogIndex  uint64 `json:"logIndex"`
}

type getResp struct {
	Value string `json:"value"`
}

type errResp struct {
	Error string `json:"error"`
}

type healthResp struct {
	Status    string `json:"status"`
	LastIndex uint64 `json:"lastIndex"`
}

// Server

type HTTPServer struct {
	node *Node
	addr string
}

func NewHTTPServer(node *Node, addr string) *HTTPServer {

	return &HTTPServer{}
}
