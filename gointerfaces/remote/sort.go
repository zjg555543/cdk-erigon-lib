package remote

import (
	"strings"

	"github.com/gateway-fm/cdk-erigon-lib/gointerfaces/types"
)

func NodeInfoReplyLess(i, j *types.NodeInfoReply) bool {
	if cmp := strings.Compare(i.Name, j.Name); cmp != 0 {
		return cmp == -1
	}
	return strings.Compare(i.Enode, j.Enode) == -1
}
