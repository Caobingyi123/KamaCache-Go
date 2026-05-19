package kamacache

//把 Peer 接口里的 Get / Set / Delete，变成真正的 gRPC 请求发给远程缓存节点。

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	pb "github.com/youngyangyang04/KamaCache-Go/pb" //grpc的协议底层代码
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	addr    string
	svcName string             // 服务名
	etcdCli *clientv3.Client   // etcd客户端
	conn    *grpc.ClientConn   // grpc 连接
	grpcCli pb.KamaCacheClient // grpc 客户端
}

// 在编译期检查 *Client 是否实现了 Peer 接口。
var _ Peer = (*Client)(nil)

// 创建一个连接的远程的Client
func NewClient(addr string, svcName string, etcdCli *clientv3.Client) (*Client, error) {
	var err error
	if etcdCli == nil {
		etcdCli, err = clientv3.New(clientv3.Config{
			Endpoints:   []string{"localhost:2379"},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create etcd client: %v", err)
		}
	}

	conn, err := grpc.NewClient(addr,
		// 使用明文连接；生产环境通常应换成 TLS credentials。
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// NewClient 只创建 gRPC channel，不会像旧版 Dial+WithBlock 那样立刻阻塞到连接成功。
		// 真正的连接会在首次 RPC 时触发，下面这个选项让 RPC 在连接暂时不可用时等待 ready。
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc client: %v", err)
	}

	// 用 gRPC 连接创建一个具体的 protobuf 客户端
	grpcClient := pb.NewKamaCacheClient(conn)

	client := &Client{
		addr:    addr,
		svcName: svcName,
		etcdCli: etcdCli,
		conn:    conn,
		grpcCli: grpcClient,
	}

	return client, nil
}

func (c *Client) Get(group, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 发送 gRPC 请求,查具体哪个group里面的哪个key
	resp, err := c.grpcCli.Get(ctx, &pb.Request{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get value from kamacache: %v", err)
	}

	return resp.GetValue(), nil
}

func (c *Client) Delete(group, key string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := c.grpcCli.Delete(ctx, &pb.Request{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return false, fmt.Errorf("failed to delete value from kamacache: %v", err)
	}

	return resp.GetValue(), nil
}

func (c *Client) Set(ctx context.Context, group, key string, value []byte) error {
	resp, err := c.grpcCli.Set(ctx, &pb.Request{
		Group: group,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("failed to set value to kamacache: %v", err)
	}
	logrus.Infof("grpc set request resp: %+v", resp)

	return nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
