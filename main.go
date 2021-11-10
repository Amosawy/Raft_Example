package main

import (
	"flag"
	"github.com/Amosawy/Raft_Example/node"
	"log"
	"strings"
)

func main()  {

	//使用命令行的格式启动：main --id 1 --cluster 127.0.0.1:22379,127.0.0.1:32379 --port :12379
	//id： 代表节点唯一标识
	//cluster： 其他节点地址。用逗号分隔
	//port：当前节点的监听的port
	port := flag.String("port",":9091","rpc listen port")
	cluster := flag.String("cluster","127.0.0.1:9091","comma sep")
	id := flag.Int("id",1,"node Id")

	flag.Parse()
	log.Print(*port," ",*cluster)
	clusters := strings.Split(*cluster,",")
	ns := make(map[int]*node.Node)
	for k,v := range clusters {
		ns[k] = node.NewNode(v)
	}

	//创建节点
	raft := &node.Raft{}
	raft.NewRaft(*id,ns)

	raft.Rpc(*port)
	raft.Start()

	select {

	}
}
