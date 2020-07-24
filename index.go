package main

// 用于在es engine上新建表的class

import (
	"context"
	"fmt"

	"github.com/olivere/elastic"
)

const (
	POST_INDEX = "post" //index的含义是，在elasticsearch的数据库端，创建了一个叫做post的index，（index之于es的意义，类似于table之于sql）
	USER_INDEX = "user"
	ES_URL     = "http://10.128.0.5:9200"
)

func main() {
	client, err := elastic.NewClient(elastic.SetURL(ES_URL)) // 连接gce上面的elasitcsearch engine的方法
	if err != nil {
		panic(err)
	}
	exists, err := client.IndexExists(POST_INDEX).Do(context.Background()) // 判断这个表是不是存在
	if err != nil {
		panic(err)
	}
	if !exists { // mapping类似于mysql中的定义表的格式，如果这个表不存在的话，就进行以下的新建操作，按照nosql进行理解
		mapping :=
			`{ 
			"mappings": {
				"properties": {
					"user": { "type": "keyword", "index": false }, 
					"message": { "type": "keyword", "index": false }, 
					"location": { "type": "geo_point" },
					"url": { "type": "keyword", "index": false },
					"type": { "type": "keyword", "index": false },
					"face": { "type": "float" }
				}
			}
		}`
		_, err := client.CreateIndex(POST_INDEX).Body(mapping).Do(context.Background()) // 新建一个表
		if err != nil {
			panic(err)
		}
	}

	exists, err = client.IndexExists(USER_INDEX).Do(context.Background())
	if err != nil {
		panic(err)
	}
	if !exists {
		mapping := `{
			"mappings": { 
				"properties": {
					"username": {"type": "keyword"},
					"password": {"type": "keyword", "index": false}, 
					"age": {"type": "long", "index": false}, 
					"gender": {"type": "keyword", "index": false}
				} 
			}
		}`

		_, err = client.CreateIndex(USER_INDEX).Body(mapping).Do(context.Background())
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("Post index is created.")
}
