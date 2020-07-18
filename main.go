package main
  
import (
	elastic "gopkg.in/olivere/elastic.v3"
	"fmt"
	"net/http"
	"encoding/json"
	"log"
	"strconv"
	"reflect"
	"github.com/pborman/uuid"
)

type Location struct {
	Lat float64 `json:"lat"` // 
	Lon float64 `json:"lon"`
}

type Post struct {
	User string `json:"user"` // User 必须大写，不然不是public
	Message string `json:"message"`
	Location Location `json:"location"`
}

const ( // 类似于java中的final定义,定义常量全为大写, 用括号包裹而不是{}!!
	INDEX = "around" // index本质是与项目的名称保持一致
	TYPE = "post"
	DISTANCE = "200km"
	ES_URL = "http://34.71.213.156:9200"


)


func main () {
	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(INDEX).Do()
	if err != nil {
		panic(err)
	}
	if !exists {
		// Create a new index.

		// 将location转化为geo_point
		mapping := `{
			"mappings":{
				"post":{
					"properties":{
						"location":{ 
							"type":"geo_point"
						}
					}
				}
			}
		}`
		_, err := client.CreateIndex(INDEX).Body(mapping).Do()
		if err != nil {
			// Handle error
			panic(err)
		}
	}

	fmt.Println("started-service")
	http.HandleFunc("/post", handlerPost) // 类似servelet的doPost
	http.HandleFunc("/search",handlerSearch) // 类似servelet的doGet
	log.Fatal(http.ListenAndServe(":8080", nil)) // log.fatel意思如果出现程序fatel应该终止
	// listenAndServe意思是监听特定端口
	// nil便是null，HandleFunc已经为http定义了 ListenAndServe(addr string, handler http.Handler)正常传参的第二部分，因而这部分加入nil即可
}

// {
//  user:"john"
// "message": "Test"
// "location" : {
//	"lat": 37
// "lon": -120
//}
//} http.Request中只能包含以上格式的json，格式必须一一对应

func handlerPost (w http.ResponseWriter, r *http.Request) { 
	// *的含义为传入指针，在此函数内修改人，function外的也会变化
	// Parse from body of request to get a json object.
	fmt.Println("Received one post request")
	decoder := json.NewDecoder(r.Body) // := 直接赋值，不定义初始类型的方法, 创建了一个json的decoder，deocde response的body部分
	// r.body, 意思是将body部分decode
	var p Post
	if err := decoder.Decode(&p); err != nil { // 分号的含义，if有两个statement，第一个用于初始化变量，此时的error的作用域不同，此时,error过了if语句就失效.不能再调用了 
		// 第二个是真正的判断 err != nil 错误不为空
		// Decode 中的 &p 为 传递指针，这样能修改原来p中的值
		// 根据p的定义格式，转化http.Request
		   panic(err) // 抛异常
	}
	fmt.Fprintf(w, "Post received: %s\n", p.Message) // fprintf将p.message写入w中去，fprintf和c语言的fprintf相同含义

	id := uuid.New() // 每次生成一个unique的string，用来标记这一次生成的内容和上一次不一样，在es中用id区分每一个数据而不是以数据的内容
	// Save to ES.
	saveToES(&p, id)
}

// Save a post to ElasticSearch
func saveToES(p *Post, id string) {
	// Create a client，用于呼叫gce上的elastic
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}

	// Save it to index 即将这个内容保存到gce中去
	_, err = es_client.Index().
		Index(INDEX).
		Type(TYPE).
		Id(id).
		BodyJson(p).
		Refresh(true).
		Do()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Post is saved to Index: %s\n", p.Message)
}


func handlerSearch (w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search")
	lat,_ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64) // 如果从request的url即http链接中获取参数,get出来是个string
	lon,_ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64) // ParseFloat返回值是两个，第二个为返回error，用 lon,_ 表示第二个返回值我们不在乎

	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km" // go里string必须是双引号
	}

	fmt.Printf( "Search received: %f %f %s\n", lat, lon, ran)
	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false)) // sniff中如果为true，可以设置回调函数
	if err != nil {
		panic(err)
	}

      // Define geo distance query as specified in
      // https://www.elastic.co/guide/en/elasticsearch/reference/5.2/query-dsl-geo-distance-query.html
    q := elastic.NewGeoDistanceQuery("location")
    q = q.Distance(ran).Lat(lat).Lon(lon)

      // Some delay may range from seconds to minutes. So if you don't get enough results. Try it later.
    searchResult, err := client.Search().Index(INDEX).Query(q).Pretty(true).Do()
    if err != nil {
    	panic(err)
    }

      // searchResult is of type SearchResult and returns hits, suggestions,
      // and all kinds of other information from Elasticsearch.
    fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
      // TotalHits is another convenience function that works even when something goes wrong.
    fmt.Printf("Found a total of %d post\n", searchResult.TotalHits())

	// Each is a convenience function that iterates over hits in a search result.
    // It makes sure you don't need to check for nil values in the response.
	// However, it ignores errors in serialization.
	
	// searchresult返回的是interface类型，通过reflect.TypeOf这种映射操作，告知searchresult（类似一个object）中，将所有能转化为post类型的数据取出，执行{}的部分
    var typ Post
    var ps []Post
    for _, item := range searchResult.Each(reflect.TypeOf(typ)) { // 类似java中的 instance of
        p := item.(Post) // 类似java中的类型转换  p = (Post) item，
        fmt.Printf("Post by %s: %s at lat %v and lon %v\n", p.User, p.Message, p.Location.Lat, p.Location.Lon)
        // TODO(student homework): Perform filtering based on keywords such as web spam etc.
        ps = append(ps, p)

    }
    js, err := json.Marshal(ps) // 转换为js的string
    if err != nil {
        panic(err)
    }

    w.Header().Set("Content-Type", "application/json")
    w.Header().Set("Access-Control-Allow-Origin", "*")
    w.Write(js)


	// fmt.Println("range is ", ran)

	// return a fake post
	// p := &Post{ // 制作一个struct的方法
	// 	User:"1111",
	// 	Message:"一生必去的100个地方",
	// 	Location: Location{
	// 		   Lat:lat,
	// 		   Lon:lon,
	// 	},
 	// }

	// js, err := json.Marshal(p) // 将post这个数据结构直接转换为json的string数据结构
	// 直接传指针p的好处，避免了一次拷贝
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Fprintf(w, "Search received: %s %s", lat, lon)
	// w.Header().Set("Content-Type","application/json")
	// w.Write(js)

}


