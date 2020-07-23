package main

// 运行逻辑：1. 在gce上运行了elasticsearch的server，由artifaceelastic.co提供，通过lesson25&26提供的start-elastic on gce instance上时刻启动
// sudo systeml enable elasticsearch可以保证这个function时刻运行
// 2. 这个class本质上调用elasticsearch的功能的restapi。通过作者olivere在github上的elasticsearch的go接口，在这个restapi中进行调用，处理数据，传递给client端
// 3. 下面会探讨不同function的功能，每一行代码的本质

//两个重要可供参考 https://www.cnblogs.com/ljhdo/p/4981928.html
// https://godoc.org/github.com/olivere/elastic#Client.CreateIndex

import (
	"context"
	"encoding/json" // 用于转换json文件
	"fmt"           // 用于打印
	"io"
	"log"
	"net/http" // 调用的默认名是http，go就是专门用来写server的语言，可以调用自己的http.request和http.ResponseWriter
	"path/filepath"
	"reflect" // ！！！！！！！！！！！！！！！！！！！有待补充
	"strconv" // go用于将string转化为其他类型 例如float64
	
	"github.com/gorilla/mux"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/olivere/elastic" // 调用默认名是elastic this package (elastic) provides an interface to the elasticsearch server
)

const ( // 类似java中的final定义，定义常量应当全部为大写，用括号包裹所有的const而不是{}
	POST_INDEX  = "post" //index的含义是，在elasticsearch的数据库端，创建了一个叫做post的index，（index之于es的意义，类似于table之于sql）
	DISTANCE    = "200km"
	ES_URL      = "http://​10.128.0.5​:9200/" //inner url，不能用external url
	BUCKET_NAME = "​283905-bucket"
)

var (
	mediaTypes = map[string]string{
		".jpeg": "image",
		".jpg":  "image",
		".gif":  "image",
		".png":  "image",
		".mov":  "video",
		".mp4":  "video",
		".avi":  "video",
		".flv":  "video",
		".wmv":  "video",
	}
)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	// `json:"user"` is for the json parsing of this User field. Otherwise, by default it's 'User'.
	User     string   `json:"user"` // 用于告诉decoder转换string 为相应json的key下数据
	Message  string   `json:"message"`
	Location Location `json:"location"`
	Url      string   `json:"url"`
	Type     string   `json:"type"`
	Face     float32  `json:"face"`
}

func main() {

	r := mux.NewRouter()
	r.Handle("/post", http.HandlerFunc(handlerPost)).Methods("POST", "OPTIONS") 
	r.Handle("/search", http.HandlerFunc(handlerSearch)).Methods("GET", "OPTIONS") 
	r.Handle("/cluster", http.HandlerFunc(handlerCluster)).Methods("GET", "OPTIONS")
	​log.Fatal(http.ListenAndServe(":8080", ​r​))

	// fmt.Println("started-service")
	// http.HandleFunc("/post", handlerPost)     // servelet样的doPost
	// http.HandleFunc("/search", handlerSearch) // servelet样的doget
	// http.HandleFunc("/cluster", handlerCluster)
	// log.Fatal(http.ListenAndServe(":8080", nil)) // log fatal的意思如果出现程序的fatel应该终止
}

func handlerPost(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one post request")
	w.Header().Set("Content-Type", "application/json")

	w.Header().Set("Access-Control-Allow-Origin", "*") 
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")
	if r.Method == "OPTIONS" { 
		return
	}

	lat, _ := strconv.ParseFloat(r.FormValue("lat"), 64)
	lon, _ := strconv.ParseFloat(r.FormValue("lon"), 64)
	p := &Post{
		User:    r.FormValue("user"),
		Message: r.FormValue("message"),
		Location: Location{
			Lat: lat,
			Lon: lon,
		},
	}
	file, header, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "Image is not available", http.StatusBadRequest)
		fmt.Printf("Image is not available %v\n", err)
		return
	}
	suffix := filepath.Ext(header.Filename)
	if t, ok := mediaTypes[suffix]; ok {
		p.Type = t
	} else {
		p.Type = "unknown"
	}
	id := uuid.New()
	mediaLink, err := saveToGCS(file, id)
	if err != nil {
		http.Error(w, "Failed to save image to GCS", http.StatusInternalServerError)
		fmt.Printf("Failed to save image to GCS %v\n", err)
		return
	}
	p.Url = mediaLink
	if p.Type == "image" {
		uri := fmt.Sprintf("gs://%s/%s", BUCKET_NAME, id)
		if score, err := annotate(uri); err != nil {
			http.Error(w, "Failed to annotate image", http.StatusInternalServerError)
			fmt.Printf("Failed to annotate the image %v\n", err)
			return
		} else {
			p.Face = score
		}
	}
	err = saveToES(p, POST_INDEX, id)
	if err != nil {
		http.Error(w, "Failed to save post to Elasticsearch", http.StatusInternalServerError)
		fmt.Printf("Failed to save post to Elasticsearch %v\n", err)
		return
	}
	// // Parse from body of request to get a json object. fmt.Println("Received one post request")
	// decoder := json.NewDecoder(r.Body) // 转换json为post文件
	// var p Post
	// if err := decoder.Decode(&p); err != nil {
	// 	panic(err)
	// }
	// fmt.Fprintf(w, "Post received: %s\n", p.Message) // 写给前端的ResponseWriter
}

func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search")
	w.Header().Set("Content-Type", "application/json")

	w.Header().Set("Access-Control-Allow-Origin", "*") 
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")
	if r.Method == "OPTIONS" { 
		return
	}

	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
	ran := DISTANCE // range is optional
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}
	fmt.Println("range is ", ran)
	query := elastic.NewGeoDistanceQuery("location")   // location对应保存在json格式中的location key，// query等同于sql中的 select location = “”
	query = query.Distance(ran).Lat(lat).Lon(lon)      // 设置ran，lat和lon各自的where ran = ？ 值
	searchResult, err := readFromES(query, POST_INDEX) // 在elasticsearch的db中的查找操作
	if err != nil {
		http.Error(w, "Failed to read post from Elasticsearch", http.StatusInternalServerError)
		fmt.Printf("Failed to read post from Elasticsearch %v.\n", err)
		return
	}
	posts := getPostFromSearchResult(searchResult) // 将查找返回结果处理成post数据结构
	js, err := json.Marshal(posts)                 // matshal可以将自定义结构转换为json
	if err != nil {
		http.Error(w, "Failed to parse posts into JSON format", http.StatusInternalServerError)
		fmt.Printf("Failed to parse posts into JSON format %v.\n", err)
		return
	}
	w.Write(js) // 返回前端的写操作
}

func readFromES(query elastic.Query, index string) (*elastic.SearchResult, error) {
	client, err := elastic.NewClient(elastic.SetURL(ES_URL)) // 连接gce上面的elasitcsearch engine的方法
	if err != nil {
		return nil, err
	}
	searchResult, err := client.Search().Index(index).Query(query).Pretty(true).Do(context.Background())
	// index对应table的名字，query对应select * from table操作，
	if err != nil {
		return nil, err
	}
	return searchResult, nil
}

func getPostFromSearchResult(searchResult *elastic.SearchResult) []Post { //
	var ptype Post
	var posts []Post

	// searchresult返回的是interface类型，通过reflect.TypeOf这种映射操作，告知searchresult（类似一个object）中，将所有能转化为post类型的数据取出，执行{}的部分
	for _, item := range searchResult.Each(reflect.TypeOf(ptype)) {
		p := item.(Post) // 类似java中过得类型转换，p = (Post) item
		posts = append(posts, p)
	}
	return posts
}

func handlerCluster(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one cluster request")
	w.Header().Set("Content-Type", "application/json")

	w.Header().Set("Access-Control-Allow-Origin", "*") 
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")
	if r.Method == "OPTIONS" { 
		return
	}

	term := r.URL.Query().Get("term")
	query := elastic.NewRangeQuery(term).Gte(0.9)
	searchResult, err := readFromES(query, POST_INDEX)
	if err != nil {
		http.Error(w, "Failed to read from Elasticsearch", http.StatusInternalServerError)
		return
	}
	posts := getPostFromSearchResult(searchResult)
	js, err := json.Marshal(posts)
	if err != nil {
		http.Error(w, "Failed to parse post object", http.StatusInternalServerError)
		fmt.Printf("Failed to parse post object %v\n", err)
		return
	}
	w.Write(js)
}

func saveToES(post *Post, index string, id string) error {
	client, err := elastic.NewClient(elastic.SetURL(ES_URL))
	if err != nil {
		return err
	}
	_, err = client.Index().Index(index).Id(id).BodyJson(post).Do(context.Background())
	if err != nil {
		return err
	}
	fmt.Printf("Post is saved to index: %s\n", post.Message)
	return nil
}

func saveToGCS(r io.Reader, objectName string) (string, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx) // call google.storge提供的api，创建client
	if err != nil {
		return "", err
	}
	bucket := client.Bucket(BUCKET_NAME) // 连接bucket
	if _, err := bucket.Attrs(ctx); err != nil {
		return "", err
	}
	object := bucket.Object(objectName)       // 创建的类型是storage.objecthandler
	wc := object.NewWriter(ctx)               // wc是bucket用于接受来自r的内容的接收器
	if _, err := io.Copy(wc, r); err != nil { // copy是将后者（r）的内容写给前者（wc）
		return "", err
	}
	if err := wc.Close(); err != nil { // 写完关闭bucket的writer
		return "", err
	}
	if err := object.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil { //
		return "", err
	}
	attrs, err := object.Attrs(ctx) // attr的类型是storage.objectattrs
	if err != nil {
		return "", err
	}
	fmt.Printf("Image is saved to GCS: %s\n", attrs.MediaLink)
	return attrs.MediaLink, nil
}
