package main

import (
	"context"
	"fmt"

	vision "cloud.google.com/go/vision/apiv1"
)

// Annotate an image file based on Cloud Vision API, return score and error if exists.
// 调用gcd提供的面部识别软件
func annotate(uri string) (float32, error) {
	ctx := context.Background()
	client, err := vision.NewImageAnnotatorClient(ctx) // 调用
	if err != nil {
		return 0.0, err
	}
	defer client.Close()
	image := vision.NewImageFromURI(uri)                       // 传入url中所包含的图片内容
	annotations, err := client.DetectFaces(ctx, image, nil, 1) // 调用detectface function
	if err != nil {
		return 0.0, err
	}
	if len(annotations) == 0 {
		fmt.Println("No faces found.")
		return 0.0, nil
	}
	return annotations[0].DetectionConfidence, nil // return 判断的结果
}
