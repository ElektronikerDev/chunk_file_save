package main

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"net/http"
)

const chunkSize = 1 * 1024 * 1024 // 1 MB

type Chunk struct {
	ID       string `bson:"_id"`
	Filename string
	Data     []byte
}

type File struct {
	ID       string `bson:"_id"`
	Filename string
	Chunks   []string
	MimeType string
}

func main() {
	// TODO: get string from .env
	// Set up a connection to the MongoDB server.
	clientOptions := options.Client().ApplyURI("mongodb://<TODO>")
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = client.Connect(nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Disconnect(nil)

	// Upload Route
	http.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		// Parse the multipart form / body
		err := r.ParseMultipartForm(32 << 20)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		file, fileHeader, err := r.FormFile("file")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer file.Close()

		// Read the file into a byte slice.
		var data []byte
		for {
			var chunk [chunkSize]byte
			n, err := file.Read(chunk[:])
			if err == io.EOF {
				break
			} else if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			data = append(data, chunk[:n]...)
		}

		fileID := uuid.New().String()
		f := &File{
			ID:       fileID,
			Filename: fileHeader.Filename,
			Chunks:   make([]string, len(data)/chunkSize+1),
			MimeType: fileHeader.Header.Get("Content-Type"),
		}

		// Split the data into chunks and store them.
		chunksColl := client.Database("chunk_file_save").Collection("chunks")
		for i, chunk := range data {
			chunkID := uuid.New().String()
			f.Chunks[i] = chunkID
			chunkData := &Chunk{
				ID:       chunkID,
				Filename: f.Filename,
				Data:     chunk,
			}
			_, err = chunksColl.InsertOne(nil, chunkData)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		// Store the File struct in the database
		filesColl := client.Database("chunk_file_save").Collection("files")
		_, err = filesColl.InsertOne(nil, f)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Send a response to the client
		response := map[string]string{
			"fileID": fileID,
		}
		responseData, err := json.Marshal(response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(responseData)
	})

	//TODO: Download Route

	http.ListenAndServe(":8080", nil)
}
