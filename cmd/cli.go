package main

import (
	"bufio"
	"crypto/rand"
	"errors"
	"io"
	"math/big"
	"strconv"

	// "github.com/anmolrajsoni15/anmols_distributed_file_system/auth"
	"context"
	"fmt"
	"time"

	"os"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/anmolrajsoni15/anmols_distributed_file_system/crypto"
	"github.com/anmolrajsoni15/anmols_distributed_file_system/p2p"
	"github.com/anmolrajsoni15/anmols_distributed_file_system/server"
	"github.com/anmolrajsoni15/anmols_distributed_file_system/store"
)

var client *mongo.Client
var userCollection *mongo.Collection
var isAuthenticated bool
var isRoot bool

// User struct to hold user information
type User struct {
	Id         string `bson:"_id,omitempty"`
	Username   string `bson:"username"`
	Password   string `bson:"password"`
	Port       int    `bson:"port"`
	EncryptKey []byte `bson:"encrypt_key"`
	ServerId   string `bson:"server_id"`
}

func initMongoDB() error {
	// Replace <username>, <password>, and <cluster-url> with your MongoDB Atlas credentials
	uri := "mongodb+srv://<username>:<password>@cluster0.tixvf.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var err error
	client, err = mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return err
	}

	userCollection = client.Database("distributed_file_system").Collection("users")
	fmt.Println("Connected to MongoDB!")
	return nil
}

func generateRandomPort() (int, error) {
	for {
		port, err := rand.Int(rand.Reader, big.NewInt(7000))
		if err != nil {
			return 0, err
		}
		port = big.NewInt(port.Int64() + 2000)

		// Check if port is unique
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		count, err := userCollection.CountDocuments(ctx, bson.M{"port": port})
		if err != nil {
			return 0, err
		}
		if count == 0 {
			return int(port.Int64()), nil
		}
	}
}

func makeServer(listenAddr string, encKey []byte, serverId string, nodes ...string) *server.FileServer {
	tcptransportOpts := p2p.TCPTransportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: p2p.NOPHandshakeFunc,
		Decoder:       p2p.DefaultDecoder{},
	}
	tcpTransport := p2p.NewTCPTransport(tcptransportOpts)

	fileServerOpts := server.FileServerOpts{
		ID:                serverId,
		EncKey:            encKey,
		StorageRoot:       "network_" + strings.Split(listenAddr, ":")[1],
		PathTransformFunc: store.CASPathTransformFunc,
		Transport:         tcpTransport,
		BootstrapNodes:    nodes,
	}

	s := server.NewFileServer(fileServerOpts)

	tcpTransport.OnPeer = s.OnPeer

	return s
}

func main() {
	reader := bufio.NewReader(os.Stdin)

	// Initialize MongoDB
	err := initMongoDB()
	if err != nil {
		fmt.Printf("Error initializing MongoDB: %v\n", err)
		return
	}

	fmt.Println("Welcome to the P2P File Sharing CLI!")
	fmt.Println("Please enter your Login details to start the server.")
	fmt.Printf("Username: ")
	username, _ := reader.ReadString('\n')
	username = strings.TrimSpace(username)
	fmt.Printf("Password: ")
	password, _ := reader.ReadString('\n')
	password = strings.TrimSpace(password)

	user, err := authenticateUser(username, password)
	if err != nil {
		fmt.Printf("Error authenticating user: %v\n", err)
		return
	}

	fmt.Printf("User authenticated successfully\n\n")

	var s *server.FileServer

	if user != nil {
		isAuthenticated = true
		if user.Username == "rootadmin" {
			isRoot = true
		}
		port := fmt.Sprint(":", user.Port)

		fmt.Printf("Enter the bootstrap ports for the server (comma separated, leave empty if none): ")
		bootstrapPortsStr, _ := reader.ReadString('\n')
		bootstrapPortsStr = strings.TrimSpace(bootstrapPortsStr)
		var bootstrapPorts []string
		if bootstrapPortsStr != "" {
			bootstrapPorts = strings.Split(bootstrapPortsStr, ",")
		}

		s = makeServer(port, user.EncryptKey, user.ServerId, bootstrapPorts...)
		go s.Start()
	}

	// fmt.Printf("Enter the port for server.")
	// port, _ := reader.ReadString('\n')
	// port = strings.TrimSpace(port)

	// fmt.Printf("Enter the bootstrap ports for the server (comma separated, leave empty if none): ")
	// bootstrapPortsStr, _ := reader.ReadString('\n')
	// bootstrapPortsStr = strings.TrimSpace(bootstrapPortsStr)
	// var bootstrapPorts []string
	// if bootstrapPortsStr != "" {
	//     bootstrapPorts = strings.Split(bootstrapPortsStr, ",")
	// }
	// s := makeServer(port, bootstrapPorts...)
	// go s.Start() // Start the server in a goroutine

	fmt.Println("Welcome to the P2P File Sharing CLI!")
	if isRoot {
		fmt.Println("Available commands: register")
	} else {
		fmt.Println("Available commands: store, get, delete, list, help, exit")
	}
	for {
		fmt.Print("> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "exit" {
			fmt.Println("Goodbye!")
			break
		}

		handleCommand(input, s)
	}
}

func handleCommand(input string, c *server.FileServer) {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return
	}

	switch parts[0] {
	case "register":
		if !isRoot {
			fmt.Println("Only root user can register new users")
			return
		}
		if len(parts) != 3 {
			fmt.Println("Usage: register <username> <password>")
			return
		}
		user, err := registerUser(parts[1], parts[2])
		if err != nil {
			fmt.Printf("Error registering user: %v\n", err)
			return
		}
		fmt.Printf("User registered successfully\n")
		fmt.Println("User Credentials:-")
		fmt.Printf("Username: %s\n", user.Username)
		fmt.Printf("Password: %s\n", user.Password)
		fmt.Printf("Port: %d\n", user.Port)
		fmt.Printf("Server ID: %s\n", user.ServerId)

		fmt.Printf("\n\nPlease login to start the server\n")
		fmt.Printf("Type 'exit' to exit the program and login with new credentials\n")

		// fmt.Printf("Encryption Key: %v\n", user.EncryptKey)
	case "store":
		if len(parts) != 3 {
			fmt.Println("Usage: store <key> <filepath>")
			return
		}
		if isRoot {
			fmt.Println("Operation not allowed for rootadmin user")
			return
		}
		if !isAuthenticated {
			fmt.Println("Please login or register first")
			return
		}
		handleStore(c, parts[1], parts[2])
	case "get":
		if len(parts) != 2 && len(parts) != 4 {
			fmt.Println("Usage: get <key>, get <key> --port <port>")
			return
		}
		if isRoot {
			fmt.Println("Operation not allowed for rootadmin user")
			return
		}
		if !isAuthenticated {
			fmt.Println("Please login or register first")
			return
		}
		if len(parts) == 2 {
			handleGet(c, parts[1])
		} else if len(parts) == 4 && parts[2] == "--port" {
			port, err := strconv.Atoi(parts[3])
			if err != nil {
				fmt.Printf("Error parsing port: %v\n", err)
				return
			}
			getOtherServedFile(c, parts[1], port)
		}
	case "delete":
		if len(parts) != 2 {
			fmt.Println("Usage: delete <key>")
			return
		}
		if isRoot {
			fmt.Println("Operation not allowed for rootadmin user")
			return
		}
		if !isAuthenticated {
			fmt.Println("Please login or register first")
			return
		}
		handleDelete(c, parts[1])
	case "list":
		if isRoot {
			fmt.Println("Operation not allowed for rootadmin user")
			return
		}
		if !isAuthenticated {
			fmt.Println("Please login or register first")
			return
		}
		handleList(c)
	case "help":
		printHelp()
	default:
		fmt.Println("Unknown command. Type 'help' for available commands.")
	}
}

func handleStore(c *server.FileServer, key string, filepath string) {
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
	}
	defer file.Close()
	err = c.Store(key, file)
	if err != nil {
		fmt.Printf("Error storing file: %v\n", err)
	} else {
		fmt.Println("File stored successfully")
	}
}

func handleGet(c *server.FileServer, key string) {
	readr, err := c.Get(key)
	if err != nil {
		fmt.Printf("Error getting file: %v\n", err)
		return
	}
	defer readr.(io.ReadCloser).Close()
	// defer c.Stop()
	// fmt.Printf("File content: %s\n", readr)

	content, err := io.ReadAll(readr)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
	}
	fmt.Printf("File content: %s\n", content)
}

func handleDelete(c *server.FileServer, key string) {
	err := c.Stores.Delete(c.ID, key)
	if err != nil {
		fmt.Printf("Error deleting file: %v\n", err)
	} else {
		fmt.Println("File deleted successfully")
	}
}

func handleList(c *server.FileServer) {
	files, err := c.Stores.List()
	if err != nil {
		fmt.Printf("Error listing files: %v\n", err)
		return
	}

	fmt.Println("Stored files:")
	for _, file := range files {
		fmt.Println(file)
	}

	if len(files) == 0 {
		fmt.Println("No files stored")
	}

}

func registerUser(username, password string) (*User, error) {
	// Check if username already exists
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	count, err := userCollection.CountDocuments(ctx, bson.M{"username": username})
	if err != nil {
		return nil, err
	}
	if count > 0 {
		return nil, errors.New("username already exists")
	}

	// Generate unique EncryptKey and ServerId
	encryptKey := crypto.NewEncryptionKey()
	serverId := crypto.GenerateID()

	// Generate unique port
	port, err := generateRandomPort()
	if err != nil {
		return nil, err
	}

	user := &User{
		Id:         serverId + fmt.Sprint(port),
		Username:   username,
		Password:   password,
		Port:       port,
		EncryptKey: encryptKey,
		ServerId:   serverId,
	}

	_, err = userCollection.InsertOne(ctx, user)
	if err != nil {
		return nil, err
	}

	return user, nil
}

func authenticateUser(username, password string) (*User, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var user User
	err := userCollection.FindOne(ctx, bson.M{"username": username, "password": password}).Decode(&user)
	if err != nil {
		return nil, errors.New("invalid username or password")
	}

	return &user, nil
}

func getOtherServedFile(c *server.FileServer, key string, port int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var user User
	err := userCollection.FindOne(ctx, bson.M{"port": port}).Decode(&user)
	if err != nil {
		fmt.Printf("Error getting user: %v\n", err)
		return
	}

	readr, err := c.GetServerd(user.ServerId, key, user.EncryptKey)
	if err != nil {
		fmt.Printf("Error getting file: %v\n", err)
		return
	}

	content, err := io.ReadAll(readr)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
	}
	fmt.Printf("File content: %s\n", content)
}

func printHelp() {
	fmt.Println("Available commands:")
	if isRoot {
		fmt.Println("  register <username> <password> - Register a new user")
	} else {
		fmt.Println("  store <key> <filepath> - Store a file")
		fmt.Println("  get <key>              - Retrieve a file")
		fmt.Println("  delete <key>           - Delete a file (not implemented)")
		fmt.Println("  list                   - List all stored files (not implemented)")
		fmt.Println("  help                   - Print this help message")
		fmt.Println("  exit                   - Exit the program")
	}
}
