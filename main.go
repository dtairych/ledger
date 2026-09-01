package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/lib/pq"
    amqp "github.com/rabbitmq/amqp091-go"
)

type LogMessage struct {
    Timestamp   time.Time              `json:"timestamp"`
    Level       string                 `json:"level"`
    Service     string                 `json:"service"`
    Message     string                 `json:"message"`
    Caller      string                 `json:"caller"`
    StackTrace  string                 `json:"stack_trace,omitempty"`
    TraceID     string                 `json:"trace_id,omitempty"`
    RequestID   string                 `json:"request_id,omitempty"`
    UserID      string                 `json:"user_id,omitempty"`
    Additional  map[string]interface{} `json:"additional,omitempty"`
}

type Config struct {
    PostgresURL string
    RabbitMQURL string
    QueueName   string
}

func getConfig() Config {
    return Config{
        PostgresURL: os.Getenv("POSTGRES_URL"),
        RabbitMQURL: os.Getenv("RABBITMQ_URL"),
        QueueName:   os.Getenv("QUEUE_NAME"),
    }
}

func initDB(ctx context.Context, postgresURL string) (*sql.DB, error) {
    db, err := sql.Open("postgres", postgresURL)
    if err != nil {
        return nil, err
    }

    // Create logs table if it doesn't exist
    _, err = db.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS logs (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            level VARCHAR(10) NOT NULL,
            service VARCHAR(100) NOT NULL,
            message TEXT NOT NULL,
            caller TEXT,
            stack_trace TEXT,
            trace_id VARCHAR(100),
            request_id VARCHAR(100),
            user_id VARCHAR(100),
            additional JSONB
        )
    `)
    if err != nil {
        return nil, err
    }

    return db, nil
}

func initRabbitMQ(ctx context.Context, rabbitMQURL, queueName string) (*amqp.Channel, *amqp.Queue, error) {
    conn, err := amqp.Dial(rabbitMQURL)
    if err != nil {
        return nil, nil, err
    }

    ch, err := conn.Channel()
    if err != nil {
        return nil, nil, err
    }

    q, err := ch.QueueDeclare(
        queueName,
        true,  // durable
        false, // delete when unused
        false, // exclusive
        false, // no-wait
        nil,   // arguments
    )
    if err != nil {
        return nil, nil, err
    }

    return ch, &q, nil
}

func processMessage(ctx context.Context, db *sql.DB, msg []byte) error {
    var logMsg LogMessage
    if err := json.Unmarshal(msg, &logMsg); err != nil {
        return err
    }

    additionalJSON, err := json.Marshal(logMsg.Additional)
    if err != nil {
        return err
    }

    _, err = db.ExecContext(ctx,
        `INSERT INTO logs (
            timestamp, level, service, message, caller, 
            stack_trace, trace_id, request_id, user_id, additional
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
        logMsg.Timestamp,
        logMsg.Level,
        logMsg.Service,
        logMsg.Message,
        logMsg.Caller,
        logMsg.StackTrace,
        logMsg.TraceID,
        logMsg.RequestID,
        logMsg.UserID,
        additionalJSON,
    )
    return err
}

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    config := getConfig()

    // Initialize PostgreSQL
    db, err := initDB(ctx, config.PostgresURL)
    if err != nil {
        log.Fatalf("Failed to initialize database: %v", err)
    }
    defer db.Close()

    // Initialize RabbitMQ
    ch, q, err := initRabbitMQ(ctx, config.RabbitMQURL, config.QueueName)
    if err != nil {
        log.Fatalf("Failed to initialize RabbitMQ: %v", err)
    }
    defer ch.Close()

    msgs, err := ch.Consume(
        q.Name,
        "",    // consumer
        false, // auto-ack
        false, // exclusive
        false, // no-local
        false, // no-wait
        nil,   // args
    )
    if err != nil {
        log.Fatalf("Failed to register a consumer: %v", err)
    }

    // Handle graceful shutdown
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

    go func() {
        for msg := range msgs {
            if err := processMessage(ctx, db, msg.Body); err != nil {
                log.Printf("Error processing message: %v", err)
                msg.Nack(false, true) // Negative acknowledgement, requeue
                continue
            }
            msg.Ack(false) // Acknowledge message
        }
    }()

    log.Printf("Ledger service is running. To exit press CTRL+C")
    <-sigChan
    log.Printf("Shutting down gracefully...")
}