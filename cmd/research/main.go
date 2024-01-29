package main

import (
    "context"
    "fmt"
    "log"
    "strings"
    "database/sql"
    "math/big"
    "time"
    "os"

    "github.com/ethereum/go-ethereum"
    "github.com/ethereum/go-ethereum/accounts/abi"
    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/ethclient"
    "github.com/ethereum/go-ethereum/core/types"
    "github.com/ethereum/go-ethereum/crypto"
    _ "github.com/lib/pq"
)

type EventData struct {
    Time      time.Time
    Sender    string
    Recipient string
    Amount0   big.Int
    Amount1   big.Int
}

const (
    contractAddress = "0x4e68ccd3e89f51c3074ca5072bbac773960dfa36"

    // TimescaleDB connection parameters
    dbHost     = "localhost"
    dbPort     = 5432
    dbUser     = "postgres"
    dbPassword = "mypassword"
    dbName     = "postgres"
)

const ContractABI = `[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount0","type":"uint128"},{"indexed":false,"internalType":"uint128","name":"amount1","type":"uint128"}],"name":"Collect","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"uint128","name":"amount0","type":"uint128"},{"indexed":false,"internalType":"uint128","name":"amount1","type":"uint128"}],"name":"CollectProtocol","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"paid0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"paid1","type":"uint256"}],"name":"Flash","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint16","name":"observationCardinalityNextOld","type":"uint16"},{"indexed":false,"internalType":"uint16","name":"observationCardinalityNextNew","type":"uint16"}],"name":"IncreaseObservationCardinalityNext","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"internalType":"int24","name":"tick","type":"int24"}],"name":"Initialize","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint8","name":"feeProtocol0Old","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol1Old","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol0New","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol1New","type":"uint8"}],"name":"SetFeeProtocol","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"int256","name":"amount0","type":"int256"},{"indexed":false,"internalType":"int256","name":"amount1","type":"int256"},{"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"internalType":"uint128","name":"liquidity","type":"uint128"},{"indexed":false,"internalType":"int24","name":"tick","type":"int24"}],"name":"Swap","type":"event"},{"inputs":[{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount","type":"uint128"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount0Requested","type":"uint128"},{"internalType":"uint128","name":"amount1Requested","type":"uint128"}],"name":"collect","outputs":[{"internalType":"uint128","name":"amount0","type":"uint128"},{"internalType":"uint128","name":"amount1","type":"uint128"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint128","name":"amount0Requested","type":"uint128"},{"internalType":"uint128","name":"amount1Requested","type":"uint128"}],"name":"collectProtocol","outputs":[{"internalType":"uint128","name":"amount0","type":"uint128"},{"internalType":"uint128","name":"amount1","type":"uint128"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"fee","outputs":[{"internalType":"uint24","name":"","type":"uint24"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeGrowthGlobal0X128","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeGrowthGlobal1X128","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"flash","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"}],"name":"increaseObservationCardinalityNext","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"liquidity","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"maxLiquidityPerTick","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount","type":"uint128"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"mint","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"observations","outputs":[{"internalType":"uint32","name":"blockTimestamp","type":"uint32"},{"internalType":"int56","name":"tickCumulative","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityCumulativeX128","type":"uint160"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint32[]","name":"secondsAgos","type":"uint32[]"}],"name":"observe","outputs":[{"internalType":"int56[]","name":"tickCumulatives","type":"int56[]"},{"internalType":"uint160[]","name":"secondsPerLiquidityCumulativeX128s","type":"uint160[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"positions","outputs":[{"internalType":"uint128","name":"liquidity","type":"uint128"},{"internalType":"uint256","name":"feeGrowthInside0LastX128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthInside1LastX128","type":"uint256"},{"internalType":"uint128","name":"tokensOwed0","type":"uint128"},{"internalType":"uint128","name":"tokensOwed1","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"protocolFees","outputs":[{"internalType":"uint128","name":"token0","type":"uint128"},{"internalType":"uint128","name":"token1","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint8","name":"feeProtocol0","type":"uint8"},{"internalType":"uint8","name":"feeProtocol1","type":"uint8"}],"name":"setFeeProtocol","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"slot0","outputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"internalType":"int24","name":"tick","type":"int24"},{"internalType":"uint16","name":"observationIndex","type":"uint16"},{"internalType":"uint16","name":"observationCardinality","type":"uint16"},{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"},{"internalType":"uint8","name":"feeProtocol","type":"uint8"},{"internalType":"bool","name":"unlocked","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"}],"name":"snapshotCumulativesInside","outputs":[{"internalType":"int56","name":"tickCumulativeInside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityInsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsInside","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"bool","name":"zeroForOne","type":"bool"},{"internalType":"int256","name":"amountSpecified","type":"int256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"swap","outputs":[{"internalType":"int256","name":"amount0","type":"int256"},{"internalType":"int256","name":"amount1","type":"int256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"int16","name":"","type":"int16"}],"name":"tickBitmap","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"tickSpacing","outputs":[{"internalType":"int24","name":"","type":"int24"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"int24","name":"","type":"int24"}],"name":"ticks","outputs":[{"internalType":"uint128","name":"liquidityGross","type":"uint128"},{"internalType":"int128","name":"liquidityNet","type":"int128"},{"internalType":"uint256","name":"feeGrowthOutside0X128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthOutside1X128","type":"uint256"},{"internalType":"int56","name":"tickCumulativeOutside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityOutsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsOutside","type":"uint32"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]` // Replace with your full ABI

func insertEvent(db *sql.DB, eventData EventData) error {
    // Assuming eventData is a struct that holds your event data
    sqlStatement := `
        INSERT INTO uniswap_events (time, sender, recipient, amount0, amount1)
        VALUES ($1, $2, $3, $4, $5)`
    
    _, err := db.Exec(sqlStatement, eventData.Time, eventData.Sender, eventData.Recipient, eventData.Amount0.String(), eventData.Amount1.String())
    return err
}

func createTableIfNotExist(db *sql.DB) error {
    createTableQuery := `
    CREATE TABLE IF NOT EXISTS uniswap_events (
        time TIMESTAMP NOT NULL,
        sender VARCHAR(42) NOT NULL,
        recipient VARCHAR(42) NOT NULL,
        amount0 NUMERIC NOT NULL,
        amount1 NUMERIC NOT NULL,
        PRIMARY KEY (time, sender, recipient)
    );`
    _, err := db.Exec(createTableQuery)
    if err != nil {
        return err
    }

    createHypertableQuery := `SELECT create_hypertable('uniswap_events', 'time');`
    _, err = db.Exec(createHypertableQuery)
    return err
}

func installTimescaleDBExtension(db *sql.DB) error {
    query := `CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;`
    _, err := db.Exec(query)
    return err
}

func readEvents(db *sql.DB) {
    rows, err := db.Query("SELECT time, sender, recipient, amount0, amount1 FROM uniswap_events ORDER BY time DESC LIMIT 10") // Adjust query as needed
    if err != nil {
        log.Printf("Error reading events: %v", err)
        return
    }
    defer rows.Close()

    for rows.Next() {
        var time time.Time
        var sender, recipient string
        var amount0, amount1 string // Assuming amounts are stored as strings

        err := rows.Scan(&time, &sender, &recipient, &amount0, &amount1)
        if err != nil {
            log.Printf("Error scanning row: %v", err)
            continue
        }

        log.Printf("Event - Time: %s, Sender: %s, Recipient: %s, Amount0: %s, Amount1: %s", time, sender, recipient, amount0, amount1)
    }

    // Check for errors from iterating over rows
    if err = rows.Err(); err != nil {
        log.Printf("Error iterating rows: %v", err)
    }
}

func main() {
    infuraURL := os.Getenv("INFURA_URL")
    if infuraURL == "" {
        log.Fatal("INFURA_URL environment variable is not set")
    }

    // Setup database connection
    db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", dbHost, dbPort, dbUser, dbPassword, dbName))
    if err != nil {
        log.Fatalf("Failed to connect to the database: %v", err)
    }
    defer db.Close()

    // Test the database connection
    if err := db.Ping(); err != nil {
        log.Fatalf("Failed to ping the database: %v", err)
    }
    fmt.Println("Successfully connected to the database")
    // Create the table if it doesn't exist
    if err := createTableIfNotExist(db); err != nil {
        log.Fatalf("Failed to create table: %v", err)
    }

    // Install TimescaleDB extension
    if err := installTimescaleDBExtension(db); err != nil {
        log.Fatalf("Failed to install TimescaleDB extension: %v", err)
    }

    // Start a Go routine to read events every 30 seconds
    go func() {
        ticker := time.NewTicker(30 * time.Second)
        defer ticker.Stop()

        for {
            select {
            case <-ticker.C:
                readEvents(db)
            }
        }
    }()

    client, err := ethclient.Dial(infuraURL)
    if err != nil {
        log.Fatalf("Failed to connect to the Ethereum client: %v", err)
    }

    contractAbi, err := abi.JSON(strings.NewReader(ContractABI))
    if err != nil {
        log.Fatalf("Failed to parse contract ABI: %v", err)
    }

    address := common.HexToAddress(contractAddress)
    swapEventSignature := []byte("Swap(address,address,int256,int256,uint160,uint128,int24)")
    swapEventTopic := crypto.Keccak256Hash(swapEventSignature)
    query := ethereum.FilterQuery{
        Addresses: []common.Address{address},
        Topics:    [][]common.Hash{{swapEventTopic}},
    }

    logs := make(chan types.Log)
    sub, err := client.SubscribeFilterLogs(context.Background(), query, logs)
    if err != nil {
        log.Fatal(err)
    }

    // Event listening loop
    for {
        select {
        case err := <-sub.Err():
            log.Fatal(err)
        case vLog := <-logs:
            log.Printf("Received event log: %v", vLog)

            // Log the raw data
            log.Printf("Raw Data: %x", vLog.Data)
            log.Printf("Topics: %v", vLog.Topics)

            // Check for sufficient topics
            if len(vLog.Topics) < 3 {
                log.Fatal("Insufficient topics in log")
            }

            event, err := contractAbi.Unpack("Swap", vLog.Data)
            if err != nil {
                log.Printf("Error unpacking event: %v", err)
                continue
            }

            log.Printf("Event received: %v", event)

            // Check if the length of the event slice matches the expected number of fields
            if len(event) != 5 { // Adjust the number based on the expected fields
                log.Printf("Invalid event data length: got %d, want 5", len(event))
                continue
            }
            // Extract indexed parameters
            sender := common.HexToAddress(vLog.Topics[1].Hex())
            recipient := common.HexToAddress(vLog.Topics[2].Hex())

            amount0, ok := event[2].(*big.Int)
            if !ok {
                log.Fatal("Invalid amount0 type")
            }

            amount1, ok := event[3].(*big.Int)
            if !ok {
                log.Fatal("Invalid amount1 type")
            }

            // Extract other fields if needed

            eventData := EventData{
                Time:      time.Now(),
                Sender:    sender.Hex(),
                Recipient: recipient.Hex(),
                Amount0:   *amount0,
                Amount1:   *amount1,
            }
            
            if err := insertEvent(db, eventData); err != nil {
              log.Fatalf("Failed to insert event: %v", err)
            }
        }
    }
}

