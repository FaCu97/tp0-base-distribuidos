package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/op/go-logging"
	"github.com/pkg/errors"
	"github.com/spf13/viper"

	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/common"
	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/domain"
)

var log = logging.MustGetLogger("log")

// InitConfig Function that uses viper library to parse configuration parameters.
// Viper is configured to read variables from both environment variables and the
// config file ./config.yaml. Environment variables takes precedence over parameters
// defined in the configuration file. If some of the variables cannot be parsed,
// an error is returned
func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("id")
	v.BindEnv("server", "address")
	v.BindEnv("loop", "period")
	v.BindEnv("loop", "amount")
	v.BindEnv("log", "level")
	v.BindEnv("batch", "maxAmount")

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	// Parse time.Duration variables and return an error if those variables cannot be parsed

	if _, err := time.ParseDuration(v.GetString("loop.period")); err != nil {
		return nil, errors.Wrapf(err, "Could not parse CLI_LOOP_PERIOD env var as time.Duration.")
	}

	return v, nil
}

// InitLogger Receives the log level to be set in go-logging as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05} %{level:.5s}     %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(baseBackend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}
	backendLeveled.SetLevel(logLevelCode, "")

	// Set the backends to be used.
	logging.SetBackend(backendLeveled)
	return nil
}

// PrintConfig Print all the configuration parameters of the program.
// For debugging purposes only
func PrintConfig(v *viper.Viper) {
	log.Infof("action: config | result: success | client_id: %s | server_address: %s | loop_amount: %v | loop_period: %v | log_level: %s",
		v.GetString("id"),
		v.GetString("server.address"),
		v.GetInt("loop.amount"),
		v.GetDuration("loop.period"),
		v.GetString("log.level"),
	)
}

func LoadBetsFromCSV(filename string) ([]domain.Bet, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error al abrir el archivo: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	bets := make([]domain.Bet, 0)
	lineNumber := 0

	for {
		record, err := reader.Read()
		lineNumber++

		if err == io.EOF {
			break
		}
		if err != nil {
			log.Warningf("action: load_bet | result: fail | line: %d | error: %v", lineNumber, err)
			continue
		}

		bet, parseErr := domain.NewBetFromCSV(record)
		if parseErr != nil {
			log.Warningf("action: load_bet | result: fail | line: %d | error: %v", lineNumber, parseErr)
			continue
		}

		bets = append(bets, bet)
	}

	if len(bets) == 0 {
		return nil, fmt.Errorf("no hay apuestas validas en %s", filename)
	}

	log.Infof("action: load_bets | result: success | file: %s | amount: %d", filename, len(bets))
	return bets, nil
}

func main() {
	v, err := InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Criticalf("%s", err)
	}

	// Print program config with debugging purposes
	PrintConfig(v)

	bets, err := LoadBetsFromCSV("agency.csv")
	if err != nil {
		log.Criticalf("%s", err)
	}

	commonBets := make([]common.Bet, 0, len(bets))
	for _, bet := range bets {
		commonBets = append(commonBets, common.Bet{
			Nombre:     bet.Nombre,
			Apellido:   bet.Apellido,
			Documento:  bet.Documento,
			Nacimiento: bet.Nacimiento,
			Numero:     uint32(bet.Numero),
		})
	}

	clientConfig := common.ClientConfig{
		ServerAddress:  v.GetString("server.address"),
		ID:             v.GetString("id"),
		LoopPeriod:     v.GetDuration("loop.period"),
		Bets:           commonBets,
		BatchMaxAmount: v.GetInt("batch.maxAmount"),
	}

	client := common.NewClient(clientConfig)

	sigtermChan := make(chan os.Signal, 1)
	signal.Notify(sigtermChan, syscall.SIGTERM)
	defer signal.Stop(sigtermChan)

	go func() {
		sig := <-sigtermChan
		log.Infof("action: shutdown_signal | result: success | signal: %s", sig.String())
		client.GracefulShutdown()
	}()

	client.StartClientLoop()
}
