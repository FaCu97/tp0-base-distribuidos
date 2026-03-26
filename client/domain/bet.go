package domain

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	MaxNameBytes    = 30
	MaxSurnameBytes = 30
	DocumentBytes   = 8
	BirthdateLayout = "2006-01-02"
	MinBetNumber    = 0
	MaxBetNumber    = 9999
)

type Bet struct {
	Nombre     string
	Apellido   string
	Documento  string
	Nacimiento string
	Numero     int
}

func NewBet(nombre, apellido, documento, nacimiento string, numero int) (Bet, error) {
	bet := Bet{
		Nombre:     strings.TrimSpace(nombre),
		Apellido:   strings.TrimSpace(apellido),
		Documento:  strings.TrimSpace(documento),
		Nacimiento: strings.TrimSpace(nacimiento),
		Numero:     numero,
	}

	if err := bet.Validate(); err != nil {
		return Bet{}, err
	}

	return bet, nil
}

func NewBetFromCSV(record []string) (Bet, error) {
	if len(record) < 5 {
		return Bet{}, fmt.Errorf("invalid csv record: expected 5 columns, got %d", len(record))
	}

	numero, err := strconv.Atoi(strings.TrimSpace(record[4]))
	if err != nil {
		return Bet{}, fmt.Errorf("invalid numero: %w", err)
	}

	return NewBet(record[0], record[1], record[2], record[3], numero)
}

func (b Bet) Validate() error {
	if b.Nombre == "" {
		return fmt.Errorf("nombre cannot be empty")
	}
	if len([]byte(b.Nombre)) > MaxNameBytes {
		return fmt.Errorf("nombre exceeds %d bytes", MaxNameBytes)
	}

	if b.Apellido == "" {
		return fmt.Errorf("apellido cannot be empty")
	}
	if len([]byte(b.Apellido)) > MaxSurnameBytes {
		return fmt.Errorf("apellido exceeds %d bytes", MaxSurnameBytes)
	}

	if len(b.Documento) != DocumentBytes {
		return fmt.Errorf("documento must have exactly %d digits", DocumentBytes)
	}
	for _, r := range b.Documento {
		if !unicode.IsDigit(r) {
			return fmt.Errorf("documento must contain only digits")
		}
	}

	if _, err := time.Parse(BirthdateLayout, b.Nacimiento); err != nil {
		return fmt.Errorf("nacimiento must match %s: %w", BirthdateLayout, err)
	}

	if b.Numero < MinBetNumber || b.Numero > MaxBetNumber {
		return fmt.Errorf("numero must be between %d and %d", MinBetNumber, MaxBetNumber)
	}

	return nil
}
