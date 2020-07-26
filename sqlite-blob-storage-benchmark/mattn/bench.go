package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func prepareFiles(dir string, nFiles, minSize, maxSize int) error {
	for i := 0; i < nFiles; i++ {
		size := minSize + rand.Intn(maxSize-minSize)
		fName := fmt.Sprintf("%d.bin", i)
		f, err := os.OpenFile(filepath.Join(dir, fName), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		defer f.Close()

		// write in chunks of 8M to save RAM
		chunkSize := 8 * 1024 * 1024
		chunk := make([]byte, chunkSize)
		for size > 0 {
			n := chunkSize
			if size < chunkSize {
				n = size
			}
			_, err = rand.Read(chunk[:n])
			if err != nil {
				return err
			}
			size -= n
			_, err = f.Write(chunk[:n])
			if err != nil {
				return err
			}
		}
		fmt.Println("created testfile:", f.Name())
	}
	return nil
}

func prepareDatabase(db *sql.DB, dir string, nFiles int) error {
	_, err := db.ExecContext(context.TODO(), `CREATE TABLE files (name TEXT NOT NULL PRIMARY KEY, data BLOB)`)
	if err != nil {
		return err
	}
	tFiles, err := filepath.Glob(dir + "/*.bin")
	if err != nil {
		return err
	}
	if len(tFiles) != nFiles {
		return fmt.Errorf("expected %d test files but was %d", nFiles, len(tFiles))
	}
	for _, tFile := range tFiles {
		data, err := ioutil.ReadFile(tFile)
		if err != nil {
			return err
		}
		_, err = db.ExecContext(context.TODO(), `INSERT INTO files VALUES(?, ?)`, filepath.Base(tFile), data)
		if err != nil {
			return err
		}
		fmt.Printf("stored %s in database\n", tFile)
	}

	return nil
}

func selectFile(ctx context.Context, db *sql.DB, filename string) error {
	row := db.QueryRowContext(ctx, "SELECT data FROM files WHERE name = ?", filename)
	var data []byte
	err := row.Scan(&data)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return fmt.Errorf("sqlite: empty file %s", filename)
	}
	return nil
}

func readFile(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	n, err := io.Copy(ioutil.Discard, f)
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("empty file %s", f.Name())
	}
	return nil
}

func run(nFiles, minSize, maxSize, concurrency int) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	tDir, err := ioutil.TempDir(wd, "sqlite-bench-*")
	if err != nil {
		return err
	}
	dbFile, err := ioutil.TempFile(wd, "sqlite-bench-*.db")
	if err != nil {
		return err
	}

	defer func() {
		fmt.Println("removing testfiles...")
		err = os.Remove(dbFile.Name())
		if err != nil {
			log.Fatal(err)
		}
		err = os.RemoveAll(tDir)
		if err != nil {
			log.Fatal(err)
		}
	}()

	rand.Seed(time.Now().UnixNano())
	err = prepareFiles(tDir, nFiles, minSize, maxSize)
	if err != nil {
		return err
	}

	db, err := sql.Open("sqlite3", dbFile.Name())
	if err != nil {
		return err
	}
	defer db.Close()
	err = prepareDatabase(db, tDir, nFiles)
	if err != nil {
		return err
	}

	resultCh := make(chan time.Duration)
	errCh := make(chan error)
	doneCh := make(chan interface{})

	for i := 0; i < concurrency; i++ {
		go func() {
			for j := 0; j < nFiles; j++ {
				start := time.Now()
				filename := fmt.Sprintf("%d.bin", j%nFiles)

				err = readFile(filepath.Join(tDir, filename))
				if err != nil {
					errCh <- err
				}
				resultCh <- time.Since(start)
			}
			doneCh <- nil
		}()
	}
	routinesRunning := concurrency
	var fileTimes []time.Duration
	for routinesRunning > 0 {
		select {
		case t := <-resultCh:
			fileTimes = append(fileTimes, t)
		case <-doneCh:
			routinesRunning--
		case err := <-errCh:
			return err
		}
	}

	for i := 0; i < concurrency; i++ {
		go func() {
			for j := 0; j < nFiles; j++ {
				start := time.Now()
				filename := fmt.Sprintf("%d.bin", j%nFiles)

				err = selectFile(context.TODO(), db, filename)
				if err != nil {
					errCh <- err
				}
				resultCh <- time.Since(start)
			}
			doneCh <- nil
		}()
	}
	routinesRunning = concurrency
	var sqlTimes []time.Duration
	for routinesRunning > 0 {
		select {
		case t := <-resultCh:
			sqlTimes = append(sqlTimes, t)
		case <-doneCh:
			routinesRunning--
		case err := <-errCh:
			return err
		}
	}
	var totalFiles time.Duration
	for _, v := range fileTimes {
		totalFiles += v
	}
	var totalSQL time.Duration
	for _, v := range sqlTimes {
		totalSQL += v
	}
	fmt.Printf("total: files: %s\tsql: %s\n", totalFiles, totalSQL)

	return nil
}

func main() {
	if len(os.Args) != 5 {
		log.Fatalf("USAGE: %s <nr-of-files> <min-size> <max-size> <concurrency>", os.Args[0])
	}
	mustAtoi := func(s string) int {
		i, err := strconv.Atoi(s)
		if err != nil {
			log.Fatal(err)
		}
		return i
	}
	nFiles := mustAtoi(os.Args[1])
	minSize, maxSize := mustAtoi(os.Args[2]), mustAtoi(os.Args[3])
	if minSize >= maxSize {
		log.Fatalf("minSize %d must be less than maxSize %d", minSize, maxSize)
	}
	concurrency := mustAtoi(os.Args[4])

	err := run(nFiles, minSize, maxSize, concurrency)
	if err != nil {
		log.Fatal(err)
	}
}
