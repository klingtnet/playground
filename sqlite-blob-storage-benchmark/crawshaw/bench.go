package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"crawshaw.io/sqlite/sqlitex"
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

func prepareDatabase(ctx context.Context, dbPool *sqlitex.Pool, dir string, nFiles int) error {
	conn := dbPool.Get(ctx)
	if conn == nil {
		return fmt.Errorf("no connection in pool available")
	}
	defer dbPool.Put(conn)
	stmt, err := conn.Prepare(`CREATE TABLE files (name TEXT NOT NULL PRIMARY KEY, data BLOB);`)
	if err != nil {
		return err
	}
	_, err = stmt.Step()
	if err != nil {
		return fmt.Errorf("CREATE stmt.Step: %w", err)
	}

	tFiles, err := filepath.Glob(dir + "/*.bin")
	if err != nil {
		return err
	}
	if len(tFiles) != nFiles {
		return fmt.Errorf("expected %d test files but was %d", nFiles, len(tFiles))
	}
	for _, tFile := range tFiles {
		err = insertFile(ctx, dbPool, tFile)
		if err != nil {
			return err
		}

		fmt.Printf("stored %s in database\n", tFile)
	}

	return nil
}

func insertFile(ctx context.Context, dbPool *sqlitex.Pool, name string) error {
	conn := dbPool.Get(ctx)
	if conn == nil {
		return fmt.Errorf("no connection in pool available")
	}
	defer dbPool.Put(conn)

	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	fInfo, err := f.Stat()
	if err != nil {
		return err
	}

	stmt, err := conn.Prepare(`INSERT INTO files (name, data) VALUES($name, $data);`)
	if err != nil {
		return err
	}
	stmt.SetText("$name", filepath.Base(name))
	stmt.SetZeroBlob("$data", fInfo.Size())
	_, err = stmt.Step()
	if err != nil {
		return fmt.Errorf("INSERT stmt.Step: %w", err)
	}
	blob, err := conn.OpenBlob("", "files", "data", conn.LastInsertRowID(), true)
	if err != nil {
		return fmt.Errorf("conn.OpenBlob: %w", err)
	}
	defer blob.Close()
	n, err := io.Copy(blob, f)
	if err != nil {
		return fmt.Errorf("io.Copy: %w", err)
	}
	if n != fInfo.Size() {
		return fmt.Errorf("expected %d bytes to be written but was %d", fInfo.Size(), n)
	}
	return nil
}

func selectFile(ctx context.Context, dbPool *sqlitex.Pool, filename string) error {
	conn := dbPool.Get(context.TODO())
	if conn == nil {
		return fmt.Errorf("no connection in pool available")
	}
	defer dbPool.Put(conn)

	stmt, err := conn.Prepare(`SELECT data FROM files WHERE name = $name`)
	if err != nil {
		return err
	}
	stmt.SetText("$name", filename)
	ok, err := stmt.Step()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("sqlite-crawshaw no data for filename: %s", filename)
	}
	n, err := io.Copy(ioutil.Discard, stmt.GetReader("data"))
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("sqlite-crawshaw empty file")
	}
	ok, err = stmt.Step()
	if ok {
		return fmt.Errorf("stmt.Step expected no more data")
	}
	if err != nil {
		return err
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
	tDir, err := ioutil.TempDir(wd, "sqlite-crawshaw-bench-*")
	if err != nil {
		return err
	}
	dbFile, err := ioutil.TempFile(wd, "sqlite-crawshaw-bench-*.db")
	if err != nil {
		return err
	}
	defer dbFile.Close()

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

	dbPool, err := sqlitex.Open(dbFile.Name(), 0, 10)
	if err != nil {
		log.Fatal(err)
	}
	defer dbPool.Close()
	err = prepareDatabase(context.TODO(), dbPool, tDir, nFiles)
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

				err = selectFile(context.TODO(), dbPool, filename)
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
