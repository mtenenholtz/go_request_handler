package go_request_handler

import(
	"log"
	"net/http"
	"encoding/json"
	"io"
)

// These values would typically be initialized by environmental variables
var (
	WorkerLimit = 0
	QueueLimit = 0
)

// Payload is the request to be sent to the database. Reconfigure variables as necessary
type Payload struct {
	Data []byte
}

// A job struct contains the specific job to be done
type Job struct {
	Payload Payload
}

var JobQueue chan Job

// A worker carries out the job as part of a WorkerPool, and also has a quit channel as a stop
type Worker struct {
	JobChan chan Job
	WorkerPool chan chan Job
	quit chan bool
}

func SpawnWorker(pool chan chan Job) Worker {
	return Worker{
		JobChan: make(chan Job),
		WorkerPool: pool,
		quit: make(chan bool),
	}
}

func (worker Worker) Work() {
	go func() {
		for {
			// Add worker into the pool
			worker.WorkerPool <- worker.JobChan

			select {
			// Replace body of this case block with logic to process payload
			case job := <-worker.JobChan:
				_ := job.Payload
				log.Print("Job received.")

			// If this case is reached, it means the job was signaled to stop. If there is any necessary teardown,
			// do it here.
			case <-worker.quit:
				return
			}
		}
	}()
}

func (worker Worker) Stop() {
	go func() {
		worker.quit <- true
	}()
}

func payloadHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Read the body into a string for json decoding
	var content = &PayloadCollection{}
	err := json.NewDecoder(io.LimitReader(r.Body, MaxLength)).Decode(&content)
	if err != nil {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Go through each payload and queue items individually to be posted to S3
	for _, payload := range content.Payloads {

		// let's create a job with the payload
		work := Job{Payload: payload}

		// Push the work onto the queue.
		JobQueue <- work
	}

	w.WriteHeader(http.StatusOK)
}

