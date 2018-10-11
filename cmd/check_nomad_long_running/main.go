package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/nomad/api"
	"github.com/jasonhancock/go-nagios"
)

func main() {
	fs := flag.CommandLine
	p := nagios.NewPlugin("nomad-unplaceable", fs)

	p.StringFlag("addr", "http://127.0.0.1:4646", "The address of the Nomad server")
	p.StringFlag("tls-cert", "", "TLS certificate to use when connecting to Nomad")
	p.StringFlag("tls-key", "", "TLS key to use when connecting to Nomad")
	p.StringFlag("tls-ca-cert", "", "TLS CA cert to use to validate the Nomad server certificate")
	p.BoolFlag("tls-insecure", false, "Whether or not to validate the server certificate")
	p.StringFlag("days", "2", "If allocation older than X days trigger alert")
	flag.Parse()

	addr := p.OptRequiredString("addr")
	tlsCert, _ := p.OptString("tls-cert")
	tlsKey, _ := p.OptString("tls-key")
	tlsCaCert, _ := p.OptString("tls-ca-cert")
	tlsInsecure, _ := p.OptBool("tls-insecure")
	sdays, _ := p.OptString("days")

	// Convert days to negitive number
	days, err := strconv.Atoi(sdays)
	if err != nil {
		p.Fatal(err)
	}
	days = days * -1

	// Create a nomad client
	cfg := api.DefaultConfig()
	cfg.Address = addr
	if tlsCert != "" && tlsKey != "" {
		cfg.TLSConfig = &api.TLSConfig{
			CACert:     tlsCaCert,
			ClientCert: tlsCert,
			ClientKey:  tlsKey,
			Insecure:   tlsInsecure,
		}
	}
	c, err := api.NewClient(cfg)
	if err != nil {
		p.Fatal(err)
	}

	// Go find long running Jobs
	longRunningAllocs, err := findLongRunningJobs(c, time.Now().AddDate(0, 0, days))
	if err != nil {
		p.Fatal(err)
	}

	code := nagios.OK
	message := "OK - long running jobs"

	if len(longRunningAllocs) > 0 {
		code = nagios.WARNING
		message = fmt.Sprintf("WARNING - Found %d long running jobs", len(longRunningAllocs))
		for _, a := range longRunningAllocs {
			p.Verbose("JobID: ", a.jobID, " Run Start: ", a.allocDate.Format("01/02/2006 15:04:05"), " Status: ", a.allocStatus)
		}
	}

	p.Exit(code, message)
}

type jobAlloc struct {
	jobID       string
	jobDate     time.Time
	allocID     string
	allocDate   time.Time
	allocStatus string
}

// findLongRunningJobs - Look at all running jobs in nomad and pull all the allocation info
func findLongRunningJobs(client *api.Client, thresDate time.Time) ([]jobAlloc, error) {
	var longRunningAlloc []jobAlloc

	// Pull all jobs in nomad
	jobs, _, err := client.Jobs().List(nil)
	if err != nil {
		return nil, err
	}

	// Check all running jobs
	for i := range jobs {
		if jobs[i].Status != "running" {
			continue
		}

		// Pull all the allocations for the job
		allocations, _, err := client.Jobs().Allocations(jobs[i].ID, true, nil)
		if err != nil {
			if strings.Contains(err.Error(), "job not found") {
				continue
			}
			return nil, err
		}

		// Check each allocation date
		for _, alloc := range allocations {
			allocDate := time.Unix(0, alloc.CreateTime)
			if allocDate.Before(thresDate) {
				t := jobAlloc{
					jobID:       jobs[i].ID,
					jobDate:     time.Unix(0, jobs[i].SubmitTime),
					allocID:     alloc.ID,
					allocDate:   allocDate,
					allocStatus: alloc.ClientStatus,
				}
				longRunningAlloc = append(longRunningAlloc, t)
			}
		}
	}

	return longRunningAlloc, nil
}
