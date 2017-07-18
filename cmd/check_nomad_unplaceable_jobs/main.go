package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/hashicorp/nomad/api"
	"github.com/jasonhancock/go-nagios"
	"github.com/pkg/errors"
)

func main() {
	fs := flag.CommandLine
	p := nagios.NewPlugin("nomad-unplaceable", fs)

	p.StringFlag("addr", "http://127.0.0.1:4646", "The address of the Nomad server")
	p.StringFlag("tls-cert", "", "TLS certificate to use when connecting to Nomad")
	p.StringFlag("tls-key", "", "TLS key to use when connecting to Nomad")
	p.StringFlag("tls-ca-cert", "", "TLS CA cert to use to validate the Nomad server certificate")
	p.BoolFlag("tls-insecure", false, "Whether or not to validate the server certificate")
	flag.Parse()

	addr := p.OptRequiredString("addr")
	tlsCert, _ := p.OptString("tls-cert")
	tlsKey, _ := p.OptString("tls-key")
	tlsCaCert, _ := p.OptString("tls-ca-cert")
	tlsInsecure, _ := p.OptBool("tls-insecure")

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

	failingJobs, err := getEvaluations(c)
	if err != nil {
		p.Fatal(err)
	}

	code := nagios.OK
	message := "OK - no unplaceable jobs"

	if len(failingJobs) > 0 {
		nodes, err := getNodes(c)
		if err != nil {
			p.Fatal(err)
		}

		unplaceable := findUnplaceableJobs(nodes, failingJobs)
		if len(unplaceable) > 0 {
			code = nagios.CRITICAL
			message = fmt.Sprintf("CRITICAL - %d unplaceable jobs", len(unplaceable))
			for _, v := range unplaceable {
				p.Verbose("type=unplaceable_job", v)
			}
		}
	}

	p.Exit(code, message)
}

func findUnplaceableJobs(nodes []node, jobs []failingJob) []failingJob {
	var unplaceableJobs []failingJob
	for j := range jobs {
		var unplaceable = true
		for n := range nodes {
			if nodes[n].resources.CPU >= jobs[j].resources.CPU &&
				nodes[n].resources.MemoryMB >= jobs[j].resources.MemoryMB &&
				nodes[n].resources.DiskMB >= jobs[j].resources.DiskMB {
				unplaceable = false
				break
			}
		}

		if unplaceable {
			unplaceableJobs = append(unplaceableJobs, jobs[j])
		}
	}

	return unplaceableJobs
}

func getNodes(client *api.Client) ([]node, error) {
	nodes, _, err := client.Nodes().List(nil)
	if err != nil {
		return nil, err
	}
	mynodes := make([]node, 0, len(nodes))

	for i := range nodes {
		resources, err := getResourcesForNode(client, nodes[i].ID)
		if err != nil {
			return nil, err
		}
		mynodes = append(mynodes, node{
			ID:        nodes[i].ID,
			resources: resources,
		})
	}

	return mynodes, nil
}

type node struct {
	ID        string
	resources *api.Resources
}

func (n node) String() string {
	return fmt.Sprintf("node_id=%s cpu=%d memory=%d disk=%d", n.ID, n.resources.CPU, n.resources.MemoryMB, n.resources.DiskMB)
}

type failingJob struct {
	jobID     string
	dimension string
	resources *api.Resources
}

func (j failingJob) String() string {
	return fmt.Sprintf("job_id=%s dimension=%q cpu=%d memory=%d disk=%d", j.jobID, j.dimension, j.resources.CPU, j.resources.MemoryMB, j.resources.DiskMB)
}

func getEvaluations(client *api.Client) ([]failingJob, error) {
	var failingJobs []failingJob
	evals, _, err := client.Evaluations().List(nil)

	if err != nil {
		return nil, err
	}

	for i := range evals {
		if len(evals[i].FailedTGAllocs) == 0 {
			continue
		}

		jobStatus, resources, err := getResourcesForJob(client, evals[i].JobID)
		if err != nil {
			return nil, err
		}

		if jobStatus != "pending" {
			continue
		}

		for _, v := range evals[i].FailedTGAllocs {
			for dim, _ := range v.DimensionExhausted {
				job := failingJob{
					jobID:     evals[i].JobID,
					dimension: dim,
					resources: resources,
				}
				failingJobs = append(failingJobs, job)
			}
		}
	}

	return failingJobs, nil
}

// getResourcesForJob sums up all the resources from all Tasks in all Task Groups in a job
func getResourcesForJob(client *api.Client, jobID string) (string, *api.Resources, error) {
	job, _, err := client.Jobs().Info(jobID, nil)
	if err != nil {
		if strings.Contains(err.Error(), "job not found") {
			return "deleted", nil, nil
		}
		return "", nil, err
	}

	var total api.Resources
	for _, tg := range job.TaskGroups {
		if tg.EphemeralDisk != nil {
			total.DiskMB += tg.EphemeralDisk.SizeMB
		}
		for _, t := range tg.Tasks {
			total.CPU += t.Resources.CPU
			total.MemoryMB += t.Resources.MemoryMB
			total.DiskMB += t.Resources.DiskMB
		}
	}

	return job.Status, &total, nil
}

func getResourcesForNode(client *api.Client, id string) (*api.Resources, error) {
	node, _, err := client.Nodes().Info(id, nil)

	if err != nil {
		return nil, errors.Wrap(err, "querying node")
	}

	// Total available resources
	total := &api.Resources{}

	r := node.Resources
	res := node.Reserved
	if res == nil {
		res = &api.Resources{}
	}
	total.CPU = r.CPU - res.CPU
	total.MemoryMB = r.MemoryMB - res.MemoryMB
	total.DiskMB = r.DiskMB - res.DiskMB
	total.IOPS = r.IOPS - res.IOPS

	return total, nil
}
