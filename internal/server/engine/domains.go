package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/larsla/takt/api"
)

// Domain is a stored domain record.
type Domain struct {
	Name          string            `json:"name"`
	UUID          string            `json:"uuid"`
	Description   string            `json:"description,omitempty"`
	OwnerEmail    string            `json:"ownerEmail,omitempty"`
	Data          map[string]string `json:"data,omitempty"`
	RetentionDays int32             `json:"retentionDays"`
	Status        api.DomainStatus  `json:"status"`
	CreatedAt     int64             `json:"createdAt"`
}

const defaultRetentionDays = 3

func (d *Domain) describe() *api.DescribeDomainResponse {
	return &api.DescribeDomainResponse{
		DomainInfo: &api.DomainInfo{
			Name:        d.Name,
			Status:      d.Status,
			Description: d.Description,
			OwnerEmail:  d.OwnerEmail,
			Data:        d.Data,
			UUID:        d.UUID,
		},
		Configuration: &api.DomainConfiguration{WorkflowExecutionRetentionPeriodInDays: d.RetentionDays},
	}
}

func (e *Engine) RegisterDomain(ctx context.Context, req *api.RegisterDomainRequest) error {
	if req.Name == "" {
		return badRequest("domain name is not set")
	}
	d := &Domain{
		Name:          req.Name,
		UUID:          uuid.NewString(),
		Description:   req.Description,
		OwnerEmail:    req.OwnerEmail,
		Data:          req.Data,
		RetentionDays: req.WorkflowExecutionRetentionPeriodInDays,
		Status:        api.DomainStatusRegistered,
		CreatedAt:     e.nowNano(),
	}
	if d.RetentionDays <= 0 {
		d.RetentionDays = defaultRetentionDays
	}
	data, _ := json.Marshal(d)
	_, err := e.domains.Create(ctx, api.Token(req.Name), data)
	if isConflict(err) {
		return &api.DomainAlreadyExistsError{Message: "domain already exists: " + req.Name}
	}
	return err
}

func (e *Engine) domain(ctx context.Context, name string) (*Domain, uint64, error) {
	if name == "" {
		return nil, 0, badRequest("domain is not set")
	}
	entry, err := e.domains.Get(ctx, api.Token(name))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, 0, notFound("domain %s does not exist", name)
	}
	if err != nil {
		return nil, 0, err
	}
	var d Domain
	if err := json.Unmarshal(entry.Value(), &d); err != nil {
		return nil, 0, err
	}
	return &d, entry.Revision(), nil
}

func (e *Engine) DescribeDomain(ctx context.Context, req *api.DescribeDomainRequest) (*api.DescribeDomainResponse, error) {
	d, _, err := e.domain(ctx, req.Name)
	if err != nil {
		return nil, err
	}
	return d.describe(), nil
}

func (e *Engine) UpdateDomain(ctx context.Context, req *api.UpdateDomainRequest) (*api.UpdateDomainResponse, error) {
	for range e.maxRetry {
		d, rev, err := e.domain(ctx, req.Name)
		if err != nil {
			return nil, err
		}
		if u := req.UpdatedInfo; u != nil {
			if u.Description != nil {
				d.Description = *u.Description
			}
			if u.OwnerEmail != nil {
				d.OwnerEmail = *u.OwnerEmail
			}
			if u.Data != nil {
				if d.Data == nil {
					d.Data = map[string]string{}
				}
				for k, v := range u.Data {
					d.Data[k] = v
				}
			}
		}
		if c := req.Configuration; c != nil && c.WorkflowExecutionRetentionPeriodInDays > 0 {
			d.RetentionDays = c.WorkflowExecutionRetentionPeriodInDays
		}
		data, _ := json.Marshal(d)
		_, err = e.domains.Update(ctx, api.Token(req.Name), data, rev)
		if isConflict(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return d.describe(), nil
	}
	return nil, &api.ServiceBusyError{Message: "too much contention on domain " + req.Name}
}

func (e *Engine) ListDomains(ctx context.Context, _ *api.ListDomainsRequest) (*api.ListDomainsResponse, error) {
	ds, err := e.allDomains(ctx)
	if err != nil {
		return nil, err
	}
	resp := &api.ListDomainsResponse{Domains: []*api.DescribeDomainResponse{}}
	for _, d := range ds {
		resp.Domains = append(resp.Domains, d.describe())
	}
	return resp, nil
}

func (e *Engine) allDomains(ctx context.Context) ([]*Domain, error) {
	lister, err := e.domains.ListKeys(ctx)
	if err != nil {
		return nil, err
	}
	var out []*Domain
	for k := range lister.Keys() {
		entry, err := e.domains.Get(ctx, k)
		if err != nil {
			continue
		}
		var d Domain
		if json.Unmarshal(entry.Value(), &d) == nil {
			out = append(out, &d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
