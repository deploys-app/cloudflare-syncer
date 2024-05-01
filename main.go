package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/acoshift/configfile"
	"github.com/acoshift/pgsql"
	"github.com/acoshift/pgsql/pgctx"
	"github.com/asaskevich/govalidator"
	"github.com/cloudflare/cloudflare-go"
	"github.com/deploys-app/api"
	"github.com/lib/pq"
	"github.com/samber/lo"
)

func main() {
	cfg := configfile.NewEnvReader()

	slog.Info("start cloudflare-syncer")

	db, err := sql.Open("postgres", cfg.MustString("db_url"))
	if err != nil {
		slog.Error("can not open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(time.Minute)

	cf, err := cloudflare.NewWithAPIToken(cfg.MustString("cloudflare_token"))
	if err != nil {
		slog.Error("can not create cf client", "error", err)
		os.Exit(1)
	}

	w := Worker{
		Client: cf,
		DB:     db,
		ZoneID: cfg.MustString("cloudflare_zone_id"),
		Token:  cfg.MustString("cloudflare_token"),
	}

	chEvent := make(chan struct{})

	ctx := context.Background()

	projectID := cfg.String("project_id")
	var pubSubClient *pubsub.Client
	if projectID != "" { // optional
		pubSubClient, err = pubsub.NewClient(ctx, projectID)
		if err != nil {
			slog.Error("can not create pubsub client", "error", err)
			os.Exit(1)
		}

		const topic = "event"
		const subscription = "cloudflare-deployer.event"

		if pubSubClient != nil {
			defer pubSubClient.Close()

			_, err = pubSubClient.CreateSubscription(ctx, subscription, pubsub.SubscriptionConfig{
				Topic:             pubSubClient.Topic(topic),
				AckDeadline:       10 * time.Second,
				RetentionDuration: time.Hour,
				ExpirationPolicy:  24 * time.Hour,
			})
			if err != nil {
				slog.Info("creating subscription error", "error", err)
			}

			go func() {
				err := pubSubClient.Subscription(subscription).Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
					slog.Info("received event", "data", string(msg.Data))

					msg.Ack()

					select {
					case chEvent <- struct{}{}:
					default:
					}
				})
				if err != nil {
					slog.Error("subscribe failed", "error", err)
					if !cfg.Bool("local") {
						os.Exit(1)
					}
				}
			}()
		}
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM)

	for {
		w.Run()

		select {
		case <-stop:
			return
		case <-time.After(10 * time.Second):
		case <-chEvent:
		}
	}
}

type Worker struct {
	Client *cloudflare.API
	DB     *sql.DB
	ZoneID string
	Token  string

	lastPending   time.Time
	lastCollect   time.Time
	lastStatus    time.Time
	statusRunning uint32
}

func (w *Worker) Run() {
	ctx := context.Background()
	ctx = pgctx.NewContext(ctx, w.DB)

	ds, err := w.getPendingDomains(ctx)
	if err != nil {
		slog.Error("can not get pending domains", "error", err)
		return
	}

	for _, d := range ds {
		slog.Info("processing", "id", d.ID, "location", d.LocationID, "domain", d.Domain, "action", d.Action)

		var err error
		switch d.Action {
		case api.Create:
			err = w.createDomain(ctx, d)
		case api.Delete:
			err = w.deleteDomain(ctx, d)
		}
		if err != nil {
			slog.Error("process error", "error", err)
		}
	}

	if time.Since(w.lastPending) >= 10*time.Second {
		w.lastPending = time.Now()
		w.runStatusVerify()
	}

	if time.Since(w.lastCollect) >= time.Hour {
		w.lastCollect = time.Now()
		w.runCollect(ctx)
		w.summaryProjectUsage(ctx)
	}

	running := atomic.LoadUint32(&w.statusRunning)
	if running == 0 && time.Since(w.lastStatus) > 6*time.Hour {
		w.lastStatus = time.Now()
		go w.runStatus()
	}
}

func (w *Worker) runStatusVerify() {
	ctx := context.Background()
	ctx = pgctx.NewContext(ctx, w.DB)

	ds, err := w.getAllDomainsVerify(ctx)
	if err != nil {
		slog.Error("can not get all domains", "error", err)
		return
	}

	for _, d := range ds {
		if d.Action == api.Delete {
			continue
		}
		slog.Info("status-verify", "id", d.ID, "location", d.LocationID, "domain", d.Domain, "action", d.Action, "status", d.Status)
		time.Sleep(100 * time.Millisecond)

		err = w.updateStatus(ctx, d)
		if err != nil {
			slog.Error("can not update status", "error", err)
		}
	}
}

func (w *Worker) runStatus() {
	atomic.StoreUint32(&w.statusRunning, 1)
	defer atomic.StoreUint32(&w.statusRunning, 0)

	defer slog.Info("status: finished")

	ctx := context.Background()
	ctx = pgctx.NewContext(ctx, w.DB)

	ds, err := w.getAllDomainsForStatus(ctx)
	if err != nil {
		slog.Error("can not get all domains", "error", err)
		return
	}

	for _, d := range ds {
		if d.Action == api.Delete {
			continue
		}
		slog.Info("run status", "id", d.ID, "location", d.LocationID, "domain", d.Domain, "action", d.Action, "status", d.Status)
		time.Sleep(300 * time.Millisecond)

		err = w.updateStatus(ctx, d)
		if err != nil {
			slog.Error("can not update status", "domain", d.Domain, "error", err)
		}
	}
}

func (w *Worker) getLocationOriginServer(ctx context.Context, id string) (string, error) {
	var originServer string
	err := pgctx.QueryRow(ctx, `
		select origin_server
		from locations
		where id = $1
	`, id).Scan(&originServer)
	if err != nil {
		return "", err
	}
	return originServer, nil
}

func (w *Worker) getPendingDomains(ctx context.Context) ([]*domain, error) {
	var xs []*domain
	err := pgctx.Iter(ctx, func(scan pgsql.Scanner) error {
		var x domain
		err := scan(
			&x.ID, &x.ProjectID, &x.LocationID, &x.Domain, &x.Wildcard, &x.Action, &x.Status,
		)
		if err != nil {
			return err
		}
		xs = append(xs, &x)
		return nil
	},
		`
			select id, project_id, location_id, domain, wildcard, action, status
			from domains
			where status = $1 and cdn = true
			order by created_at
		`, api.DomainStatusPending,
	)
	if err != nil {
		return nil, err
	}
	return xs, nil
}

func (w *Worker) getAllDomainsForStatus(ctx context.Context) ([]*domain, error) {
	var xs []*domain
	err := pgctx.Iter(ctx, func(scan pgsql.Scanner) error {
		var x domain
		err := scan(
			&x.ID, &x.ProjectID, &x.LocationID, &x.Domain, &x.Wildcard, &x.Action, &x.Status,
		)
		if err != nil {
			return err
		}
		xs = append(xs, &x)
		return nil
	},
		`
			select id, project_id, location_id, domain, wildcard, action, status
			from domains
			where action = $1 and status = any($2) and cdn = true
			order by created_at
		`, api.Create, pq.Array([]int64{int64(api.DomainStatusSuccess), int64(api.DomainStatusError)}),
	)
	if err != nil {
		return nil, err
	}
	return xs, nil
}

func (w *Worker) getAllDomainsForCollect(ctx context.Context) ([]*domain, error) {
	var xs []*domain
	err := pgctx.Iter(ctx, func(scan pgsql.Scanner) error {
		var x domain
		err := scan(
			&x.ID, &x.ProjectID, &x.LocationID, &x.Domain, &x.Wildcard, &x.Action, &x.Status,
		)
		if err != nil {
			return err
		}
		xs = append(xs, &x)
		return nil
	},
		`
			select id, project_id, location_id, domain, wildcard, action, status
			from domains
			where action = $1 and cdn = true
			order by created_at
		`, api.Create,
	)
	if err != nil {
		return nil, err
	}
	return xs, nil
}

func (w *Worker) getAllDomainsVerify(ctx context.Context) ([]*domain, error) {
	var xs []*domain
	err := pgctx.Iter(ctx, func(scan pgsql.Scanner) error {
		var x domain
		err := scan(
			&x.ID, &x.LocationID, &x.Domain, &x.Wildcard, &x.Action, &x.Status,
		)
		if err != nil {
			return err
		}
		xs = append(xs, &x)
		return nil
	},
		`
			select id, location_id, domain, wildcard, action, status
			from domains
			where action = $1 and cdn = true and (status = $2 or verification->'ssl'->>'pending' = 'true')
			order by created_at
		`, api.Create, api.DomainStatusVerify,
	)
	if err != nil {
		return nil, err
	}
	return xs, nil
}

func (w *Worker) setDomainStatus(ctx context.Context, id int64, status api.DomainStatus) error {
	_, err := pgctx.Exec(ctx, `
		update domains
		set status = $2
		where id = $1
	`, id, status)
	return err
}

func (w *Worker) setDomainVerification(ctx context.Context, id int64, info api.DomainVerification) error {
	_, err := pgctx.Exec(ctx, `
		update domains
		set verification = $2
		where id = $1
	`, id, pgsql.JSON(info))
	return err
}

func (w *Worker) getDomainAction(ctx context.Context, id int64) (api.Action, error) {
	var action api.Action
	err := pgctx.QueryRow(ctx, `
		select action
		from domains
		where id = $1
	`, id).Scan(&action)
	if err != nil {
		return action, err
	}
	return action, nil
}

func (w *Worker) removeDomain(ctx context.Context, id int64) error {
	_, err := pgctx.Exec(ctx, `
		delete from domains where id = $1
	`, id)
	return err
}

type domain struct {
	ID         int64
	ProjectID  int64
	LocationID string
	Domain     string
	Wildcard   bool
	Action     api.Action
	Status     api.DomainStatus
}

func (w *Worker) createDomain(ctx context.Context, d *domain) error {
	if !govalidator.IsDNSName(d.Domain) {
		return errors.New("invalid domain")
	}

	originServer, err := w.getLocationOriginServer(ctx, d.LocationID)
	if err != nil {
		return err
	}
	if originServer == "" {
		return fmt.Errorf("location not support")
	}

	sslConfig := cloudflare.CustomHostnameSSL{
		Method:               "txt",
		Type:                 "dv",
		Wildcard:             lo.ToPtr(d.Wildcard),
		CertificateAuthority: "google", // google, lets_encrypt
		Settings:             sslSettings,
	}

	resp, err := w.Client.CreateCustomHostname(ctx, w.ZoneID, cloudflare.CustomHostname{
		Hostname:           d.Domain,
		CustomOriginServer: originServer,
		SSL:                &sslConfig,
	})
	{
		var serr *cloudflare.RequestError
		if errors.As(err, &serr) {
			if lo.Contains(serr.ErrorCodes(), 1407) {
				// Invalid custom hostname.
				// Custom hostnames have to be smaller than 256 characters in length,
				// cannot be IP addresses, cannot contain spaces,
				// cannot contain any special characters such as _~`!@#$%^*()=+{}[]|\;:'",<>/? and
				// cannot begin or end with a '-' character.
				// Please check your input and try again. (1407)
				return w.setDomainStatus(ctx, d.ID, api.DomainStatusError)
			}
			if lo.Contains(serr.ErrorCodes(), 1411) {
				// Custom hostnames ending in example.com, example.net, or example.org are prohibited.
				return w.setDomainStatus(ctx, d.ID, api.DomainStatusError)
			}
		}
	}
	if err != nil {
		slog.Error("create failed", "error", err)
		return err
	}
	if !resp.Success {
		return fmt.Errorf("create custom hostname error; %v", resp.Errors)
	}
	err = w.setDomainStatus(ctx, d.ID, api.DomainStatusVerify)
	if err != nil {
		return err
	}

	d.Status = api.DomainStatusVerify
	return w.updateStatus(ctx, d)
}

func (w *Worker) deleteDomain(ctx context.Context, d *domain) error {
	hostnameID, err := w.Client.CustomHostnameIDByName(ctx, w.ZoneID, d.Domain)
	{
		var serr *cloudflare.RequestError
		if errors.As(err, &serr) {
			if lo.Contains(serr.ErrorCodes(), 1407) {
				return w.removeDomain(ctx, d.ID)
			}
		}
	}
	if err != nil {
		if err.Error() == "CustomHostname could not be found" { // TODO: cloudflare-go hardcode this error
			return w.removeDomain(ctx, d.ID)
		}
		return err
	}

	err = w.Client.DeleteCustomHostname(ctx, w.ZoneID, hostnameID)
	if err != nil {
		return err
	}

	return w.removeDomain(ctx, d.ID)
}

func (w *Worker) updateStatus(ctx context.Context, d *domain) error {
	if d.LocationID == "custom" {
		slog.Info("updateStatus: skip", "location", "custom", "domain", d.Domain)
		return nil
	}

	hostnameID, err := w.Client.CustomHostnameIDByName(ctx, w.ZoneID, d.Domain)
	if err != nil {
		if err.Error() == "CustomHostname could not be found" { // TODO: cloudflare-go hardcode this error
			slog.Warn("updateStatus: domain not found, skip", "domain", d.Domain)
			return nil
		}
		return err
	}

	x, err := w.Client.CustomHostname(ctx, w.ZoneID, hostnameID)
	if err != nil {
		return err
	}

	status := map[cloudflare.CustomHostnameStatus]api.DomainStatus{
		cloudflare.PENDING: api.DomainStatusVerify,
		cloudflare.ACTIVE:  api.DomainStatusSuccess,
		cloudflare.MOVED:   api.DomainStatusError,
		cloudflare.DELETED: api.DomainStatusError,
		cloudflare.BLOCKED: api.DomainStatusError,
	}[x.Status]

	if d.Status != status {
		slog.Info("updateStatus: status change", "domain", d.Domain, "from", d.Status, "to", status)
		err := pgctx.RunInTx(ctx, func(ctx context.Context) error {
			action, err := w.getDomainAction(ctx, d.ID)
			if err != nil {
				return err
			}
			if action != api.Create {
				return fmt.Errorf("action stale")
			}
			err = w.setDomainStatus(ctx, d.ID, status)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	// auto update settings
	// if x.SSL != nil {
	// 	if !isSSLSettingsUpToDate(x.SSL.Settings) {
	// 		logs.Infof("ssl outdated; domain=%s, updating...", d.Domain)
	// 		x.SSL.Settings = sslSettings
	// 		resp, err := w.Client.UpdateCustomHostnameSSL(ctx, w.ZoneID, hostnameID, x.SSL)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		if !resp.Success {
	// 			logs.Infof("update ssl failed; domain=%s, error=%v", d.Domain, resp.Errors)
	// 		}
	// 	}
	// }

	{
		var info api.DomainVerification
		info.Ownership.Name = x.OwnershipVerification.Name
		info.Ownership.Type = x.OwnershipVerification.Type
		info.Ownership.Value = x.OwnershipVerification.Value
		info.Ownership.Errors = x.VerificationErrors
		if info.Ownership.Errors == nil {
			info.Ownership.Errors = []string{}
		}
		if x.SSL != nil {
			info.SSL.Errors = []string{}
			info.SSL.Pending = x.SSL.Status == "pending_validation"
			for _, v := range x.SSL.ValidationErrors {
				info.SSL.Errors = append(info.SSL.Errors, v.Message)
			}
			info.SSL.DCV = api.DomainVerificationSSLDCV{
				Name:  fmt.Sprintf("_acme-challenge.%s", d.Domain),
				Value: fmt.Sprintf("%s.f47eba41660218cf.dcv.cloudflare.com", d.Domain), // TODO: move to config ?
			}
			info.SSL.Records = make([]api.DomainVerificationSSLRecord, 0, len(x.SSL.ValidationRecords))
			for _, v := range x.SSL.ValidationRecords {
				info.SSL.Records = append(info.SSL.Records, api.DomainVerificationSSLRecord{
					TxtName:  v.TxtName,
					TxtValue: v.TxtValue,
				})
			}
		}

		err = w.setDomainVerification(ctx, d.ID, info)
		if err != nil {
			return err
		}

		// domain banned by cloudflare
		if x.Status == cloudflare.BLOCKED {
			slog.Info("domain blocked by cloudflare, deleting", "domain", d.Domain)
			err = w.Client.DeleteCustomHostname(ctx, w.ZoneID, hostnameID)
			if err != nil {
				return err
			}
		}

		// TODO: status deleted handling
	}

	return nil
}

var sslSettings = cloudflare.CustomHostnameSSLSettings{
	HTTP2:         "on",
	HTTP3:         "on",
	TLS13:         "on",
	MinTLSVersion: "1.2",
	EarlyHints:    "on",
}

func isSSLSettingsUpToDate(s cloudflare.CustomHostnameSSLSettings) bool {
	return s.HTTP2 == sslSettings.HTTP2 &&
		// s.HTTP3 == sslSettings.HTTP3 &&
		s.TLS13 == sslSettings.TLS13 &&
		s.MinTLSVersion == sslSettings.MinTLSVersion &&
		s.EarlyHints == sslSettings.EarlyHints
}

func (w *Worker) collectCacheHit(ctx context.Context, d *domain, datetime time.Time) error {
	date := datetime.Format("2006-01-02")

	hostFilter := []any{
		map[string]any{
			"clientRequestHTTPHost": d.Domain,
		},
	}
	if d.Wildcard {
		hostFilter = append(hostFilter, map[string]any{
			"clientRequestHTTPHost_like": "%." + d.Domain,
		})
	}

	reqBody := map[string]any{
		"query": // language=GraphQL
		`
			query($zoneTag: string!, $filter: ZoneHttpRequestsAdaptiveGroupsFilter_InputObject!) {
				viewer {
					zones(filter: { zoneTag: $zoneTag }) {
						cacheStatus: httpRequestsAdaptiveGroups(filter: $filter, limit: 50) {
							count
							avg {
								sampleInterval
							}
							sum {
								edgeResponseBytes
							}
							dimensions {
								cacheStatus
							}
						}
					}
				}
			}
		`,
		"variables": map[string]any{
			"zoneTag": w.ZoneID,
			"filter": map[string]any{
				"AND": []any{
					map[string]any{
						"cacheStatus": "hit",
					},
					map[string]any{
						"date": date,
					},
					map[string]any{
						"OR": hostFilter,
					},
				},
			},
		},
	}

	reqBodyJSON, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.cloudflare.com/client/v4/graphql", bytes.NewReader(reqBodyJSON))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+w.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("not ok")
	}

	var respBody struct {
		Data struct {
			Viewer struct {
				Zones []struct {
					CacheStatus []struct {
						Avg struct {
							SampleInterval float64 `json:"sampleInterval"`
						} `json:"avg"`
						Count      int64 `json:"count"`
						Dimensions struct {
							CacheStatus string `json:"cacheStatus"`
						} `json:"dimensions"`
						Sum struct {
							EdgeResponseBytes int64 `json:"edgeResponseBytes"`
						} `json:"sum"`
					} `json:"cacheStatus"`
				} `json:"zones"`
			} `json:"viewer"`
		} `json:"data"`
	}
	err = json.NewDecoder(resp.Body).Decode(&respBody)
	if err != nil {
		return err
	}

	var cacheBytes int64
	if len(respBody.Data.Viewer.Zones) > 0 && len(respBody.Data.Viewer.Zones[0].CacheStatus) > 0 {
		cacheBytes = respBody.Data.Viewer.Zones[0].CacheStatus[0].Sum.EdgeResponseBytes
	}

	_, err = pgctx.Exec(ctx, `
		insert into domain_daily_usages (project_id, date, domain, name, value, updated_at)
		values ($1, $2, $3, $4, $5, now())
		on conflict (project_id, date, domain, name) do update
		set value = excluded.value,
		    updated_at = excluded.updated_at
	`, d.ProjectID, date, d.Domain, "hit_edge_response_bytes", cacheBytes)
	return err
}

func (w *Worker) runCollect(ctx context.Context) {
	now := time.Now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	runYday := time.Since(today) <= 3*time.Hour
	yday := today.AddDate(0, 0, -1)

	domains, err := w.getAllDomainsForCollect(ctx)
	if err != nil {
		slog.Error("can not get all domains for collect", "error", err)
		return
	}

	for _, d := range domains {
		d := d
		slog.Info("collecting", "domain", d.Domain, "project", d.ProjectID)
		var wg sync.WaitGroup
		if runYday {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := w.collectCacheHit(ctx, d, yday)
				if err != nil {
					slog.Error("can not collect cache hit", "domain", d.Domain, "error", err)
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := w.collectCacheHit(ctx, d, today)
			if err != nil {
				slog.Error("can not collect cache hit", "domain", d.Domain, "error", err)
			}
		}()
		wg.Wait()
	}
}

func (w *Worker) summaryProjectUsage(ctx context.Context) {
	now := time.Now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	runYday := time.Since(today) <= 3*time.Hour
	yday := today.AddDate(0, 0, -1)

	if runYday {
		err := w.summaryProjectUsageDate(ctx, yday)
		if err != nil {
			slog.Error("can not summary project usage", "error", err)
		}
	}

	err := w.summaryProjectUsageDate(ctx, today)
	if err != nil {
		slog.Error("can not summary project usage", "error", err)
	}
}

func (w *Worker) summaryProjectUsageDate(ctx context.Context, date time.Time) error {
	p := map[int64]int64{}
	err := pgctx.Iter(ctx, func(scan pgsql.Scanner) error {
		var (
			projectID int64
			value     int64
		)
		err := scan(&projectID, &value)
		if err != nil {
			return err
		}
		p[projectID] = value
		return nil
	},
		`
			select project_id, sum(value)
			from domain_daily_usages
			where name = 'hit_edge_response_bytes' and date >= $1 and date < $2
			group by project_id
		`, date, date.AddDate(0, 0, 1),
	)
	if err != nil {
		return err
	}

	for projectID, value := range p {
		_, err := pgctx.Exec(ctx, `
			insert into project_usages (project_id, location_id, date, name, value, updated_at)
			values ($1, '', $2, $3, $4, now())
			on conflict (project_id, location_id, date, name) do update
			set value = excluded.value,
			    updated_at = excluded.updated_at
		`, projectID, date.Format("2006-01-02"), "cache_egress", value)
		if err != nil {
			return err
		}
	}

	return nil
}
