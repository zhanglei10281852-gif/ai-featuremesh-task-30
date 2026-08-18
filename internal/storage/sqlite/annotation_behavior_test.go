package sqlite

import (
	"testing"
	"time"

	"github.com/zhanglei10281852-gif/ai-featuremesh-base/internal/domain"
	"github.com/zhanglei10281852-gif/ai-featuremesh-base/internal/repository"
)

func TestFilteredInferenceRunTotalMatchesItems(t *testing.T) {
	store, ctx, now := testStore(t)
	workspace, source, target, computePool, _ := seedCatalog(t, store, ctx, now)
	inference_runs := []domain.InferenceRun{
		{ID: "run_filter_1", WorkspaceID: workspace.ID, SourceZoneID: source.ID, TargetZoneID: target.ID, ComputePoolID: computePool.ID, Reference: "FILTER-1", State: domain.InferenceRunQueued, ScheduledStartAt: now.Add(time.Hour), ExpectedFinishAt: now.Add(2 * time.Hour), TotalEstimatedRows: 10, Version: 1, CreatedAt: now, UpdatedAt: now},
		{ID: "run_filter_2", WorkspaceID: workspace.ID, SourceZoneID: source.ID, TargetZoneID: target.ID, ComputePoolID: computePool.ID, Reference: "FILTER-2", State: domain.InferenceRunStaged, ScheduledStartAt: now.Add(2 * time.Hour), ExpectedFinishAt: now.Add(3 * time.Hour), TotalEstimatedRows: 10, Version: 1, CreatedAt: now, UpdatedAt: now},
	}
	if err := store.WithTx(ctx, func(tx repository.Tx) error {
		for _, shipment := range inference_runs {
			if err := tx.InsertInferenceRun(ctx, shipment); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var page repository.InferenceRunPage
	if err := store.Read(ctx, func(reader repository.Reader) error {
		var err error
		page, err = reader.ListInferenceRuns(ctx, repository.InferenceRunFilter{State: domain.InferenceRunQueued, Page: repository.PageRequest{Limit: 10}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].ID != "run_filter_1" {
		t.Fatalf("filtered page = %+v", page)
	}
}

func TestJobCannotBeClaimedTwice(t *testing.T) {
	store, ctx, now := testStore(t)
	job := domain.OutboxJob{ID: "job_once", Kind: "inference_run_planned", AggregateID: "run_once", Payload: []byte(`{}`), Status: domain.JobPending, MaxAttempts: 3, AvailableAt: now, CreatedAt: now, UpdatedAt: now}
	if err := store.WithTx(ctx, func(tx repository.Tx) error { return tx.InsertJob(ctx, job) }); err != nil {
		t.Fatal(err)
	}
	claim := func() []domain.OutboxJob {
		var jobs []domain.OutboxJob
		if err := store.WithTx(ctx, func(tx repository.Tx) error {
			var err error
			jobs, err = tx.ClaimJobs(ctx, now, 10)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return jobs
	}
	if jobs := claim(); len(jobs) != 1 {
		t.Fatalf("first claim = %+v", jobs)
	}
	if jobs := claim(); len(jobs) != 0 {
		t.Fatalf("second claim = %+v", jobs)
	}
}
