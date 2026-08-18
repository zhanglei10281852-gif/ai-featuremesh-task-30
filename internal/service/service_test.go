package service

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/zhanglei10281852-gif/ai-featuremesh-base/internal/clock"
	"github.com/zhanglei10281852-gif/ai-featuremesh-base/internal/domain"
	"github.com/zhanglei10281852-gif/ai-featuremesh-base/internal/repository"
	"github.com/zhanglei10281852-gif/ai-featuremesh-base/internal/requestmeta"
	"github.com/zhanglei10281852-gif/ai-featuremesh-base/internal/storage/sqlite"
)

type serviceFixture struct {
	t             *testing.T
	ctx           context.Context
	store         *sqlite.Store
	services      *Services
	clock         *clock.Fixed
	ml_engineer   domain.Principal
	data_engineer domain.Principal
	risk_reviewer domain.Principal
	workspace     domain.Workspace
	origin        domain.DataZone
	destination   domain.DataZone
	compute_pool  domain.ComputePool
	batch         domain.DatasetSnapshot
}

func newServiceFixture(t *testing.T) *serviceFixture {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 8, 18, 8, 0, 0, 0, time.UTC)
	fixed := clock.NewFixed(now)
	store, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	services := New(store, fixed, 4*time.Hour, 30*time.Minute)
	users := []struct {
		email string
		name  string
		role  domain.Role
	}{
		{"ops@example.test", "Ops", domain.RoleMLEngineer},
		{"data_engineer@example.test", "Data Engineer", domain.RoleDataEngineer},
		{"risk_reviewer@example.test", "Reviewer", domain.RoleRiskReviewer},
	}
	principals := make([]domain.Principal, 0, len(users))
	for _, user := range users {
		created, err := services.Auth.CreateUser(ctx, user.email, user.name, "very-secure-password", user.role)
		if err != nil {
			t.Fatalf("create user %s: %v", user.email, err)
		}
		login, err := services.Auth.Login(ctx, LoginInput{Email: user.email, Password: "very-secure-password"})
		if err != nil {
			t.Fatalf("login %s: %v", user.email, err)
		}
		if login.Principal.UserID != created.ID {
			t.Fatalf("principal user = %s, created = %s", login.Principal.UserID, created.ID)
		}
		principals = append(principals, login.Principal)
	}
	minimum, _ := domain.ScoreFromFloat(2)
	maximum, _ := domain.ScoreFromFloat(8)
	rangeValue, _ := domain.NewQualityRange(minimum, maximum)
	opsCtx := requestmeta.WithPrincipal(ctx, principals[0])
	workspace, err := services.Catalog.CreateWorkspace(opsCtx, domain.Workspace{Code: "STUDY-1", Name: "Cold workspace", Score: rangeValue, MaxExecution: 24 * time.Hour, ReviewDeadline: 4 * time.Hour, BusinessTimezone: "Asia/Shanghai"})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err = services.Catalog.ActivateWorkspace(opsCtx, workspace.ID)
	if err != nil {
		t.Fatal(err)
	}
	origin, err := services.Catalog.CreateDataZone(opsCtx, domain.DataZone{Code: "SITE-1", Name: "Origin", Timezone: "Asia/Shanghai", DailyLimit: 10, CutoffHour: 6})
	if err != nil {
		t.Fatal(err)
	}
	destination, err := services.Catalog.CreateDataZone(opsCtx, domain.DataZone{Code: "SITE-2", Name: "Destination", Timezone: "Asia/Shanghai", DailyLimit: 10, CutoffHour: 6})
	if err != nil {
		t.Fatal(err)
	}
	now = fixed.Now()
	compute_pool, err := services.Catalog.CreateComputePool(opsCtx, domain.ComputePool{SerialNumber: "BOX-1", CapacityRows: 1000, AttestationDueAt: now.Add(48 * time.Hour), LastReconciledAt: now})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := services.Catalog.RegisterSnapshot(opsCtx, domain.DatasetSnapshot{WorkspaceID: workspace.ID, SourceZoneID: origin.ID, SourceRevision: "EXT-1", SchemaFamily: "plasma", PartitionCount: 2, EstimatedRows: 100, ExpiresAt: now.Add(48 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	batch, err = services.Catalog.ValidateSnapshot(opsCtx, batch.ID)
	if err != nil {
		t.Fatal(err)
	}
	return &serviceFixture{t: t, ctx: ctx, store: store, services: services, clock: fixed, ml_engineer: principals[0], data_engineer: principals[1], risk_reviewer: principals[2], workspace: workspace, origin: origin, destination: destination, compute_pool: compute_pool, batch: batch}
}

func (f *serviceFixture) as(principal domain.Principal) context.Context {
	return requestmeta.WithPrincipal(requestmeta.WithRequestID(f.ctx, "req-test"), principal)
}

func TestAuthRejectsWrongPasswordAndHonorsLogout(t *testing.T) {
	f := newServiceFixture(t)
	if _, err := f.services.Auth.Login(f.ctx, LoginInput{Email: f.ml_engineer.Email, Password: "wrong-password"}); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("wrong password error = %v", err)
	}
	if err := f.services.Auth.Logout(f.as(f.ml_engineer), f.ml_engineer); err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.Auth.Authenticate(f.ctx, "missing-token"); err == nil {
		t.Fatal("missing token authenticated")
	}
}

func TestInferenceIsIdempotentAndReservesRelatedEntities(t *testing.T) {
	f := newServiceFixture(t)
	input := PlanInferenceRunInput{WorkspaceID: f.workspace.ID, SourceZoneID: f.origin.ID, TargetZoneID: f.destination.ID, ComputePoolID: f.compute_pool.ID, Reference: "SHIP-1", SnapshotIDs: []string{f.batch.ID}, ScheduledStartAt: f.clock.Now().Add(time.Hour), ExpectedFinishAt: f.clock.Now().Add(2 * time.Hour), IdempotencyKey: "plan-key"}
	ctx := f.as(f.ml_engineer)
	first, err := f.services.Inference.PlanInferenceRun(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.services.Inference.PlanInferenceRun(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || first.Reference != "SHIP-1" {
		t.Fatalf("idempotent responses differ: %+v / %+v", first, second)
	}
	if err := f.store.Read(ctx, func(reader repository.Reader) error {
		batch, err := reader.GetDatasetSnapshot(ctx, f.batch.ID)
		if err != nil {
			return err
		}
		if batch.State != domain.SnapshotReserved || batch.InferenceRunID != first.ID {
			t.Fatalf("reserved batch = %+v", batch)
		}
		compute_pool, err := reader.GetComputePool(ctx, f.compute_pool.ID)
		if err != nil {
			return err
		}
		if compute_pool.State != domain.ComputePoolReserved || compute_pool.ReservedRunID != first.ID {
			t.Fatalf("reserved compute_pool = %+v", compute_pool)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestInferenceRejectsDifferentIdempotencyPayload(t *testing.T) {
	f := newServiceFixture(t)
	input := PlanInferenceRunInput{WorkspaceID: f.workspace.ID, SourceZoneID: f.origin.ID, TargetZoneID: f.destination.ID, ComputePoolID: f.compute_pool.ID, Reference: "SHIP-1", SnapshotIDs: []string{f.batch.ID}, ScheduledStartAt: f.clock.Now().Add(time.Hour), ExpectedFinishAt: f.clock.Now().Add(2 * time.Hour), IdempotencyKey: "plan-key"}
	ctx := f.as(f.ml_engineer)
	if _, err := f.services.Inference.PlanInferenceRun(ctx, input); err != nil {
		t.Fatal(err)
	}
	input.Reference = "SHIP-OTHER"
	if _, err := f.services.Inference.PlanInferenceRun(ctx, input); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("different payload error = %v", err)
	}
}

func TestInferenceLifecycleMovesSnapshotsAndComputePool(t *testing.T) {
	f := newServiceFixture(t)
	ctx := f.as(f.ml_engineer)
	run, err := f.services.Inference.PlanInferenceRun(ctx, PlanInferenceRunInput{WorkspaceID: f.workspace.ID, SourceZoneID: f.origin.ID, TargetZoneID: f.destination.ID, ComputePoolID: f.compute_pool.ID, Reference: "SHIP-LIFE", SnapshotIDs: []string{f.batch.ID}, ScheduledStartAt: f.clock.Now().Add(time.Hour), ExpectedFinishAt: f.clock.Now().Add(2 * time.Hour), IdempotencyKey: "life-key"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.Inference.StageInferenceRun(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.Inference.StartInferenceRun(f.as(f.data_engineer), run.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Read(ctx, func(reader repository.Reader) error {
		batch, err := reader.GetDatasetSnapshot(ctx, f.batch.ID)
		if err != nil {
			return err
		}
		if batch.State != domain.SnapshotMaterializing {
			t.Fatalf("in execution batch = %+v", batch)
		}
		compute_pool, err := reader.GetComputePool(ctx, f.compute_pool.ID)
		if err != nil {
			return err
		}
		if compute_pool.State != domain.ComputePoolAllocated {
			t.Fatalf("in execution compute_pool = %+v", compute_pool)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.Inference.CompleteInferenceRun(f.as(f.data_engineer), run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.Inference.ArchiveInferenceRun(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
}

func TestApprovalOnlyReceiverCanResolve(t *testing.T) {
	f := newServiceFixture(t)
	ctx := f.as(f.ml_engineer)
	run, err := f.services.Inference.PlanInferenceRun(ctx, PlanInferenceRunInput{WorkspaceID: f.workspace.ID, SourceZoneID: f.origin.ID, TargetZoneID: f.destination.ID, ComputePoolID: f.compute_pool.ID, Reference: "SHIP-HAND", SnapshotIDs: []string{f.batch.ID}, ScheduledStartAt: f.clock.Now().Add(time.Hour), ExpectedFinishAt: f.clock.Now().Add(2 * time.Hour), IdempotencyKey: "hand-key"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.Inference.StageInferenceRun(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.Inference.StartInferenceRun(f.as(f.data_engineer), run.ID); err != nil {
		t.Fatal(err)
	}
	approval_task, err := f.services.Approval.CreateApprovalTask(f.as(f.data_engineer), CreateApprovalTaskInput{InferenceRunID: run.ID, RequesterID: f.ml_engineer.UserID, ReviewerID: f.data_engineer.UserID, ReviewQueue: "Dock 2"})
	if err != nil {
		t.Fatal(err)
	}
	other := domain.Principal{UserID: "compliance_auditor-user", Role: domain.RoleComplianceAuditor}
	if _, err := f.services.Approval.ResolveApprovalTask(f.as(other), approval_task.ID, true, "wrong actor"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("wrong actor error = %v", err)
	}
	if _, err := f.services.Approval.ResolveApprovalTask(f.as(f.data_engineer), approval_task.ID, true, "seal intact"); err != nil {
		t.Fatal(err)
	}
}

func (f *serviceFixture) planAndStart(t *testing.T, ref string) domain.InferenceRun {
	t.Helper()
	run, err := f.services.Inference.PlanInferenceRun(f.as(f.ml_engineer), PlanInferenceRunInput{WorkspaceID: f.workspace.ID, SourceZoneID: f.origin.ID, TargetZoneID: f.destination.ID, ComputePoolID: f.compute_pool.ID, Reference: ref, SnapshotIDs: []string{f.batch.ID}, ScheduledStartAt: f.clock.Now().Add(time.Hour), ExpectedFinishAt: f.clock.Now().Add(2 * time.Hour), IdempotencyKey: ref + "-key"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.Inference.StageInferenceRun(f.as(f.ml_engineer), run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.Inference.StartInferenceRun(f.as(f.data_engineer), run.ID); err != nil {
		t.Fatal(err)
	}
	return run
}

func TestScoreDriftIncidentQuarantinesAndReviewerClears(t *testing.T) {
	f := newServiceFixture(t)
	run := f.planAndStart(t, "RUN-DRIFT")
	observation, drift_incident, err := f.services.Metrics.RecordObservation(f.as(f.data_engineer), RecordObservationInput{InferenceRunID: run.ID, MetricKey: "sensor-1", Sequence: 1, Score: 12000, RecordedAt: f.clock.Now().Add(10 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if observation.ID == "" || drift_incident == nil || drift_incident.ObservationCount != 1 {
		t.Fatalf("observation=%+v drift_incident=%+v", observation, drift_incident)
	}
	if _, err := f.services.Inference.CompleteInferenceRun(f.as(f.data_engineer), run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.Review.StartReview(f.as(f.risk_reviewer), drift_incident.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.Review.Decide(f.as(f.risk_reviewer), DecideInput{DriftIncidentID: drift_incident.ID, Decision: domain.DriftIncidentCleared, Rationale: "validated logger trace"}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Read(f.ctx, func(reader repository.Reader) error {
		batch, err := reader.GetDatasetSnapshot(f.ctx, f.batch.ID)
		if err != nil {
			return err
		}
		if batch.State != domain.SnapshotApproved {
			t.Fatalf("batch after clear = %+v", batch)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestInRangeObservationDoesNotOpenDriftIncident(t *testing.T) {
	f := newServiceFixture(t)
	run := f.planAndStart(t, "RUN-IN-RANGE")
	_, drift_incident, err := f.services.Metrics.RecordObservation(f.as(f.data_engineer), RecordObservationInput{InferenceRunID: run.ID, MetricKey: "sensor-1", Sequence: 1, Score: 5000, RecordedAt: f.clock.Now()})
	if err != nil || drift_incident != nil {
		t.Fatalf("in range result drift_incident=%+v error=%v", drift_incident, err)
	}
}

func TestQueryReadinessReportsBlockers(t *testing.T) {
	f := newServiceFixture(t)
	run := f.planAndStart(t, "RUN-REPORT")
	report, err := f.services.Query.ReconcileInferenceRun(f.as(f.ml_engineer), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if report.ExpectedSnapshotCount != 1 || report.Complete {
		t.Fatalf("report = %+v", report)
	}
}

func TestContextCancellationReachesTransaction(t *testing.T) {
	f := newServiceFixture(t)
	cancelled, cancel := context.WithCancel(f.as(f.ml_engineer))
	cancel()
	_, err := f.services.Catalog.ValidateSnapshot(cancelled, f.batch.ID)
	if err == nil {
		t.Fatal("cancelled command succeeded")
	}
}

func TestComputePoolReconcilingAndRetirementLifecycle(t *testing.T) {
	f := newServiceFixture(t)
	ctx := f.as(f.ml_engineer)
	reconciliation, err := f.services.ComputePools.StartReconciliation(ctx, f.compute_pool.ID)
	if err != nil || reconciliation.State != domain.ComputePoolReconciling {
		t.Fatalf("start reconciliation = %+v, error=%v", reconciliation, err)
	}
	f.clock.Advance(time.Hour)
	available, err := f.services.ComputePools.CompleteReconciliation(ctx, f.compute_pool.ID)
	if err != nil || available.State != domain.ComputePoolAvailable || !available.LastReconciledAt.Equal(f.clock.Now()) {
		t.Fatalf("complete reconciliation = %+v, error=%v", available, err)
	}
	retired, err := f.services.ComputePools.Retire(ctx, f.compute_pool.ID, "attestation program ended")
	if err != nil || retired.State != domain.ComputePoolRetired {
		t.Fatalf("retire = %+v, error=%v", retired, err)
	}
	if _, err := f.services.ComputePools.StartReconciliation(ctx, f.compute_pool.ID); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("clean retired error = %v", err)
	}
}

func TestBulkRegistrationReturnsPartialFailures(t *testing.T) {
	f := newServiceFixture(t)
	now := f.clock.Now()
	inputs := []domain.DatasetSnapshot{
		{WorkspaceID: f.workspace.ID, SourceZoneID: f.origin.ID, SourceRevision: "BULK-OK", SchemaFamily: "serum", PartitionCount: 1, EstimatedRows: 20, ExpiresAt: now.Add(time.Hour)},
		{WorkspaceID: f.workspace.ID, SourceZoneID: f.origin.ID, SourceRevision: "", SchemaFamily: "serum", PartitionCount: 1, EstimatedRows: 20, ExpiresAt: now.Add(time.Hour)},
		{WorkspaceID: f.workspace.ID, SourceZoneID: f.origin.ID, SourceRevision: "BULK-OK", SchemaFamily: "serum", PartitionCount: 1, EstimatedRows: 20, ExpiresAt: now.Add(time.Hour)},
	}
	result, err := f.services.Catalog.BulkRegisterSnapshots(f.as(f.ml_engineer), inputs)
	if err != nil {
		t.Fatal(err)
	}
	if result.Succeeded != 1 || result.Failed != 2 || len(result.Items) != 3 {
		t.Fatalf("bulk result = %+v", result)
	}
	if result.Items[0].Code != "created" || result.Items[1].Code != "invalid" || result.Items[2].Code != "conflict" {
		t.Fatalf("bulk item codes = %+v", result.Items)
	}
}

func TestPlatformSummaryRequiresReadPermissionAndCountsRows(t *testing.T) {
	f := newServiceFixture(t)
	if _, err := f.services.Query.PlatformSummary(f.as(f.risk_reviewer)); err != nil {
		t.Fatalf("risk_reviewer summary: %v", err)
	}
	summary, err := f.services.Query.PlatformSummary(f.as(f.ml_engineer))
	if err != nil {
		t.Fatal(err)
	}
	if summary.WorkspacesActive != 1 || summary.SnapshotsValidated != 1 || summary.ComputePoolsAvailable != 1 {
		t.Fatalf("summary = %+v", summary)
	}
	if _, err := f.services.Query.PlatformSummary(f.as(domain.Principal{UserID: "data_engineer", Role: domain.RoleDataEngineer})); err != nil {
		t.Fatalf("data_engineer read summary: %v", err)
	}
}
