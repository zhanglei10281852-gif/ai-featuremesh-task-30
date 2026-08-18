package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zhanglei10281852-gif/ai-featuremesh-base/internal/domain"
	"github.com/zhanglei10281852-gif/ai-featuremesh-base/internal/repository"
)

func TestLoggedOutSessionCannotBeReused(t *testing.T) {
	f := newServiceFixture(t)
	login, err := f.services.Auth.Login(f.ctx, LoginInput{Email: f.ml_engineer.Email, Password: "very-secure-password"})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.services.Auth.Logout(f.as(login.Principal), login.Principal); err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.Auth.Authenticate(f.ctx, login.Token); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("authentication after logout error = %v", err)
	}
}

func TestCancellationReleasesReservations(t *testing.T) {
	f := newServiceFixture(t)
	ctx := f.as(f.ml_engineer)
	shipment, err := f.services.Inference.PlanInferenceRun(ctx, PlanInferenceRunInput{
		WorkspaceID: f.workspace.ID, SourceZoneID: f.origin.ID, TargetZoneID: f.destination.ID,
		ComputePoolID: f.compute_pool.ID, Reference: "SHIP-CANCEL", SnapshotIDs: []string{f.batch.ID},
		ScheduledStartAt: f.clock.Now().Add(time.Hour), ExpectedFinishAt: f.clock.Now().Add(2 * time.Hour), IdempotencyKey: "cancel-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.Inference.CancelInferenceRun(ctx, shipment.ID, "route withdrawn"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Read(ctx, func(reader repository.Reader) error {
		batch, err := reader.GetDatasetSnapshot(ctx, f.batch.ID)
		if err != nil {
			return err
		}
		container, err := reader.GetComputePool(ctx, f.compute_pool.ID)
		if err != nil {
			return err
		}
		if batch.State != domain.SnapshotValidated || batch.InferenceRunID != "" {
			t.Fatalf("cancelled batch = %+v", batch)
		}
		if container.State != domain.ComputePoolAvailable || container.ReservedRunID != "" {
			t.Fatalf("cancelled container = %+v", container)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCompletionKeepsQuarantinedSnapshots(t *testing.T) {
	f := newServiceFixture(t)
	shipment := f.planAndStart(t, "SHIP-QUARANTINE")
	_, excursion, err := f.services.Metrics.RecordObservation(f.as(f.data_engineer), RecordObservationInput{
		InferenceRunID: shipment.ID, MetricKey: "sensor-q", Sequence: 1, Score: 12000, RecordedAt: f.clock.Now().Add(time.Minute),
	})
	if err != nil || excursion == nil {
		t.Fatalf("record excursion = %+v, error = %v", excursion, err)
	}
	if _, err := f.services.Inference.CompleteInferenceRun(f.as(f.data_engineer), shipment.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Read(f.ctx, func(reader repository.Reader) error {
		batch, err := reader.GetDatasetSnapshot(f.ctx, f.batch.ID)
		if err == nil && batch.State != domain.SnapshotQuarantined {
			t.Fatalf("arrived quarantined batch = %+v", batch)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestArchivingWaitsForQualityResolution(t *testing.T) {
	f := newServiceFixture(t)
	shipment := f.planAndStart(t, "SHIP-BLOCKED-CLOSE")
	if _, _, err := f.services.Metrics.RecordObservation(f.as(f.data_engineer), RecordObservationInput{
		InferenceRunID: shipment.ID, MetricKey: "sensor-close", Sequence: 1, Score: 12000, RecordedAt: f.clock.Now().Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.Inference.CompleteInferenceRun(f.as(f.data_engineer), shipment.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.Inference.ArchiveInferenceRun(f.as(f.ml_engineer), shipment.ID); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("close with unresolved samples error = %v", err)
	}
}

func TestSecondPendingApprovalTaskIsRejected(t *testing.T) {
	f := newServiceFixture(t)
	shipment := f.planAndStart(t, "SHIP-HANDOFF-UNIQUE")
	input := CreateApprovalTaskInput{InferenceRunID: shipment.ID, RequesterID: f.ml_engineer.UserID, ReviewerID: f.data_engineer.UserID, ReviewQueue: "Dock 4"}
	if _, err := f.services.Approval.CreateApprovalTask(f.as(f.data_engineer), input); err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.Approval.CreateApprovalTask(f.as(f.data_engineer), input); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("second pending handoff error = %v", err)
	}
}

func TestExpiredApprovalTaskCannotBeAccepted(t *testing.T) {
	f := newServiceFixture(t)
	shipment := f.planAndStart(t, "SHIP-HANDOFF-EXPIRED")
	handoff, err := f.services.Approval.CreateApprovalTask(f.as(f.data_engineer), CreateApprovalTaskInput{
		InferenceRunID: shipment.ID, RequesterID: f.ml_engineer.UserID, ReviewerID: f.data_engineer.UserID, ReviewQueue: "Dock 5",
	})
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(31 * time.Minute)
	if _, err := f.services.Approval.ResolveApprovalTask(f.as(f.data_engineer), handoff.ID, true, "late acceptance"); !errors.Is(err, domain.ErrExpired) {
		t.Fatalf("expired handoff error = %v", err)
	}
}

func TestDriftIncidentAccumulatesObservations(t *testing.T) {
	f := newServiceFixture(t)
	shipment := f.planAndStart(t, "SHIP-AGGREGATE")
	for sequence, temperature := range []domain.MilliScore{12000, -1000} {
		_, _, err := f.services.Metrics.RecordObservation(f.as(f.data_engineer), RecordObservationInput{
			InferenceRunID: shipment.ID, MetricKey: "sensor-a", Sequence: int64(sequence + 1), Score: temperature,
			RecordedAt: f.clock.Now().Add(time.Duration(sequence+1) * time.Minute),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	page, err := f.services.Query.DriftIncidents(f.as(f.risk_reviewer), repository.DriftIncidentFilter{InferenceRunID: shipment.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ObservationCount != 2 || page.Items[0].Minimum != -1000 || page.Items[0].Maximum != 12000 {
		t.Fatalf("drift_incidents = %+v", page)
	}
}

func TestRejectedReviewRejectsSnapshots(t *testing.T) {
	f := newServiceFixture(t)
	shipment := f.planAndStart(t, "SHIP-REJECT")
	_, excursion, err := f.services.Metrics.RecordObservation(f.as(f.data_engineer), RecordObservationInput{
		InferenceRunID: shipment.ID, MetricKey: "sensor-r", Sequence: 1, Score: 12000, RecordedAt: f.clock.Now().Add(time.Minute),
	})
	if err != nil || excursion == nil {
		t.Fatalf("record excursion = %+v, error = %v", excursion, err)
	}
	if _, err := f.services.Review.Decide(f.as(f.risk_reviewer), DecideInput{DriftIncidentID: excursion.ID, Decision: domain.DriftIncidentRejected, Rationale: "exposure exceeded protocol"}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Read(f.ctx, func(reader repository.Reader) error {
		batch, err := reader.GetDatasetSnapshot(f.ctx, f.batch.ID)
		if err == nil && batch.State != domain.SnapshotRejected {
			t.Fatalf("rejected batch = %+v", batch)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReadinessShowsPendingApprovalTask(t *testing.T) {
	f := newServiceFixture(t)
	shipment := f.planAndStart(t, "SHIP-RECONCILE-HANDOFF")
	if _, err := f.services.Approval.CreateApprovalTask(f.as(f.data_engineer), CreateApprovalTaskInput{
		InferenceRunID: shipment.ID, RequesterID: f.ml_engineer.UserID, ReviewerID: f.data_engineer.UserID, ReviewQueue: "Dock 6",
	}); err != nil {
		t.Fatal(err)
	}
	report, err := f.services.Query.ReconcileInferenceRun(f.as(f.ml_engineer), shipment.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !report.PendingApprovalTask || report.Complete {
		t.Fatalf("reconciliation = %+v", report)
	}
}

func TestBulkResultsOwnIndependentSnapshots(t *testing.T) {
	f := newServiceFixture(t)
	now := f.clock.Now()
	result, err := f.services.Catalog.BulkRegisterSnapshots(f.as(f.ml_engineer), []domain.DatasetSnapshot{
		{WorkspaceID: f.workspace.ID, SourceZoneID: f.origin.ID, SourceRevision: "BULK-A", SchemaFamily: "ranking-features-v2", PartitionCount: 1, EstimatedRows: 20, ExpiresAt: now.Add(time.Hour)},
		{WorkspaceID: f.workspace.ID, SourceZoneID: f.origin.ID, SourceRevision: "BULK-B", SchemaFamily: "fraud-features-v3", PartitionCount: 2, EstimatedRows: 30, ExpiresAt: now.Add(2 * time.Hour)},
	})
	if err != nil || result.Succeeded != 2 {
		t.Fatalf("bulk result = %+v, error = %v", result, err)
	}
	firstID := result.Items[0].Snapshot.ID
	result.Items[1].Snapshot.ID = "changed"
	if result.Items[0].Snapshot.ID != firstID || result.Items[0].Snapshot == result.Items[1].Snapshot {
		t.Fatalf("bulk item ownership = %+v", result.Items)
	}
}

func TestDuplicateObservationPreservesConflict(t *testing.T) {
	f := newServiceFixture(t)
	shipment := f.planAndStart(t, "SHIP-DUPLICATE-READING")
	input := RecordObservationInput{InferenceRunID: shipment.ID, MetricKey: "sensor-d", Sequence: 1, Score: 5000, RecordedAt: f.clock.Now()}
	if _, _, err := f.services.Metrics.RecordObservation(f.as(f.data_engineer), input); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.services.Metrics.RecordObservation(f.as(f.data_engineer), input); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("duplicate reading error = %v", err)
	}
}

func TestDataEngineerCannotReadAuditTrail(t *testing.T) {
	f := newServiceFixture(t)
	if _, err := f.services.Query.Audit(f.as(f.data_engineer), repository.AuditFilter{Page: repository.PageRequest{Limit: 10}}); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("courier audit error = %v", err)
	}
}

func TestCancelledSnapshotValidationDoesNotAdvance(t *testing.T) {
	f := newServiceFixture(t)
	registered, err := f.services.Catalog.RegisterSnapshot(f.as(f.ml_engineer), domain.DatasetSnapshot{
		WorkspaceID: f.workspace.ID, SourceZoneID: f.origin.ID, SourceRevision: "CANCELLED-VALIDATION",
		SchemaFamily: "ranking-features-v2", PartitionCount: 1, EstimatedRows: 10, ExpiresAt: f.clock.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(f.as(f.ml_engineer))
	cancel()
	if _, err := f.services.Catalog.ValidateSnapshot(cancelled, registered.ID); err == nil {
		t.Fatal("cancelled state change succeeded")
	}
	if err := f.store.Read(f.ctx, func(reader repository.Reader) error {
		batch, err := reader.GetDatasetSnapshot(f.ctx, registered.ID)
		if err == nil && batch.State != domain.SnapshotRegistered {
			t.Fatalf("cancelled batch = %+v", batch)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
