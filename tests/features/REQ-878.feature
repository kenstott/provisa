# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-878 — Point-In-Time Reconstruction
  # Point-in-time MV reconstruction from the delta ledger (REQ-877). An append-only delta ledger is a temporal substrate: fo…

  Scenario: REQ-878 default behaviour
    Given an MV with row-level delta capture enabled and a delta ledger spanning refresh versions 1..N When a caller reconstructs the MV as of version K (K<N) via forward-fold from base or reverse from the live table Then the reconstructed rows equal the MV state as of version K, and reconstructing an unknown version fails loud
