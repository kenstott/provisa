// Copyright (c) 2026 Kenneth Stott
// Canary: 39667dd5-5838-4334-8559-8bb5a0a54863
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * "This profile has already been offered the guided tour."
 *
 * Its own module rather than a named export from useTour: the cloud e2e global setup seeds this
 * key and runs in Node, where importing the tour provider would drag React and driver.js in.
 */
export const TOUR_SEEN_KEY = "provisa_tour_seen";

/**
 * "Never offer the guided tour on this device again."
 *
 * Set by the welcome modal's checkbox. Device-scoped on purpose: there is no server-side user
 * preference store, and the offer is a UI nicety, not an account setting.
 */
export const TOUR_DECLINED_KEY = "provisa_tour_declined";

/**
 * "This browser session has already been offered the tour."
 *
 * sessionStorage: declining with "Maybe later" silences the offer for the rest of the session and
 * it comes back on the next sign-in, which is what the checkbox is there to stop permanently.
 */
export const TOUR_OFFERED_KEY = "provisa_tour_offered";

/**
 * "This browser session has already had the demo server's tour state reset."
 *
 * A demo deployment clears TOUR_SEEN_KEY once per browser session (useTour's
 * resetTourStateForDemoSession) so each new visitor is offered the tour. An automated run is not a
 * new visitor: it sets this flag so the reset is already spent and its seen-flag survives.
 */
export const TOUR_DEMO_RESET_KEY = "provisa_tour_demo_reset";
