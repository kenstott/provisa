// Copyright (c) 2026 Kenneth Stott
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
 * "This browser session has already had the demo server's tour state reset."
 *
 * A demo deployment clears TOUR_SEEN_KEY once per browser session (useTour's
 * resetTourStateForDemoSession) so each new visitor is offered the tour. An automated run is not a
 * new visitor: it sets this flag so the reset is already spent and its seen-flag survives.
 */
export const TOUR_DEMO_RESET_KEY = "provisa_tour_demo_reset";
