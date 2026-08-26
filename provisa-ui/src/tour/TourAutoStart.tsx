// Copyright (c) 2026 Kenneth Stott
// Canary: 0c0b54e0-2bea-4ce0-8338-05fa819010c4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useLocation, useSearchParams } from "react-router-dom";
import { claimTourOffer, resetTourStateForDemoSession, useTour } from "./useTour";
import { useAuth } from "../context/AuthContext";
import { TourWelcomeModal } from "./TourWelcomeModal";

/**
 * Decides how the guided tour opens on arrival.
 *
 * `?tour=1` starts it outright and strips the param so a refresh doesn't relaunch — that URL is an
 * explicit request. Otherwise the tour is offered, not launched: a welcome modal once per browser
 * session, until it is taken or the modal's checkbox turns the offer off for good.
 */
export function TourAutoStart({ demoMode }: { demoMode: boolean }) {
  const { startTour, available } = useTour();
  const { loading: authLoading } = useAuth();
  const [searchParams, setSearchParams] = useSearchParams();
  const { pathname } = useLocation();
  const tourParam = searchParams.get("tour");
  // /login and /register are identity routes, not the product: someone arriving on an invite link
  // with a live session still lands on the sign-in form, and a tour offered over it covers the
  // thing they came to do. The offer is not spent here either — it waits for a route that has a
  // product to show.
  const identityRoute = pathname === "/login" || pathname === "/register";
  // Decided on the first render rather than in the effect: claimTourOffer marks the session
  // offered, so it must run exactly once per mount.
  const [offering, setOffering] = useState(() => {
    if (tourParam !== null || identityRoute) return false;
    // Each visit to a demo server is a new visitor: drop the previous one's seen-flag, declined
    // flag and half-finished progress before deciding whether to offer, so the tour is offered
    // afresh rather than suppressed by someone else's session.
    if (demoMode) resetTourStateForDemoSession();
    return claimTourOffer();
  });
  useEffect(() => {
    if (tourParam === null) return;
    // The itinerary is built from the viewer's rights, and those arrive from the identity
    // bootstrap after the first render. Starting before they land asks for a tour of no steps,
    // which startTour declines — and the param is consumed by then, so nothing ever asks again.
    if (authLoading) return;
    setSearchParams(
      (p) => {
        const n = new URLSearchParams(p);
        n.delete("tour");
        return n;
      },
      { replace: true },
    );
    // ?tour=1 always starts fresh from the top.
    startTour({ restart: true });
  }, [tourParam, authLoading, startTour, setSearchParams]);
  // A viewer whose rights open none of the tour's pages is offered nothing: the modal's Start would
  // open a tour with no steps in it.
  return offering && available ? <TourWelcomeModal onClose={() => setOffering(false)} /> : null;
}
