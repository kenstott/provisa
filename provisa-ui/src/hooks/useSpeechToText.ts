// Copyright (c) 2026 Kenneth Stott
// Canary: 2e8b4f13-6a7d-4c05-9e1f-3b8a6d2c7f54
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useEffect, useRef, useState } from "react";

/**
 * REQ-1805: mic-to-text for supported browsers, via the Web Speech API's SpeechRecognition —
 * client-side only, no backend involved. Chrome/Edge ship a working implementation; Safari's is
 * unreliable and Firefox has none, so `supported` gates the mic button's very presence rather
 * than showing a control that silently does nothing on those browsers.
 *
 * One utterance per `start()` call (continuous: false) — the caller gets a single final
 * transcript via `onResult`, matching a chat draft field rather than open-ended dictation.
 */
export function useSpeechToText(onResult: (transcript: string) => void) {
  const [supported] = useState(
    () => typeof window !== "undefined" && !!(window.SpeechRecognition || window.webkitSpeechRecognition),
  );
  const [listening, setListening] = useState(false);
  const recognitionRef = useRef<SpeechRecognition | null>(null);
  const onResultRef = useRef(onResult);

  useEffect(() => {
    onResultRef.current = onResult;
  }, [onResult]);

  useEffect(() => () => recognitionRef.current?.abort(), []);

  const start = useCallback(() => {
    if (!supported || listening) return;
    const Ctor = window.SpeechRecognition ?? window.webkitSpeechRecognition;
    if (!Ctor) return;
    const recognition = new Ctor();
    recognition.lang = navigator.language || "en-US";
    recognition.interimResults = false;
    recognition.continuous = false;
    recognition.onresult = (event) => {
      const transcript = Array.from({ length: event.results.length }, (_, i) => event.results[i][0].transcript)
        .join(" ")
        .trim();
      if (transcript) onResultRef.current(transcript);
    };
    recognition.onend = () => setListening(false);
    recognition.onerror = () => setListening(false);
    recognitionRef.current = recognition;
    setListening(true);
    recognition.start();
  }, [supported, listening]);

  const stop = useCallback(() => {
    recognitionRef.current?.stop();
  }, []);

  return { supported, listening, start, stop };
}
